import peewee as pw
import enum
import datetime
from typing import List, Tuple, Union, Optional
import dataclasses
import hashlib


class RequestStatus(enum.Enum):
    PENDING = 1
    CLOSED = 2
    DOWNLOADED = 3
    ERROR = 4


def status_from_str(status: str):
    lut = dict(closed=RequestStatus.CLOSED, open=RequestStatus.PENDING)
    return lut[status.lower()]


class TaskType(enum.Enum):
    DOWNLOAD = 0
    BULK_DOWNLOAD = 1
    UPDATE_METADATA = 2


@dataclasses.dataclass
class DatabaseStats:
    total_request_count: int
    pending_request_count: int
    downloaded_request_count: int
    closed_request_count: int
    error_request_count: int
    last_scrape: datetime.datetime
    document_count: int


# Because I have very minimal performance requirements
# I'll just use SQLite as a persistent queue
# If I ever feel the need to scale this up I'll
# switch to something better. This has the advantage of allowing us
# to easily keep track of stats, history, etc on top of the queue.
db = pw.SqliteDatabase("")


class FOIARequest(pw.Model):
    id = pw.PrimaryKeyField()
    date_submitted = pw.DateField()
    date_checked = pw.DateField()
    department = pw.CharField()
    scrape_source = pw.CharField()
    request_id = pw.CharField()
    request_status = pw.IntegerField()
    document_count = pw.IntegerField()

    class Meta:
        database = db
        constraints = [pw.SQL("UNIQUE (scrape_source, request_id)")]


class DocumentDownload(pw.Model):
    id = pw.PrimaryKeyField()
    request = pw.ForeignKeyField(FOIARequest, null=True)
    document_id = pw.CharField(null=True)
    date_downloaded = pw.DateField()
    is_bulk = pw.BooleanField()
    download_path = pw.CharField()
    checksum = pw.CharField()
    document_count = pw.IntegerField()

    class Meta:
        database = db


class WorkQueue(pw.Model):
    id = pw.PrimaryKeyField()
    target_source = pw.CharField()
    task_type = pw.IntegerField()
    task_target_id = pw.CharField()
    document_name = pw.CharField(null=True)
    document_id = pw.CharField(null=True)

    class Meta:
        database = db


class ScrapeMetadata(pw.Model):
    id = pw.PrimaryKeyField()
    scrape_source = pw.CharField()
    last_scrape_date = pw.DateField()

    class Meta:
        database = db
        constraints = [pw.SQL("UNIQUE (scrape_source)")]


def get_doc_md5(file_path: str):
    with open(file_path, "rb") as f:
        return hashlib.md5(f.read()).hexdigest()


class DBSession:
    def __init__(self, db_path: str) -> None:
        db.init(db_path)
        db.connect()
        db.create_tables([FOIARequest, DocumentDownload, WorkQueue, ScrapeMetadata])

    def atomic(self):
        return db.atomic()

    def get_requests(self, query=None):
        return FOIARequest.select().where(query)

    def get_request(self, scrape_source: str, request_id: str) -> Optional[FOIARequest]:
        return FOIARequest.get_or_none(
            (FOIARequest.scrape_source == scrape_source)
            & (FOIARequest.request_id == request_id)
        )

    def update_request(self, req: FOIARequest, **kwargs):
        where = FOIARequest.id == req.id
        FOIARequest.update(**kwargs).where(where).execute()

    def add_request(
        self,
        scrape_source: str,
        request_id: str,
        request_status: RequestStatus,
        request_date: datetime.datetime,
        department: str,
        document_count: int,
    ):
        if req := self.get_request(scrape_source, request_id):
            self.update_request(
                req,
                department=department,
                document_count=document_count,
                date_checked=datetime.datetime.now(),
                request_status=request_status.value,
            )
            return self.get_request(scrape_source, request_id)
        else:
            req = FOIARequest.create(
                scrape_source=scrape_source,
                request_id=request_id,
                date_submitted=request_date,
                date_checked=datetime.datetime.now(),
                department=department,
                document_count=document_count,
                request_status=request_status.value,
            )
            return req

    def get_task(
        self,
        task_type: TaskType,
        target_source: str,
        target_id: str,
        document_id: str = None,
    ):
        return WorkQueue.select().where(
            (WorkQueue.target_source == target_source)
            & (WorkQueue.task_type == task_type.value)
            & (WorkQueue.task_target_id == target_id)
            & (WorkQueue.document_id == document_id)
        )

    def add_bulk_download_task(self, target_source: str, request_id: str):
        if self.get_task(TaskType.BULK_DOWNLOAD, target_source, request_id):
            return

        WorkQueue.create(
            target_source=target_source,
            task_type=TaskType.BULK_DOWNLOAD.value,
            task_target_id=request_id,
        )

    def add_download_task(
        self, target_source: str, request_id: str, doc_id: str, doc_name: str
    ):
        if self.get_task(TaskType.DOWNLOAD, target_source, request_id, doc_id):
            return

        WorkQueue.create(
            target_source=target_source,
            task_type=TaskType.DOWNLOAD.value,
            task_target_id=request_id,
            document_id=doc_id,
            document_name=doc_name,
        )

    def add_update_task(self, target_source: str, request_id: str):
        if self.get_task(TaskType.UPDATE_METADATA, target_source, request_id):
            return

        WorkQueue.create(
            target_source=target_source,
            task_type=TaskType.UPDATE_METADATA.value,
            task_target_id=request_id,
        )

    def get_tasks(self, query=None):
        return WorkQueue.select().where(query)

    def get_tasks_for_source(self, target_source: str):
        return self.get_tasks(WorkQueue.target_source == target_source)

    def clear_tasks(self):
        WorkQueue.delete().execute()

    def mark_task_completed(self, task: WorkQueue):
        WorkQueue.delete().where(WorkQueue.id == task.id).execute()

    def _add_download(
        self,
        request: FOIARequest,
        path: str,
        is_bulk: bool,
        document_count: int,
        document_id: str = None,
    ) -> DocumentDownload:
        checksum = get_doc_md5(path)
        return DocumentDownload.create(
            request=request,
            date_downloaded=datetime.datetime.now(),
            is_bulk=is_bulk,
            download_path=path,
            checksum=checksum,
            document_id=document_id,
            document_count=document_count,
        )

    def add_download(
        self, request: FOIARequest, path: str, document_id: str
    ) -> DocumentDownload:
        return self._add_download(request, path, False, 1, document_id)

    def add_bulk_download(self, request: FOIARequest, path: str) -> DocumentDownload:
        return self._add_download(request, path, True, request.document_count)

    def get_downloads(self, query=None) -> DocumentDownload:
        return DocumentDownload.select().where(query)

    def get_download(self, scrape_source: str, request_id: str, document_id: str):
        return (
            DocumentDownload.select()
            .join(FOIARequest)
            .where(
                (DocumentDownload.document_id == document_id)
                & (FOIARequest.scrape_source == scrape_source)
                & (FOIARequest.request_id == request_id)
            )
            .get_or_none()
        )

    def get_bulk_download(
        self, scrape_source: str, request_id: str, checksum: str = None
    ):
        query = (
            (DocumentDownload.is_bulk == True)
            & (FOIARequest.request_id == request_id)
            & (FOIARequest.scrape_source == scrape_source)
        )
        if checksum:
            query = query & (DocumentDownload.checksum == checksum)

        return DocumentDownload.select().join(FOIARequest).where(query).get_or_none()

    def mark_request_closed(self, request: FOIARequest):
        self.update_request(
            request,
            date_checked=datetime.datetime.now(),
            request_status=RequestStatus.CLOSED.value,
        )

    def mark_request_error(self, request: FOIARequest):
        self.update_request(
            request,
            date_checked=datetime.datetime.now(),
            request_status=RequestStatus.ERROR.value,
        )

    def mark_request_pending(self, request: FOIARequest):
        self.update_request(
            request,
            date_checked=datetime.datetime.now(),
            request_status=RequestStatus.PENDING.value,
        )

    def get_last_scrape_date(self, source: str) -> Optional[datetime.datetime]:
        try:
            return ScrapeMetadata.get(
                ScrapeMetadata.scrape_source == source
            ).last_scrape_date
        except Exception:
            return None

    def update_scrape_date(self, source: str):
        (
            ScrapeMetadata.insert(
                scrape_source=source, last_scrape_date=datetime.datetime.now()
            )
            .on_conflict(
                conflict_target=(ScrapeMetadata.scrape_source,),
                preserve=(ScrapeMetadata.id, ScrapeMetadata.scrape_source),
                update={ScrapeMetadata.last_scrape_date: datetime.datetime.now()},
            )
            .execute()
        )

    def get_open_requests(self):
        return FOIARequest.select().where(
            FOIARequest.request_status == RequestStatus.PENDING.value
        )

    def get_downloaded_requests(self, before_date: datetime.datetime = None):
        query = None
        if before_date:
            query = DocumentDownload.date_downloaded < before_date
        return self.get_downloads(query)

    def get_closed_requests(self):
        return FOIARequest.select().where(
            FOIARequest.request_status == RequestStatus.CLOSED.value
        )

    def get_error_requests(self):
        return FOIARequest.select().where(
            FOIARequest.request_status == RequestStatus.ERROR.value
        )

    def get_stats(self) -> DatabaseStats:
        all_requests = list(self.get_requests())
        pending = list(self.get_open_requests())
        downloaded = list(self.get_downloaded_requests())
        closed = list(self.get_closed_requests())
        error = list(self.get_error_requests())
        total_docs = sum([d.document_count for d in downloaded])
        last_scrape = ScrapeMetadata.get().last_scrape_date

        return DatabaseStats(
            total_request_count=len(all_requests),
            pending_request_count=len(pending),
            downloaded_request_count=len(downloaded),
            closed_request_count=len(closed),
            error_request_count=len(error),
            last_scrape=last_scrape,
            document_count=total_docs,
        )
