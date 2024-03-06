import peewee as pw
import enum
import datetime
from typing import List, Tuple, Union, Optional
import dataclasses

class RequestStatus (enum.Enum):
    PENDING = 1
    CLOSED = 2
    DOWNLOADED = 3
    ERROR = 4

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
# switch to something better. This has advantage of allowing us
# to easily keep track of stats, history, etc on top of the queue.
db = pw.SqliteDatabase("")

class DocumentRequest (pw.Model):
    id = pw.PrimaryKeyField()
    date_submitted = pw.DateField()
    date_checked = pw.DateField()
    date_downloaded = pw.DateField(null = True)
    document_paths = pw.CharField()
    department = pw.CharField()
    scrape_source = pw.CharField()
    request_id = pw.CharField()
    request_status = pw.IntegerField()
    document_count = pw.IntegerField()
    needs_download = pw.BooleanField(default=True)

    class Meta:
        database = db
        constraints = [pw.SQL("UNIQUE (scrape_source, request_id)")]

class ScrapeMetadata (pw.Model):
    id = pw.PrimaryKeyField()
    scrape_source = pw.CharField()
    last_scrape_date = pw.DateField()

    class Meta:
        database = db
        constraints = [pw.SQL("UNIQUE (scrape_source)")]
    

class DBSession:
    def __init__(self,  db_path: str) -> None:
        db.init(db_path)
        db.connect()
        db.create_tables([
            DocumentRequest,
            ScrapeMetadata
        ])
    
    def get_requests (
        self,
        query = None
    ):
        return DocumentRequest.select().where(query)
    
    def get_request (
        self,
        request_id: str,
        scrape_source: str,
    ) -> Optional[DocumentRequest]:
        return (DocumentRequest.get_or_none(
            (DocumentRequest.scrape_source == scrape_source) & 
            (DocumentRequest.request_id == request_id)
        ))
    
    def update_request (self, req: DocumentRequest, **kwargs):
        where = DocumentRequest.id == req.id
        DocumentRequest.update(
            **kwargs
        ).where(where).execute()
    
    
    def add_pending_document_request (
        self,
        scrape_source: str,
        request_id: str,
        needs_download: bool = True # Sometimes we want to just refresh the metadata
    ):
        if req := self.get_request(request_id, scrape_source):
            self.mark_document_pending(req)
            return req
        else:
            req = (DocumentRequest.create(
                scrape_source = scrape_source,
                request_id = request_id,
                needs_download = needs_download,
                request_status = RequestStatus.PENDING.value,
                date_submitted = datetime.datetime.now(),
                date_checked = datetime.datetime.now(), # TODO: Hack, need to update the schema
                document_paths = "",
                department = "",
                document_count = 0
            ))
            return req

    def update_document_metadata (
        self,
        request: DocumentRequest,
        date_submitted: datetime.datetime,
        document_count: int,
        department: str
    ):
        self.update_request(
            request,
            date_submitted = date_submitted,
            document_count = document_count,
            department = department,
            date_checked = datetime.datetime.now()
        )
    
    def mark_document_downloaded (
        self,
        request: DocumentRequest,
        paths: str,
        count: int = 0
    ):
        self.update_request(
            request,
            request_status = RequestStatus.DOWNLOADED.value,
            document_paths = paths,
            date_checked = datetime.datetime.now(),
            date_downloaded = datetime.datetime.now(),
            document_count = count,
            needs_download = False
        )

    def mark_document_closed (self, request: DocumentRequest):
        self.update_request(
            request, 
            date_checked = datetime.datetime.now(),
            request_status = RequestStatus.CLOSED.value
        )

    def mark_document_error (self, request: DocumentRequest):
        self.update_request(
            request, 
            date_checked = datetime.datetime.now(),
            request_status = RequestStatus.ERROR.value
        )


    def mark_document_pending (self, request: DocumentRequest, download = True):
        self.update_request(
            request, 
            date_checked = datetime.datetime.now(),
            request_status = RequestStatus.PENDING.value,
            needs_download = download
        )
    

    def get_last_scrape_date (self, source: str) -> Optional[datetime.datetime]:
        try:
            return ScrapeMetadata.get(ScrapeMetadata.scrape_source == source).last_scrape_date
        except:
            return None

    def update_scrape_date (self, source: str):
        (ScrapeMetadata.insert(
            scrape_source = source,
            last_scrape_date = datetime.datetime.now()
        ).on_conflict(
            conflict_target = (ScrapeMetadata.scrape_source,),
            preserve = (ScrapeMetadata.id, ScrapeMetadata.scrape_source),
            update = {ScrapeMetadata.last_scrape_date: datetime.datetime.now()}
        ).execute())

    def get_open_requests (self):
        return DocumentRequest.select().where(DocumentRequest.request_status == RequestStatus.PENDING.value)

    def remove_pending (self):
        return DocumentRequest.delete().where(DocumentRequest.request_status == RequestStatus.PENDING.value).execute()

    def get_downloaded_requests (self, before_date: datetime.datetime = None):
        query = DocumentRequest.request_status == RequestStatus.DOWNLOADED.value
        if before_date:
            query = query & (DocumentRequest.date_downloaded < before_date)
        return self.get_requests(query)

    def get_closed_requests (self):
        return DocumentRequest.select().where(DocumentRequest.request_status == RequestStatus.CLOSED.value)

    def get_error_requests (self):
        return DocumentRequest.select().where(DocumentRequest.request_status == RequestStatus.ERROR.value)
    
    def get_stats (self) -> DatabaseStats:
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
            document_count=total_docs
        )
        






