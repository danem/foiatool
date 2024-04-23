import foiatool.apis.common as common
import foiatool.config as fconfig

import requests
import concurrent.futures
import time
import urllib
import pathlib
import lxml.html

# TODO: Generalize this interface so we can support other platforms like GovQA
class NextRequestAPI:
    IS_OPEN = 1
    IS_CLOSED = 1 << 1
    REQUESTS_ENDPOINT = "requests"
    DOCUMENTS_ENDPOINT = "documents"

    def __init__(
        self, 
        url: str,
        download_dir: str,
        username: str,
        password: str
    ) -> None:
        self._url = url.strip()
        self._username = username
        self._password = password
        self._download_dir = download_dir
        self._authenticated = False

        self._session = requests.Session()
        self._session.headers["user-agent"] = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"

    def _get_csrf (self, page_txt: str):
        # TODO: Maybe there's a better way to do this...
        page = lxml.html.fromstring(page_txt)
        if meta_element := page.xpath("//meta[@name='csrf-token']"):
            return meta_element[0].get("content")
        return None
    
    def _get_session (self, csrf_url: str = None) -> requests.Session:
        if csrf_url:
            page = self._session.get(csrf_url)
            page.raise_for_status()

            csrf = self._get_csrf(page.content)
            self._session.headers.update({"x-csrf-token": csrf})

        return self._session
    
    def sign_in (self):
        if self._authenticated:
            return 

        url = f"{self._url}/users/sign_in"
        sess = self._get_session(url)

        csrf = sess.headers.get("x-csrf-token")
        payload = {
            "authenticity_token": csrf,
            "user[email]": self._username,
            "user[password]": self._password
        }
        resp = sess.post(url, params=payload)
        resp.raise_for_status()
        
        self._authenticated = True
    
    def _download_document (self, request_id, doc_id, fname):
        # TODO: If there were some API to get document info from doc_id 
        # I'd only need doc_id and not request_id or fname
        session = self._get_session()
        outpath = common.normalize_file_name(self._download_dir, request_id, fname)
        url = f"{self._url}/documents/{doc_id}/download"
        common.download_file(session, url, outpath)
        return outpath

    def _initiate_bulk_download (self, sess: requests.Session, request_id: str):
        doc_ids = [dd["id"] for dd in self.get_docs_info_for_request(request_id).get("documents", [])]

        post_data = dict(
            request_id = request_id,
            bulk_action = "download",
            all_selected = True,
            visibility = "all"
        )

        resp = sess.put(f"{self._url}/client/documents/bulk", json=post_data)
        resp.raise_for_status()

        job_id = resp.json().get("jobId", [None])[0]
        if not job_id:
            raise common.DownloadException(f"Unable to initiate download: {resp.status_code}")

        return job_id
    
    def _poll_background_job (self, sess: requests.Session, request_id: str, job_id: str, job_type: str):
        resp = sess.get(f"{self._url}/background_job_logs", params=dict(pretty_id = request_id))
        resp.raise_for_status()

        for job in resp.json().get("jobs", []):
            if job.get("id") == job_id and job.get("status", "") == "working":
                return True
        return False

    def _perform_bulk_download (self, request_id: str):
        session = self._get_session(f"{self._url}/requests/{request_id}")

        job_id = self._initiate_bulk_download(session, request_id)
        while self._poll_background_job(session, request_id, job_id, "zipfile_creator"):
            time.sleep(2) # Timing seen in browser

        params = dict(jid = job_id, request_id = request_id)
        resp = session.get(f"{self._url}/client/documents/download", params=params)
        resp.raise_for_status()

        data = resp.json()
        if not (url := data.get("url", "")) or not (fname := data.get("filename", "")):
            raise common.DownloadException(f"Error zipping document: {data.get('message', '')}")

        outpath = common.normalize_file_name(self._download_dir, request_id, fname)
        common.download_file(session, url, outpath, display_progress=False)

        return outpath

    def _perform_search (self, session: requests.Session, term: str, page: int, endpoint: str, open_mask: int = 0):
        params = dict(search_term = term, page_number = page)
        if open_mask & NextRequestAPI.IS_OPEN == NextRequestAPI.IS_OPEN:
            params["open"] = True
        if open_mask & NextRequestAPI.IS_CLOSED == NextRequestAPI.IS_CLOSED:
            params["closed"] = True
        
        return session.get(f"{self._url}/client/{endpoint}", params=params).json()
    
    def _search (self, term: str, endpoint: str, open_mask: int = 0):
        page = 0
        consumed = 0

        session = self._get_session()

        resp = self._perform_search(session, term, page, endpoint, open_mask)
        total_count = resp.get("total_count", 0)

        while consumed < total_count:
            reqs = resp.get(endpoint, [])
            consumed += len(reqs)
            yield reqs

            page += 1
            resp = self._perform_search(session, term, page, endpoint, open_mask)
    
    def search_requests (self, term: str, open_mask: int = 0):
        return self._search(term, NextRequestAPI.REQUESTS_ENDPOINT, open_mask)

    def search_documents (self, term: str):
        return self._search(term, NextRequestAPI.DOCUMENTS_ENDPOINT, 0)
        
    def get_request_info (self, req_id: str):
        return requests.get(f"{self._url}/client/{NextRequestAPI.REQUESTS_ENDPOINT}/{req_id}").json()
    
    # TODO: Can't find an API endpoint for this...
    # def get_document_info (self, doc_id: str):
    #     return requests.get(f"{self._url}/client/{NextRequestAPI.DOCUMENTS_ENDPOINT}/{doc_id}").json()
        
    def get_docs_info_for_request (self, req_id: str):
        params = dict(request_id = req_id)
        return requests.get(f"{self._url}/client/request_documents", params=params).json()
    
    def download_docs_for_request (self, request_id: str) -> concurrent.futures.Future:
        with concurrent.futures.ThreadPoolExecutor() as pool:
            promise = pool.submit(self._perform_bulk_download, request_id)
        return promise
    
    def download_document (self, request_id: str, doc_id: str, doc_name: str) -> concurrent.futures.Future:
        with concurrent.futures.ThreadPoolExecutor() as pool:
            promise = pool.submit(self._download_document, request_id, doc_id, doc_name)
        return promise
    
    def download_dir (self):
        return self._download_dir
    
    def url (self):
        return self._url

def initialize_nextrequest_client (
    config: fconfig.RequestConfig
) -> NextRequestAPI:
    download_dir = common.get_download_dir(config)
    download_dir.mkdir(parents=True, exist_ok=True)
    return NextRequestAPI(config.url, str(download_dir), config.user, config.password)