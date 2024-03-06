from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import watchdog.events as wevents
import watchdog.observers.polling as wobservers

import foiatool.apis.common as common

import requests
import concurrent.futures
import time
import threading
import urllib
import pathlib
import curlify

# TODO: Generalize this interface so we can support other platforms like GovQA
# TODO: Get rid of Selenium, not worth the trouble
class NextRequestAPI:
    IS_OPEN = 1
    IS_CLOSED = 1 << 1
    REQUESTS_ENDPOINT = "requests"
    DOCUMENTS_ENDPOINT = "documents"

    def __init__(
        self, 
        driver: webdriver.Chrome,
        url: str,
        download_dir: str,
        username: str,
        password: str
    ) -> None:
        self._url = url.strip()
        self._driver = driver
        self._username = username
        self._password = password
        self._download_dir = download_dir

    
    def _get_cookies (self):
        cookies = {c["name"]: c["value"] for c in self._driver.get_cookies()}
        return cookies
    
    def _get_headers (self):
        return {
            "accept": "application/json",
            "content-type": "application/json",
            "origin": f"{self._url}"
        }
    
    def sign_in (self):
        # TODO: Get rid of selenium. 
        self._driver.get(f"{self._url}/users/sign_in")

        email_field = self._driver.find_element(By.ID, "user_email")
        pass_field = self._driver.find_element(By.ID, "user_password")
        login_btn = self._driver.find_element(By.CSS_SELECTOR, "#new_user > div.form__actions > button")

        email_field.send_keys(self._username)
        pass_field.send_keys(self._password)
        login_btn.click()
    
    def _initiate_bulk_download (self, request_id: str):
        doc_ids = [dd["id"] for dd in self.get_docs_info_for_request(request_id).get("documents", [])]

        post_data = dict(
            request_id = request_id,
            bulk_action = "download",
            doc_ids = doc_ids
        )
        # TODO: I need to include the csrf token. Not sure how I will get it without selenium
        headers = {**self._get_headers(), "referrer": f"{self._url}/requests/{request_id}"}
        resp = requests.put(f"{self._url}/client/documents/bulk", json=post_data, allow_redirects=False, cookies=self._get_cookies(), headers=headers)
        print(curlify.to_curl(resp.request))
        resp.raise_for_status()

        print(resp.content)
        job_id = resp.json().get("jobId", [None])[0]
        if not job_id:
            raise Exception(f"Unable to initiate download: {resp.status_code}")

        return job_id
    
    def _poll_background_job (self, request_id: str, job_id: str, job_type: str):
        cookies = self._get_cookies()
        headers = self._get_headers()
        params = dict(pretty_id = request_id)

        resp = requests.get(f"{self._url}/background_job_logs", params=params, cookies=cookies, headers=headers)
        resp.raise_for_status()

        data = resp.json()
        for job in data.get("jobs", []):
            if job.get("id") == job_id and job.get("status", "") == "working":
                return True
        return False

    def _perform_bulk_download (self, request_id: str):
        job_id = self._initiate_bulk_download(request_id)
        while self._poll_background_job(request_id, job_id, "zipfile_creator"):
            time.sleep(2) # Timing seen in browser

        params = dict(jid = job_id, request_id = request_id)
        resp = requests.get(f"{self._url}/client/documents/download", params=params, cookies=self._get_cookies(), headers=self._get_headers())
        resp.raise_for_status()

        data = resp.json()
        url, fname = data["url"], data["filename"]

        outpath = common.normalize_file_name(self._download_dir, request_id, fname)
        common.download_file(url, outpath, cookies = self._get_cookies(), headers = self._get_headers(), display_progress=True)

        return outpath

    def _perform_search (self, term: str, page: int, endpoint: str, open_mask: int = 0):
        params = dict(search_term = term, page_number = page)
        if open_mask & NextRequestAPI.IS_OPEN == NextRequestAPI.IS_OPEN:
            params["open"] = True
        if open_mask & NextRequestAPI.IS_CLOSED == NextRequestAPI.IS_CLOSED:
            params["closed"] = True

        return requests.get(f"{self._url}/client/{endpoint}", params=params).json()
    
    def _search (self, term: str, endpoint: str, open_mask: int = 0):
        page = 0
        consumed = 0

        resp = self._perform_search(term, page, endpoint, open_mask)
        total_count = resp.get("total_count", 0)

        while consumed < total_count:
            reqs = resp.get(endpoint, [])
            consumed += len(reqs)
            yield reqs

            page += 1
            resp = self._perform_search(term, page, endpoint, open_mask)
    
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

def initialize_nextrequest_client (
    url: str,
    download_path: str,
    username: str,
    password: str,
    headless: bool
) -> NextRequestAPI:
    url_parts = urllib.parse.urlparse(url)
    download_dir = pathlib.Path(download_path) / url_parts.netloc
    download_dir.mkdir(exist_ok=True)
    download_dir = str(download_dir)

    driver = common.initialize_selenium(download_dir, headless)
    return NextRequestAPI(driver, url, download_dir, username, password)