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
import urllib
import pathlib

# TODO: Generalize this interface so we can support other platforms like GovQA
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

        self._monitor = common.FolderMonitor(download_dir)
        

    def sign_in (self):
        self._driver.get(f"{self._url}/users/sign_in")

        email_field = self._driver.find_element(By.ID, "user_email")
        pass_field = self._driver.find_element(By.ID, "user_password")
        login_btn = self._driver.find_element(By.CSS_SELECTOR, "#new_user > div.form__actions > button")

        email_field.send_keys(self._username)
        pass_field.send_keys(self._password)
        login_btn.click()

    
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
    
    def download_docs_for_request (self, req_id: str) -> concurrent.futures.Future:
        self._driver.get(f"{self._url}/requests/{req_id}")
        waiter = WebDriverWait(self._driver, 30)

        doc_tab = waiter.until(
            EC.presence_of_element_located((By.CSS_SELECTOR,  "button[tabindex='-1']"))
        )
        doc_tab.click()

        download_all_chk = waiter.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "input[aria-labelledby='select-all-documents-label']"))
        )
        time.sleep(2) # Hack
        download_all_chk.click()


        time.sleep(2) # Hack
        if self._driver.find_elements(By.CLASS_NAME, "select-all-documents-view-button"):
            select_all_btn = waiter.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".select-all-documents-view-button"))
            )
            time.sleep(1) # Hack
            select_all_btn.click()


        # Start waiting before we actually initiate the download to avoid deadlocks
        promise = self._monitor.wait()

        download_btn = waiter.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "button[aria-label='Download documents']" ))
        )
        download_btn.click()
        return promise
    
    def download_doc_by_id (self, doc_id: str):
        self._driver.get(f"{self._url}/documents/{doc_id}")
        waiter = WebDriverWait(self._driver, 30)

        download_btn = waiter.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".fas.fa-download.qa-download-doc-icon"))
        )
        download_btn.click()



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