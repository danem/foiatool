import foiatool.config as fconfig
import foiatool.data as fdb
import foiatool.apis as fapi

import argparse
import tqdm
from typing import List, Optional, Union
import datetime
import concurrent.futures
import dateutil.parser as dparser
import logging
import time
import os
import pathlib
import urllib.parse

def _get_download_dir_for_config ():
    pass

def _parse_datetime(txt: str, permissive:bool = False):
    return dparser.parse(txt, fuzzy=permissive)

def get_user_choice (prompt, default = False):
    yeses = ["y", "yes", "1"]
    yn = "[Y/n]:" if default else "[y/N]:"

    choice = input(prompt + " " + yn)

    if choice.lower() in yeses or (len(choice) == 0 and default):
        return True
    return False

# Safely overwrite existing files. This is necessary because of how
# selenium works. AFAIK there's no easy way to monitor downloads etc
class OverWriteGuard:
    def __init__ (self, path):
        path = path if path else ""
        self._path = path
        self._backup = path + ".bak"
    
    def __enter__ (self):
        if self._path and os.path.exists(self._path):
            os.rename(self._path, self._backup)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not os.path.exists(self._path) and os.path.exists(self._backup):
            os.rename(self._backup, self._path)
        else:
            if os.path.exists(self._backup):
                os.remove(self._backup)

def normalize_file_name (
    config: fconfig.RequestConfig,
    request: fdb.DocumentRequest,
    file_name: str
) -> pathlib.Path:
    doc_date = request.date_submitted.strftime("%Y%m%d")
    folder_name = f"{request.request_id}_{doc_date}"
    out_path = pathlib.Path(config.url) / folder_name
    if os.path.splitext(file_name)[-1] not in [".zip"]:
        out_path /= os.path.basename(file_name)
    else:
        out_path = out_path.with_suffix(".zip")
    return out_path


def fetch_new_requests (
    config: fconfig.RequestConfig, 
    dbsess: fdb.DBSession,
    driver: fapi.NextRequestAPI
):
    last_update = dbsess.get_last_scrape_date(config.url)
    initial_count = len(dbsess.get_requests())
    logging.info(f"Fetching latest request updates for search terms {config.search_terms}")
    logging.info(f"Fetching latest documents for search terms {config.document_search_terms}")
    logging.info(f"Index last updated on {last_update}")

    # Search foia requests
    pbar = tqdm.tqdm()
    pbar.set_description("Searching Requests")
    for term in config.search_terms:
        for page in driver.search_requests(term, fapi.NextRequestAPI.IS_CLOSED):
            for item in page:
                if dbsess.get_request(item["id"], config.url) or item["id"] in config.ignore_ids:
                    continue
                dbsess.add_pending_document_request(config.url, item.get("id"))
                pbar.update(1)

            # Be nice
            time.sleep(config.download_nice_seconds)
    pbar.close()

    # TODO: It may be possible that there are documents that appear in the document search portal, 
    # but aren't available for download via the request download page. I haven't seen any instance
    # of this myself, but it is something to look out for, and the code as is, doesn't handle it.
    # A more robust approach would require changing the DB schema, and complicating a bunch of other
    # thigs, so I won't address it until I need to.

    # Search through documents
    pbar = tqdm.tqdm()
    pbar.set_description("Searching Documents")
    for term in config.document_search_terms:
        for page in driver.search_documents(term):
            for item in page:
                # Make sure we haven't already downloaded all of the documents from this request
                parent_request = dbsess.get_request(item.get("pretty_id"), config.url)
                if parent_request and parent_request.request_status == fdb.RequestStatus.DOWNLOADED:
                    continue

                dbsess.add_pending_document_request(config.url, item.get("pretty_id"))
                pbar.update(1)

            # Be nice
            time.sleep(config.download_nice_seconds)
    pbar.close()

    dbsess.update_scrape_date(config.url)

    new_count = len(dbsess.get_requests())
    logging.info(f"Fetching complete. Found {new_count - initial_count} new documents")


def visit_pending_requests (
    config: fconfig.RequestConfig, 
    dbsess: fdb.DBSession,
    driver: fapi.NextRequestAPI
):
    pending = dbsess.get_open_requests()
    pending = [r for r in pending if r.request_id not in config.ignore_ids]

    logging.info(f"Found {len(pending)} requests in the queue. Visiting")
    logging.info(f"Ignoring requests: {config.ignore_ids}")

    driver.sign_in()

    pbar = tqdm.tqdm(pending)
    for req in pbar:
        # Be nice
        time.sleep(config.download_nice_seconds)

        pbar.set_description(f"Fetching request {req.request_id}")
        info = driver.get_request_info(req.request_id)
        if info.get("request_state") != "Closed":
            continue
        
        req_info = driver.get_request_info(req.request_id)
        doc_info = driver.get_docs_info_for_request(req.request_id)
        doc_count = doc_info.get("total_documents_count", 0)
        # TODO: Not 100% sure this can be trusted
        if doc_count == 0:
            dbsess.mark_document_closed(req)
            continue

        request_date = _parse_datetime(req_info.get("request_date", ""), True)
        
        # Set request metadata
        dbsess.update_document_metadata(
            req,
            document_count = doc_count,
            department = req_info.get("department_names"),
            date_submitted = request_date
        )

        if not req.needs_download:
            continue

        with OverWriteGuard(req.document_paths) as _:
            try:
                promise = driver.download_docs_for_request(req.request_id)
                pbar.set_description(f"Waiting for documents to download for {req.request_id}")
                result = promise.result(config.download_timeout_seconds)

                new_path = normalize_file_name(config, req, result)
                new_path.parent.mkdir(parents=True, exist_ok=True)
                pathlib.Path(result).rename(new_path)

                dbsess.mark_document_downloaded(req, str(new_path), doc_count)
            except (TimeoutError, concurrent.futures.InvalidStateError):
                # for some reason the document failed to download. Ignore this document
                dbsess.mark_document_error(req)

def redownload_requests (
    config: fconfig.RequestConfig,
    dbsess: fdb.DBSession,
    nrapi: fapi.NextRequestAPI,
    before_date: datetime.datetime = None
):
    logging.info(f"Redownloading documents")

    reqs = dbsess.get_downloaded_requests(before_date)
    for req in reqs:
        # Put the requests back on the work queue
        dbsess.mark_document_pending(req)

    visit_pending_requests(config, dbsess, nrapi)


def repair_data (
    config: fconfig.RequestConfig,
    dbsess: fdb.DBSession,
    nrapi: fapi.NextRequestAPI
):
    logging.info("Repairing data and ensuring integrity")
    pbar = tqdm.tqdm(dbsess.get_requests())
    bad_count = 0

    for req in pbar:
        pbar.set_description(f"Checking request: {req.request_id}")

        fname = os.path.basename(req.document_paths)
        if req.request_id in config.ignore_ids:
            dbsess.mark_document_error(req)
            bad_count += 1

        elif req.request_status == fdb.RequestStatus.DOWNLOADED.value:
            if (not os.path.exists(req.document_paths) 
                or fname.startswith(".com.google.Chrome")):
                dbsess.mark_document_pending(req)
                bad_count += 1
    
    logging.info(f"Found {bad_count} broken records")
    visit_pending_requests(config, dbsess, nrapi)

def fetch_request (
    config: fconfig.RequestConfig,
    dbsess: fdb.DBSession,
    nrapi: fapi.NextRequestAPI,
    request_id
):
    logging.info("Fetching a single request")
    dbsess.add_pending_document_request(config.url, request_id, True)
    visit_pending_requests(config, dbsess, nrapi)


def main ():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", nargs="?", help="Path to config file. If not supplied, it will be automatically found")

    subparsers = parser.add_subparsers(title='action', dest='cmd')

    clear_cache_cmd = subparsers.add_parser("clear-pending")

    redownload_cmd = subparsers.add_parser("redownload", help="Re-download files. Useful for hanlding uncaught scraper bugs and data-loss")
    time_group = redownload_cmd.add_mutually_exclusive_group()
    time_group.add_argument("--before", nargs="?", type=datetime.datetime.fromisoformat, help="Only re-download files that were downloaded before the specified date")
    time_group.add_argument("--today", action="store_true", default=False, help="Only re-download files that were downloaded before today")

    refresh_cmd = subparsers.add_parser("repair")
    fetch_cmd = subparsers.add_parser("fetch", help="Fetch a single request specified by its ID")
    fetch_cmd.add_argument("request_url", help="NextRequest request url or document url")

    subparsers.add_parser("stats", help="Display stats about the database")

    subparsers.add_parser("schedule", help="Schedule foiatool to run daily")
    subparsers.add_parser("unschedule", help="Unschedule foiatool")

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    if args.config:
        config = fconfig.load_config(args.config)
    else:
        config = fconfig.find_config(os.getcwd())
    
    if msg := fconfig.verify_config(config):
        logging.error(msg)
        return

    dbsess = fdb.DBSession(config.db_path)

    # Selenium not needed
    if args.cmd == "clear-pending":
        logging.info("Clearing pending queue")
        dbsess.remove_pending()
        return
    elif args.cmd == "stats":
        stats = dbsess.get_stats()
        total = stats.total_request_count
        msg = f"""total_requests: {stats.total_request_count}
error_requests: {stats.error_request_count} ({stats.error_request_count / total}%)
pending_requests: {stats.pending_request_count} ({stats.pending_request_count / total}%)
closed_requests: {stats.closed_request_count} ({stats.closed_request_count / total}%)
downloaded_requests: {stats.downloaded_request_count} ({stats.downloaded_request_count / total}%)
document_count: {stats.document_count}"""
        print(msg)
        return
    
    # Initialize API clients
    apis_lut = {}
    for rc in config.request_config:
        url_parts = urllib.parse.urlparse(rc.url)
        api = fapi.initialize_nextrequest_client(
            rc.url,
            config.download_path,
            rc.user, rc.password,
            config.selenium_headless
        )
        apis_lut[url_parts.netloc] = (api, rc)


    if args.cmd == "redownload":
        before = args.before
        if args.today:
            before = datetime.datetime.today()
        for nrapi, conf in apis_lut.values():
            redownload_requests(conf, dbsess, nrapi, before)
    elif args.cmd == "repair":
        for nrapi, conf in apis_lut.values():
            repair_data(conf, dbsess, nrapi)
    elif args.cmd == "fetch":
        url_parts = urllib.parse.urlparse(args.request_url)
        if res := apis_lut.get(url_parts.netloc):
            nrapi, conf = res
            fetch_request(conf, dbsess, nrapi, url_parts.path)
        else:
            logging.error(f"No configuration found for the provided url: {url_parts.netloc}")
            return
    else:
        for nrapi, conf in apis_lut.values():
            fetch_new_requests(conf, dbsess, nrapi)
            visit_pending_requests(conf, dbsess, nrapi)


if __name__ == "__main__":
    main()
