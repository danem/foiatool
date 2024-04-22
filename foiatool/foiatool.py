import foiatool.config as fconfig
import foiatool.data as fdb
import foiatool.apis as fapi

import argparse
import tqdm
from typing import List, Optional, Union
import dateutil.parser as dparser
import logging
import time
import os
import urllib.parse
import concurrent
import datetime

def _parse_datetime(txt: str, permissive:bool = False):
    return dparser.parse(txt, fuzzy=permissive)

def get_user_choice (prompt, default = False):
    yeses = ["y", "yes", "1"]
    yn = "[Y/n]:" if default else "[y/N]:"

    choice = input(prompt + " " + yn)

    if choice.lower() in yeses or (len(choice) == 0 and default):
        return True
    return False
    

def fetch_new_requests (
    config: fconfig.RequestConfig, 
    dbsess: fdb.DBSession,
    driver: fapi.NextRequestAPI
):
    with dbsess.atomic():
        last_update = dbsess.get_last_scrape_date(config.url)
        initial_count = len(dbsess.get_tasks())
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
                    dbsess.add_bulk_download_task(item["id"])
                    pbar.update(1)

                # Be nice
                time.sleep(config.download_nice_seconds)
        pbar.close()

        # Search through documents
        pbar = tqdm.tqdm()
        pbar.set_description("Searching Documents")
        for term in config.document_search_terms:
            for page in driver.search_documents(term):
                for item in page:
                    # some documents aren't associated with a request...
                    request_id = item.get("pretty_id", None)
                    doc_id = item["id"]
                    title = item["title"]
                    fname = f"{doc_id}_{title}"

                    if not request_id or dbsess.get_download(request_id, doc_id):
                        continue

                    dbsess.add_download_task(request_id, doc_id, fname)
                    pbar.update(1)

                # Be nice
                time.sleep(config.download_nice_seconds)
        pbar.close()

        dbsess.update_scrape_date(config.url)

        new_count = len(dbsess.get_tasks())
        logging.info(f"Fetching complete. Found {new_count - initial_count} new documents")


def visit_pending_requests (
    config: fconfig.RequestConfig, 
    dbsess: fdb.DBSession,
    driver: fapi.NextRequestAPI
):
    pending = dbsess.get_tasks()

    logging.info(f"Found {len(pending)} requests in the queue. Visiting")
    logging.info(f"Ignoring requests: {config.ignore_ids}")

    driver.sign_in()

    error_count = 0
    pbar = tqdm.tqdm(pending)
    for req in pbar:
        pbar.set_description(f"Fetching request {req.task_target_id}")
        req_info = driver.get_request_info(req.task_target_id)
        req_status = fdb.status_from_str(req_info["request_state"])

        dept_names = req_info["department_names"]
        doc_info = driver.get_docs_info_for_request(req_info["pretty_id"])
        doc_count = doc_info.get("total_documents_count", 0)
        request_date = _parse_datetime(req_info.get("request_date", ""), True)

        foia_request = dbsess.add_request(
            config.url,
            req_info["pretty_id"],
            req_status,
            request_date,
            dept_names,
            doc_count
        )

        if req.task_type not in [fdb.TaskType.DOWNLOAD.value, fdb.TaskType.BULK_DOWNLOAD.value]:
            continue

        try:
            pbar.set_description(f"Waiting for documents to download for {req.task_target_id}")
            if req.task_type == fdb.TaskType.DOWNLOAD.value:
                promise = driver.download_document(req.task_target_id, req.document_id, req.document_name)
                result_path = promise.result(config.download_timeout_seconds)
                dbsess.add_download(foia_request, result_path, req.document_id)
            else:
                if req_status != fdb.RequestStatus.CLOSED:
                    continue
                promise = driver.download_docs_for_request(req.request_id)
                result_path = promise.result(config.download_timeout_seconds)
                dbsess.add_bulk_download(foia_request, result_path)
        except (TimeoutError, concurrent.futures.InvalidStateError, fapi.DownloadException, fapi.HTTPException, fapi.ConnectionException):
            # for some reason the document failed to download. Ignore this document for the time being
            dbsess.mark_request_error(foia_request)
            error_count += 1
            pbar.set_postfix({"errors": error_count})

        dbsess.mark_task_completed(req)
        # Be nice
        time.sleep(config.download_nice_seconds)

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


# Finds records whose downloaded document has been moved or deleted.
# This can fix the database if the whole project folder has been moved.
# TODO: I shouldn't store the entire download path, but instead the 
# path relative to the root download folder.
def repair_data (
    config: fconfig.RequestConfig,
    dbsess: fdb.DBSession,
    nrapi: fapi.NextRequestAPI
):
    logging.info("Creating downloaded document index")

    index = {}
    for dpath, _, files in os.walk(nrapi.download_dir()):
        for file in files:
            fpath = os.path.join(dpath, file)
            chksum = fdb.get_doc_md5(fpath)
            index[chksum] = fpath

    logging.info("Repairing data and ensuring integrity")

    pbar = tqdm.tqdm(dbsess.get_requests())
    bad_count = 0

    for req in pbar:
        pbar.set_description(f"Checking request: {req.request_id}")

        if req.request_id in config.ignore_ids:
            dbsess.mark_document_error(req)
            bad_count += 1

        elif req.request_status == fdb.RequestStatus.DOWNLOADED.value:
            if (not os.path.exists(req.document_paths)):
                if npath := index.get(req.download_checksum, ""):
                    dbsess.mark_document_downloaded(req, npath, req.document_count, req.download_checksum)
                else:
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
    dbsess.add_bulk_download_task(request_id)
    visit_pending_requests(config, dbsess, nrapi)


def main ():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", nargs="?", help="Path to config file. If not supplied, it will be automatically found")
    parser.add_argument("--no-search", default=False, action="store_true", help="Skip searching")


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

    init_parser = subparsers.add_parser("init", help="Initialize project")
    init_parser.add_argument("dir", nargs="?", default="./")

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    if args.cmd == "init":
        logging.info(f"Initializing foiatool in {os.path.abspath(args.dir)}")
        fconfig.init_project(os.path.abspath(args.dir))
        return

    if args.config:
        config = fconfig.load_config(args.config)
        logging.info(f"Found config at {args.config}")
    else:
        path = fconfig.find_config_path(os.getcwd())
        config = fconfig.load_config(path)
        logging.info(f"Found config at {path}")
    
    if not config:
        logging.error("No foiatool config found")
        return
    
    dbsess = fdb.DBSession(config.db_path)

    # Selenium not needed
    if args.cmd == "clear-pending":
        logging.info("Clearing pending queue")
        dbsess.clear_tasks()
        return
    elif args.cmd == "stats":
        stats = dbsess.get_stats()
        total = stats.total_request_count
        msg = f"""total_requests: {stats.total_request_count}
error_requests: {stats.error_request_count} ({stats.error_request_count / total})
pending_requests: {stats.pending_request_count} ({stats.pending_request_count / total})
closed_requests: {stats.closed_request_count} ({stats.closed_request_count / total})
downloaded_requests: {stats.downloaded_request_count} ({stats.downloaded_request_count / total})
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
            req_id = url_parts.path.strip("/").split("/")[-1]
            fetch_request(conf, dbsess, nrapi, req_id)
        else:
            logging.error(f"No configuration found for the provided url: {url_parts.netloc}")
            return
    else:
        for nrapi, conf in apis_lut.values():
            if not args.no_search:
                fetch_new_requests(conf, dbsess, nrapi)
            visit_pending_requests(conf, dbsess, nrapi)


if __name__ == "__main__":
    main()
