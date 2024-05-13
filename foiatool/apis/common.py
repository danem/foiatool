import foiatool.config as fconfig

import requests
import pathlib
import tqdm
import os
import re
import urllib


def get_download_dir(config: fconfig.RequestConfig) -> pathlib.Path:
    url_parts = urllib.parse.urlparse(config.url)
    download_dir = pathlib.Path(config.download_path) / url_parts.netloc
    return download_dir


def truncate_file_name(file_name: str):
    fname, ext = os.path.splitext(file_name)
    tlen = 255 - len(ext)
    return fname[:tlen] + ext


def normalize_file_name(download_dir: str, request_id: str, file_name: str):
    # Some documents don't have requests associated with them
    folder_name = f"{request_id}" if request_id else "orphans"
    out_dir = pathlib.Path(download_dir) / folder_name
    out_dir.mkdir(parents=True, exist_ok=True)
    file_name = truncate_file_name(file_name)
    file_name = re.sub("[:/]", "_", file_name)
    return str(out_dir / file_name)


def download_file(
    session: requests.Session,
    url: str,
    outpath: str = None,
    display_progress: bool = False,
):
    resp = session.get(url, stream=True, allow_redirects=True)
    resp.raise_for_status()

    total_size = int(resp.headers.get("content-length", 0))
    block_size = 1024

    try:
        pbar = None
        if display_progress:
            pbar = tqdm.tqdm(total=total_size, unit="B", unit_scale=True)

        with open(outpath, "wb") as f:
            for chunk in resp.iter_content(chunk_size=block_size):
                read_amt = f.write(chunk)
                if pbar:
                    pbar.update(read_amt)
    finally:
        if pbar:
            pbar.close()


class DownloadException(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)


HTTPException = requests.HTTPError
ConnectionException = requests.ConnectionError
