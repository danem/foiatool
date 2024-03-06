import requests
import pathlib
import tqdm

from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service


def normalize_file_name (
    download_dir: str,
    request_id: str,
    file_name: str
):
    folder_name = f"{request_id}"
    out_dir = pathlib.Path(download_dir) / folder_name
    out_dir.mkdir(parents=True, exist_ok=True)
    return str(out_dir / file_name)

def download_file(url: str, outpath: str, cookies = {}, headers = {}, display_progress: bool = False):
    resp = requests.get(url, stream=True, allow_redirects=True, cookies=cookies, headers=headers)
    resp.raise_for_status()

    total_size = int(resp.headers.get("content-length", 0))
    block_size = 1024

    try:
        pbar = None
        if display_progress:
            pbar = tqdm.tqdm(total=total_size, unit = "B", unit_scale=True) 

        with open(outpath, 'wb') as f:
            for chunk in resp.iter_content(chunk_size=block_size): 
                read_amt = f.write(chunk)
                if pbar:
                    pbar.update(read_amt)
    finally:
        pbar.close()

def initialize_selenium (download_dir: str, headless: bool):
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_experimental_option("prefs", {
        "download.default_directory": download_dir
    })
    if headless:
        chrome_options.add_argument("--headless")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    return driver