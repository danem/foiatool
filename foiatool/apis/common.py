import watchdog.events as wevents
import watchdog.observers.polling as wobservers
import concurrent.futures
import requests

from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support import expected_conditions as EC

# TODO: This assumes single threaded scraping. This is probably ok for now.
# Given we're using selenium and there doesn't seem to be an easy way to access
# the download queue programatically we won't be able to do multi-threaded
# scraping any time soon. Perhaps multi-process would work. But given the volume
# of pages, we probably can get away with a single thread solution
class DownloadBlocker (wevents.FileSystemEventHandler):
    def __init__(self) -> None:
        super().__init__()
        self._promise = None

    def _release (self, path: str):
        if self._promise is not None:
            try:
                self._promise.set_result(path)
            except concurrent.futures.InvalidStateError:
                # TODO: This happens when a download has multiple parts.
                # This should still work so I'm not fixing this at the moment
                pass
    
    def wait (self) -> concurrent.futures.Future:
        self._promise = concurrent.futures.Future()
        return self._promise
    
    def on_created(self, event):
        super().on_created(event)

        # TODO: For large files, chrome writes chunks labeled .crdownload.
        # I haven't thoroughly tested this, but it seems to work reliably
        if not event.src_path.endswith("crdownload") and not event.src_path.startswith(".com.google.Chrome"):
            self._release(event.src_path)
    
    def on_moved(self, event):
        super().on_moved(event)
        self._release(event.dest_path)

class FolderMonitor:
    """Allow a thread to block until a file has been created in the specified folder"""
    def __init__(self, folder: str) -> None:
        self._blocker = DownloadBlocker()
        
        self._observer = wobservers.PollingObserver()
        self._observer.schedule(self._blocker, folder)
        self._observer.start()
    
    def wait (self) -> concurrent.futures.Future:
        return self._blocker.wait()

def download_file(url: str, outpath: str):
    with requests.get(url, stream=True, allow_redirects=True) as r:
        r.raise_for_status()
        with open(outpath, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192): 
                f.write(chunk)

def initialize_selenium (download_dir: str, headless: bool):
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_experimental_option("prefs", {
        "download.default_directory": download_dir
    })
    if headless:
        chrome_options.add_argument("--headless")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    return driver