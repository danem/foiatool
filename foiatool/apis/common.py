import requests
import pathlib
import tqdm

def normalize_file_name (
    download_dir: str,
    request_id: str,
    file_name: str
):
    folder_name = f"{request_id}"
    out_dir = pathlib.Path(download_dir) / folder_name
    out_dir.mkdir(parents=True, exist_ok=True)
    return str(out_dir / file_name)

def download_file(session: requests.Session, url: str, outpath: str, display_progress: bool = False):
    resp = session.get(url, stream=True, allow_redirects=True)
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
