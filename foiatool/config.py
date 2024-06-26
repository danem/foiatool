import foiatool.data as fdb

import dataclasses
import pathlib
import toml
import os
from typing import List
import shutil

__FOIATOOLS_CONFIG__ = "foiatool.toml"
__FOIATOOLS_DB__ = "foia.db"
__FOIATOOLS_DIR__ = "foia"
__FOIATOOLS_DOWNLOAD__ = "downloads"


@dataclasses.dataclass
class RequestConfig:
    url: str
    user: str
    password: str
    search_terms: List[str]
    document_search_terms: List[str]
    ignore_ids: List[str]
    download_nice_seconds: int
    download_timeout_seconds: int
    download_path: str


@dataclasses.dataclass
class Config:
    db_path: str
    request_config: List[RequestConfig]


def default_config(root_dir: str):
    root_dir = pathlib.Path(root_dir)
    download_dir = root_dir / __FOIATOOLS_DOWNLOAD__
    download_dir.mkdir(parents=True, exist_ok=True)

    # Use relative paths to enable pain-free moving of the foia folder
    # Paths get translated to absolute paths when loading the config
    return Config(
        db_path=__FOIATOOLS_DB__,
        request_config=[
            RequestConfig(
                url="",
                user="",
                password="",
                search_terms=[],
                document_search_terms=[],
                ignore_ids=[],
                download_nice_seconds=2,
                download_timeout_seconds=1200,
                download_path=__FOIATOOLS_DOWNLOAD__,
            ),
        ],
    )


def init_project(path: str, overwrite=False) -> Config:
    init_dir = pathlib.Path(path) / __FOIATOOLS_DIR__
    if init_dir.exists():
        if not overwrite:
            raise Exception("foiatools is already initialized here.")
        else:
            shutil.rmtree(init_dir)
    init_dir.mkdir(parents=True, exist_ok=True)

    conf = default_config(init_dir.absolute())
    with open(init_dir / __FOIATOOLS_CONFIG__, "w") as f:
        toml.dump(dataclasses.asdict(conf), f)

    dbpath = init_dir / __FOIATOOLS_DB__
    if os.path.exists(dbpath):
        os.remove(dbpath)
    # Create db file
    fdb.DBSession(dbpath)

    return conf


def load_config(path: str) -> Config:
    path = os.path.abspath(path)
    if not os.path.exists(path):
        raise RuntimeError(f"Config not found at {path}")

    root_dir = os.path.dirname(path)
    with open(path, "r") as f:
        config = toml.load(f)

        request_configs = [RequestConfig(**v) for v in config["request_config"]]
        del config["request_config"]

        config = Config(request_config=request_configs, **config)

        # make paths absolute
        if not os.path.isabs(config.db_path):
            config.db_path = os.path.abspath(os.path.join(root_dir, config.db_path))

        for rc in config.request_config:
            if not os.path.isabs(rc.download_path):
                rc.download_path = os.path.abspath(
                    os.path.join(root_dir, rc.download_path)
                )

        return config


def find_project_dir(start_dir=None):
    if not start_dir:
        start_dir = os.getcwd()

    current = pathlib.Path(os.path.abspath(start_dir))

    while current.parent != current:
        if (current / __FOIATOOLS_CONFIG__).exists():
            return current
        current = current.parent

    return None


def find_config_path(start_dir=None):
    if pdir := find_project_dir(start_dir):
        path = os.path.join(pdir, __FOIATOOLS_CONFIG__)
        return str(path)
    return None
