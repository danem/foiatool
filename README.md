#### FOIATool

Tool for journalists, researchers, and concerned citizens to gather documents from NextRequest based FOIA portals. You can use this tool to periodically search for and download documents related to topics of interest.

##### Installation

```
python3 -m pip install .
```

##### Basic Usage

Generate a config file:

```
foiatool init
```

Edit the config file `foia/foiatool.toml`:

```toml
db_path = "foia.db"

[[request_config]]
url = "https://sanfrancisco.nextrequest.com"
user = "<USER NAME>"
password = "<PASSWORD>"
search_terms = ["police"]
document_search_terms = ["budget"]
ignore_ids = ["123"]
download_timeout = 1200
download_nice_seconds = 2
download_path = "downloads"

[[request_config]]
url = "https://another_site.nextrequest.com"
user = "<USER NAME>"
password = "<PASSWORD>"
search_terms = []
document_search_terms = []
ignore_ids = []
download_timeout = 1200
download_nice_seconds = 2
download_path = "downloads"
```

Paths can be specified as absolute or relative paths.

Run downloader:
```
foiatool 
```

##### Example FOIA Portals

- San Francisco
    - [Main Portal](https://sanfrancisco.nextrequest.com)
    - [BART](https://bart.nextrequest.com)
    - [Department of Police Accountability](https://sfdpa.nextrequest.com)


##### Future

- Support GovQA based portals
- Enable multi-threaded downloading
- Provide an easy cross-platform way to schedule the tool

##### Requirements

- peewee
- requests

