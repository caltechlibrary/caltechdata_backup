# caltechdata_read

[![DOI](https://data.caltech.edu/badge/81266861.svg)](https://data.caltech.edu/badge/latestdoi/81266861)

caltechdata_read queries the caltechDATA (Invenio 3) API, returns data, and adds
to dataset structure on disk

In development.  Requires dataset (https://github.com/caltechlibrary/dataset), requests (http://docs.python-requests.org/en/master/), and clint (https://github.com/kennethreitz/clint).

## Initialization

Create a collection by typing:
    
```shell
    dataset init caltechdata
    export DATASET=caltechdata
```

## Usage

```shell
   python caltechdata_read.py [-h]
```

optional arguments:
  -h, --help  show this help message and exit

