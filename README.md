# caltechdata_read

[![DOI](https://data.caltech.edu/badge/81266861.svg)](https://data.caltech.edu/badge/latestdoi/81266861)

caltechdata_read queries the caltechDATA (Invenio 3) API, returns data, and adds
to dataset structure on disk

In development.  Requires Python 3 (Recommended via Anaconda https://www.anaconda.com/download) with reqests and clint (pip install clint).  Requires dataset (https://github.com/caltechlibrary/dataset)).

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

