
# Data Load - Overview

This process will pull data files (duckdb format) from object storage, cache
them locally and then load the to the oracle test database.

The script does the following to accomplish the load:

* pull the data from object store
* disable the constraints
* load the data classifications
* iterate table by table loading the data
* if a column is defined as 'classified' dummy values are injected in based
    on how the classification object is configured.
* re-enable the constraints,
* if constraints fail, then script will identify the records that are causing
    the issue and deletes them, then retries

Other misc details
* no blob data
* spatial data only bounding boxes are loaded.

# Configuration / Setup

### 1. Environment variables

The data load needs the following environment variables to be defined.

-- object store env vars
* OBJECT_STORE_USER_<env>=
* OBJECT_STORE_SECRET_<env>=
* OBJECT_STORE_BUCKET_<env>=
* OBJECT_STORE_HOST_<env>=

-- local database parameters
* ORACLE_HOST_LOCAL=localhost
* ORACLE_PORT_LOCAL=1523
* ORACLE_SERVICE_LOCAL=dbdock_test_01
* ORACLE_SYNC_USER_LOCAL=THE
* ORACLE_SYNC_PASSWORD_LOCAL=default
* ORACLE_SCHEMA_TO_SYNC_LOCAL=THE

-- optional env vars:
* OBJECT_STORE_DATA_DIRECTORY - the path in object store where the data files
    should be imported / exported from
* DATA_CLASSIFICATION_SS - the path to the data classification spreadsheet
* LOCAL_DATA_DIR - the local path where data will be cached after it has
    been retrieved from object store.


### 2. Database

The oracle test database needs to up and runing.

`docker compose up oracle-test`

### 3. Data Classification

The script uses the data classification config to identify columns that should
be populated with fake data on load.

Copy the spreadsheet from the fsa object store bucket to:
`data-population/data/temp/data_classification.xlsx`


# Data Load

`uv run python db_env_utils/main_ingest.py`
