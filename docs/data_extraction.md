# Data Extraction

This step will extract data from an on prem database to duckdb files in object
storage.

# Configuration

## 1. Environment

You need to set up a significant number of environment variables in order to
be able to run the extract.  The fastest way to do this is to rename and or copy
`envsample` to  `.env` and populate the definitions in that file and then load
them as environment variables using something like:

`set -a; source .env; set +a`

In order to be able to support different environments, environment variables
are named according to the environments that they are for.

### Local Database Environment Variables
These are the env vars that are used to connect to the local database.

```
ORACLE_HOST_LOCAL=localhost
ORACLE_PORT_LOCAL=1523
ORACLE_SERVICE_LOCAL=dbdock_test_01
ORACLE_SYNC_USER_LOCAL=THE
ORACLE_SYNC_PASSWORD_LOCAL=default
ORACLE_SCHEMA_TO_SYNC_LOCAL=THE
```

### Object Store Environment Variables

Replace <env> with either TEST or PROD depending on the env you are describing.

```
OBJECT_STORE_USER_<env>=<access key id>
OBJECT_STORE_SECRET_<env>=<secret access key>
OBJECT_STORE_BUCKET_<env>=<bucket name>
OBJECT_STORE_HOST_<env>=nrs.objectstore.gov.bc.ca
OBJECT_STORE_DATA_DIRECTORY=<directory for the data>
```

### On prem database

Replace <env> with either TEST or PROD depending on the env you are describing.

ORACLE_HOST_<env>=<database host>
ORACLE_PORT_<env>=<database port>
ORACLE_SERVICE_<env>=<oracle service name>
ORACLE_USER_<env>=<oracle user>
ORACLE_PASSWORD_<env>=<oracle user password>
ORACLE_SCHEMA_TO_SYNC_<env>=THE

## 2. Network

The script needs to be run from a location that has access to the database
servers.  The most common solution to this issue is to open up a vpn connection
allowing communication directly from your local machine to the database.

## 3. Local Database

The script queries the tables in the 'THE' Schema to identify what objects need
to be extracted.  If the database is not already running...

`docker compose up oracle-test`

## 4. Data Classification Spreadsheet

The data classification spreadsheet is cached in the FSA object store bucket in
the folder data_classification.

You need to copy that spreadsheet from object store to `data-population/data/temp/data_classification.xlsx`

# Run Extract

Having addressed all of the above configuration steps, you can now proceed with
running the extraction script.

Navigate to the `data-population` directory.

`cd data-population`

and run the script:

`uv run python db_env_utils/main_extract.py ORA PROD`

Depending on the amount of data that is being pulled, the script can take a long
up to 3 hours to run.

## About Extract

* extract does not pull any blob/clob data
* data classifications are stored either in the data classification spreadsheet
    and is also augemented in the constants module in the DATA_TO_MASK variable.
* any geometry/spatial data only extracts the bounding box for the data.
* custom configuration can be added to the constants modules BIG_DATA_FILTERS
    which defined where clause to be applied to any large tables.