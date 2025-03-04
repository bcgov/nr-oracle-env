# Description

This folder contains tooling that is used to extract and impoort data.

* extract: from on prem databases to file formats cached in object store.
* load:    from object store cache to local "just enough oracle" env.

The intent of this tooling is to provide a local "data/database" environment
where developers can work on new and / or legacy applications that interact
with the "traditional" on-prem databases, safely by creating disposable easily
recreated local versions of these databases.

## Summary

The tool in the data-population folder will read a local version of the oracle
database, and extract the tables that have been defined in that database.

The tooling will then iterate over that table list and extract the data from an
on-prem instance for the tables that exist in the dockerized oracle database.
The data that is extracted is then stored as parquet files in object storage.

The import/ingest process loads the data from object store to the docker
oracle database.

# Extract Data

## Pre-requisites

* [Access to on-prem database](#access-to-on-prem-database)
* [Local dockerized oracle database](#dockerized-version-of-local-db)
* [Object store bucket to cache data](#object-store-bucket)

### Access to on-prem database

Run necessary vpn, and any other tweaks to verify that you can connect to the
oracle database from the location where the data is being extracted from (local
machine)

Easiest way to do this is ping the database host.

### Dockerized version of local db

This is a locally running version of a "just enough oracle" database.  It is
an oracle database, where DDL (migrations) have been successfully run, but
currently does not contain any data.

This process will read the table from this database in order to determine what
data needs to be extracted from the on-prem database.

### Object store bucket

All data that is extracted is stored in an object store bucket.  Data import/ingest
processes will get the data from the object store bucket.

## Run Extract

### Populate the following environment variables

``` bash
# local database parameters - This is the database that is typically running
#  locally that contains the database migrations.
ORACLE_HOST_LOCAL=localhost
ORACLE_PORT_LOCAL=1521
ORACLE_SERVICE_LOCAL=dbdock_01
ORACLE_SYNC_USER_LOCAL=THE
ORACLE_SYNC_PASSWORD_LOCAL=default
ORACLE_SCHEMA_TO_SYNC_LOCAL

# on prem database parameters (example below is for 'TEST')
ORACLE_HOST_TEST=<host>
ORACLE_PORT_TEST=<port likely 1521>
ORACLE_SERVICE_TEST=<db service name>
ORACLE_USER_TEST=<db username for connection to data>
ORACLE_PASSWORD_TEST=<db password>

# object store parameters (example below is for 'TEST')
OBJECT_STORE_USER_TEST=<username to connect to object store>
OBJECT_STORE_SECRET_TEST=<secret to connect>
OBJECT_STORE_BUCKET_TEST=<bucket name>
OBJECT_STORE_HOST_TEST=nrs.objectstore.gov.bc.ca
# path convension is <team>/ora-db example: silva/ora-db
OBJECT_STORE_DATA_DIRECTORY=<path where files are located in object store>
ORACLE_SCHEMA_TO_SYNC_TEST=THE
```

### Run the script

`uv run python db_env_utils/main_extract.py

