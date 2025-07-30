# Data Load

The data load was tested by running the data ingestion scripts agains the
DBDOCK_TEST_O1 database that is created in the docker compose at the root of
this project.  (not the lexis specific one located in the root of the lexis
folder.)

## Configure the environment Variables

In order to run the ingest you will need to following env vars to be populated,
so that the ingestion script can communicate with object store and the destination
database.

### Object Store Parameters

* OBJECT_STORE_USER_PROD=<user id>
* OBJECT_STORE_SECRET_PROD=<secret>
* OBJECT_STORE_BUCKET_PROD=<bucket name>
* OBJECT_STORE_HOST_PROD=nrs.objectstore.gov.bc.ca
* OBJECT_STORE_DATA_DIRECTORY=lexis/ora-db

### Local Database Parameters

* ORACLE_HOST_LOCAL=localhost
* ORACLE_PORT_LOCAL=1523
* ORACLE_SERVICE_LOCAL=dbdock_test_01
* ORACLE_SYNC_USER_LOCAL=THE
* ORACLE_SYNC_PASSWORD_LOCAL=default
* ORACLE_SCHEMA_TO_SYNC_LOCAL=THE

* LOCAL_DATA_DIR=<local cache for data>
* DATA_CLASSIFICATION_JSON=<path to the json for data classification>

### Misc Parameters

* LOCAL_DATA_DIR: When the data is extracted from the database it is first cached
    here, and then once the extraction is complete the data in this location is
    uploaded to object store.
* DATA_CLASSIFICATION_JSON: This is the path to the json doc.. `fixed_data_classification.json`
    that was generated in the data classification step.


## Run the load script

```
cd data-population
uv run python db_env_utils/main_ingest.py ORA PROD
```


