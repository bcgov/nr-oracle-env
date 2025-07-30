# Pull data from PROD

## Data classification

The data classification spreadsheets have been cached to this folder in the
 [FDS Sharepoint:](https://bcgov.sharepoint.com/teams/03678/Shared%20Documents/Forms/AllItems.aspx?id=%2Fteams%2F03678%2FShared%20Documents%2FGeneral%2FData%20Classifications%2FForests%5FTest%5FData%5FRefresh%2FData%5FClassification%5FSpreadsheets%2FApproved%5Fby%5FSecurity&viewid=d81a9f32%2Dd7fe%2D490a%2Dbf22%2Dcea9e64a79d5&csf=1&web=1&e=LprZSo&ovuser=6fdb5200%2D3d0d%2D4a8a%2Db036%2Dd3685e359adc%2CKevin%2ENetherton%40gov%2Ebc%2Eca&OR=Teams%2DHL&CT=1752704712726&clickparams=eyJBcHBOYW1lIjoiVGVhbXMtRGVza3RvcCIsIkFwcFZlcnNpb24iOiI0OS8yNTA2MTIxNjQyMSIsIkhhc0ZlZGVyYXRlZFVzZXIiOmZhbHNlfQ%3D%3D&CID=4578b2a1%2D40c2%2D9000%2D550d%2D2664c4b6e918&cidOR=SPO&FolderCTID=0x012000661E056E05762A4C9AEB04E04A706D67)

All the spreadsheets were downloaded to the `./data_classifications` folder.

the script `../../utils/consolidate_data_classifications.py` was run which
generated a consolidated data classification json doc `./data_classifications/consolidated_data_classification`.

Currently the data extraction script does not support masking primary / foreign
key columns so those columns were then removed creating the json doc
`./data_classifications/fixed_data_classifications.json`.  The script used
to create that is: `../utils/analyze_data_class`

## Data Extraction

The data extraction will use the data classification doc to identify which columns
should be masked.  In order to extract data from prod you will need to make
the following env vars available to the script.

### Required Environment Variables

#### Local Oracle Params

These are suggested values based on using the test database that is defined in
the docker compose at the root level of the repository.  If trying to build this
for the specific lexis database (created in the projects_specific/lexis folder)
you will likely need to change the paramters.

* ORACLE_HOST_LOCAL=localhost
* ORACLE_PORT_LOCAL=1523
* ORACLE_SERVICE_LOCAL=dbdock_test_01
* ORACLE_SYNC_USER_LOCAL=THE
* ORACLE_SYNC_PASSWORD_LOCAL=default
* ORACLE_SCHEMA_TO_SYNC_LOCAL=THE

#### Prod Oracle Parameters

* ORACLE_HOST_PROD=<database host or ip>
* ORACLE_PORT_PROD=<database port likely 1521>
* ORACLE_SERVICE_PROD=<database service name>
* ORACLE_USER_PROD=<database user id>
* ORACLE_PASSWORD_PROD=Igl00l<database user id password>
* ORACLE_SCHEMA_TO_SYNC_PROD=<schema that is being extracted from>


#### Object Store Parameters

These parameters allow the extraction script to communicate with object storage
which is where the data that is extracted will be cached.

* OBJECT_STORE_USER_PROD=<user id>
* OBJECT_STORE_SECRET_PROD=<secret>
* OBJECT_STORE_BUCKET_PROD=<bucket name>
* OBJECT_STORE_HOST_PROD=nrs.objectstore.gov.bc.ca
* OBJECT_STORE_DATA_DIRECTORY=lexis/ora-db

#### Misc Parameters

* LOCAL_DATA_DIR=<local cache for data>
* DATA_CLASSIFICATION_JSON=<path to the json for data classification>

* LOCAL_DATA_DIR: When the data is extracted from the database it is first cached
    here, and then once the extraction is complete the data in this location is
    uploaded to object store.
* DATA_CLASSIFICATION_JSON: This is the path to the json doc.. `fixed_data_classification.json`
    that was generated in the data classification step.

### Running the extract

Having populated all the various env vars defined above, you can now run the
script.

```bash
cd data-population
uv run python db_env_utils/main_extract ORA PROD
```