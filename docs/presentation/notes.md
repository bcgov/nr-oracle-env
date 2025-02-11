# Demos

## Oracle struct

* start the oracle struct database
    `docker compose up oracle`

  or first time, loading the datapump file... takes 20-30 minutes
    `docker compose up oracle-dp-import`

* Demo the struct in dbeaver
* Demo permissions ammended

## Dependency Script

### See options
`uv run main.py show-deps --help`

### Get deps for

`uv run main.py show-deps --seed-table SEEDLOT --schema THE`

### Get deps in json format

`uv run main.py show-deps --seed-table SEEDLOT --schema THE --out-format json`

## Generate migration

### Options
`uv run main.py create-migrations --help`

### Create migration

```
uv run main.py \
create-migrations \
--seed-table SEED_PLAN_ZONE \
--schema THE \
--migration-name seed_plan_zone \
--migration-folder .data//migrations-demo2
```


--migration-folder /home/kjnether/fsa_proj/nr-fds-pyetl/data_prep/migrations_ora/sql


* debug package

uv run python main.py \
   create-migrations \
   --seed-table CUT_BLOCK_OPEN_ADMIN \
   --schema THE \
   --migration-folder /home/kjnether/fsa_proj/nr-silva/local_ora/migrations/sql \
   --migration-name cut_blk_open_adm


uv run main.py show-deps --seed-table CUT_BLOCK_OPEN_ADMIN --schema THE


THE"."SEEDLOT_PLAN_ZONE
THE"."SEEDLOT_OWNER_QUANTITY

CNS_T_TEST_REP_MC
CNS_T_TSC_TEST_RESULT
CNS_T_RQST_ITM_ACTVTY

uv run python main.py \
   create-migrations \
   --seed-table CNS_T_RQST_ITM_ACTVTY \
   --schema CONSEP \
   --migration-folder /home/kjnether/fsa_proj/nr-fds-pyetl/data_prep/migrations_ora/sql \
   --migration-name consep_t_rqst_i_a



uv run python main.py \
   create-migrations \
   --seed-table OPEN_CATEGORY_CODE \
   --schema THE \
   --migration-folder /home/kjnether/fsa_proj/nr-silva/local_ora/migrations/sql \
   --migration-name open_cat_cd




OPEN_CATEGORY_CODE
AGE_CLASS_CODE
HEIGHT_CLASS_CODE
OPENING_STATUS_CODE
STOCKING_CLASS_CODE
SITE_CLASS_CODE
SITE_INDEX_SOURCE_CODE
STOCKING_STATUS_CODE
TSB_NUMBER_CODE
TREE_SPECIES_CODE
RESULTS_AUDIT_ACTION_CODE
CUT_BLOCK_OPEN_ADMIN
OPENING_ATTACHMENT
OPENING
ORG_UNIT
RESULTS_ELECTRONIC_SUBMISSION
SILV_ADMIN_ZONE
OPENING_AMENDMENT_HISTORY
STOCKING_EVENT_HISTORY
RESULTS_AUDIT_EVENT
ACTIVITY_TREATMENT_UNIT
SILV_RELIEF_APPLICATION
STOCKING_STANDARD_UNIT
STOCKING_MILESTONE
FOREST_CLIENT

uv run python main.py \
   create-migrations \
   --seed-table CUT_BLOCK_OPEN_ADMIN \
   --schema THE \
   --migration-name cut_blk_open_adm
