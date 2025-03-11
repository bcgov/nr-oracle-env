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

`uv run main.py show-deps --seed-object SEEDLOT --schema THE`

### Get deps in json format

`uv run main.py show-deps --seed-object SEEDLOT --schema THE --out-format json`

## Generate migration

### Options
`uv run main.py create-migrations --help`

### Create migration

```
uv run main.py \
create-migrations \
--seed-object SEED_PLAN_ZONE \
--schema THE \
--migration-name seed_plan_zone \
--migration-folder .data//migrations-demo2
```






