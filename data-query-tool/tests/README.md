#  Testing

Most of the tests in require the DBDOCK_STRUCT_01 database.  Tests also use the
.env file in the root folder of this project to retrieve connection parameters.

Tests in here are mostly used to support development.  Test coverage is poor,
should this proof of concept gain popularity, then circle back and increase
test coverage and strategy.

# Verification of output

To verify the output create a new fresh database and run the migrations that
are output by this tool.

* **output folder** - `data-query-tool/data/migrations`

# Generation of new migrations
```bash
cd data-query-tool
# SEEDLOT
python main.py create-migrations --seed-table SEEDLOT --schema THE --migration-folder ./data/migrations --migration-name seedlot

# RESULT INBOX PREVIEW
python main.py create-migrations --seed-table RESULTS_INBOX_PREVIEW --schema THE --migration-folder ./data/migrations --migration-name rslt_ib_prev

# FORHEALTH_RSLT
python main.py create-migrations --seed-table FORHEALTH_RSLT --schema THE --migration-folder ./data/migrations --migration-name forhealth_rslt

```

# Test the migration files

```
docker compose up   oracle-migrations-test
```