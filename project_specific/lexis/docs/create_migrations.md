# Generate Migrations

The following scripts were run to generate the migrations.  These migrations
were extracted from the original data pump export file that was generated approx
a year ago.  Tried running against DBSTR01, however the THE user does not have
access to the DBMS_METADATA functions on that database.

The migrations generated are located in the './migrations' folder

### generate migrations for LEXIS_CODES - done

``` bash
uv run python main.py create-migrations \
    --seed-object LEXIS_CODES \
    --schema THE \
    --migration-folder /home/kjnether/fsa_proj/nr-fsa-orastruct/project_specific/lexio/migrations \
    --migration-name lxs_cds
```

### generate migrations for LEXIS_POLICY - done

``` bash
uv run python main.py create-migrations \
    --seed-object LEXIS_POLICY \
    --schema THE \
    --migration-folder /home/kjnether/fsa_proj/nr-fsa-orastruct/project_specific/lexis/migrations \
    --migration-name lxs_plcy
```

### generate migrations for LEXIS_REPORTING - running

``` bash
uv run python main.py create-migrations \
    --seed-object LEXIS_REPORTING \
    --schema THE \
    --migration-folder /home/kjnether/fsa_proj/nr-fsa-orastruct/project_specific/lexis/migrations \
    --migration-name lxs_rpt
```
