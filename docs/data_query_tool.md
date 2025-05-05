# Create Database Migrations From seed Table

This script will recieve an input (seed table), and will query for foreign key
dependencies recursively as well as any dependencies identified through
triggers.  The tool can also use the dependency analysis to generate database
migration files that will create the database structure for the seed table and
any other database objects that relate to the seed table.

The following sections will go over:
1. Run the dependency analysis for a single database object
1. Generate migration files for a database object and all of its dependencies.

# Configure Environment

### 1. Install uv

The repository was developed using uv, which simplifies how to handle python
dependencies.

`pip install uv`

### 2. Start the database

The dependency analysis need to be able to connect to the structural database
in order to identify the dependencies.

if you have already run the datapump import then it should be as easy as:
`docker compose up oracle`

* ["tl;dr" structural database](../README.md#a-structural-oracle-database)
* [detailed instructions on structural database](struct_db.md)

# Run Dependency Analysis

This script will extract from the structural database a dependency analysis that
identifies the dependencies of the 'SEEDLOT' table.

`uv run python main.py show-deps --seed-object SEEDLOT`

The script will output something similar to this:

    ```
    TABLE: THE.SEEDLOT
    --------- dependencies ---------
        TABLE: the.client_location
        --------- dependencies ---------
            TABLE: the.forest_client
            --------- dependencies ---------
                TABLE: the.client_type_company_xref
                --------- dependencies ---------
                    TABLE: the.registry_company_type_code
                    TABLE: the.client_type_code
                TABLE: the.client_id_type_code
             ...
    ```

To break it down, SEEDLOT has a foreign key dependency on CLIENT_LOCATION, and
CLIENT_LOCATION has a foreign key dependency on FOREST_CLIENT... etc.

If you would like the data in a json data structure the script supports that
also...

`uv run python main.py show-deps --seed-object SEEDLOT --out-format json`

To view all the different options for show-deps

`uv run python main.py show-deps --help`

# Create the migrations

If you would like to output migration files that will create the ddl for all
the objects identified in the dependency analysis

```
uv run main.py \
    create-migrations \
    --migration-folder ./data/my_migrations \
    --migration-name seedlot_table_migration \
    --schema THE \
    --seed-table SEEDLOT
```

The following will generate a series of files that are aggregated by object
type, for example:

```
* V1.0.0__seedlot.sql - contains tables / views / sequences
* V1.0.2__seedlot_TYP.sql - oracle types
* V1.0.3__seedlot_PKG.sql - packages
* V1.0.4__seedlot_FP.sql - functions and procedures
* V1.0.5__seedlot_TRG.sql - triggers.
```

If the source table is something like a code table without any foreign keys or
trigger dependencies the output will likely be a single table like
 `V1.0.0__seedlot_table_migration.sql` and is located in the
folder `./data/my_migrations`.

IF there are already migrations files in the ./data/my_migrations folder, the
script will read those scripts to ensure that any subsequent migrations that
get generated do not duplicate already existing objects.

[dependency script docs](./dependency_tool_cli.md)
