# Background

<img src="https://lh3.googleusercontent.com/pw/AP1GczOiFckwC6jkI7XcZEJlDDQpFH0xLF5jajiijcry_9f7sn0O_1HSWMW8SQLLvtjXV_UMiSDiB5MSh_up4jE3DaqUbPs-ucSNq4P3IkT0YMAlvwtsKH_T1YY3xQeG1LTniYcc4L5f16DlkFeQTuP9Hq_Gcw=w1306-h589-s-no-gm?authuser=0" width="700px">

When Nirvana was still making music, the paradigm for multiple application integration
was at the database level.  During this dark era, if one application required access
to another applications data, it was usually accomplished by a database grant.

Challenged with this approach to application data integrations include:
* uncertainty over which application owns what data
* what is the minimum amount of data required to keep an application running
* Debugging applications typically requires wholesale access to the database, however
getting this level of access due to sentivity of data, and security concerns becomes
difficult and time consuming.

The objective of this repo is to facilitate the creation of local oracle databases to
support development activities.

# Components at play

This repo includes the following components:
1. **Structural Oracle Database** An oracle database that is populated from a datapump file that contains all
    the database stucture
1. **Test Oracle Database** An oracle database that can be used to verify the
    migrations, and data loads
1. **Data Dependency Tool** Used to identify database dependencies, and generate migration
    files from the dependencies
1. **Data Population Tool** Having identified the subset of tables from the struct
    database, this tool is used to extract the data from on prem databases, and
    then load that data to the Test Oracle Database.

Subsequent Sections will dive into each of these components in more detail.

# A) Structural Oracle Database

In summary this database will not contain any data.  Instead its a structural
database that contains all the database objects that exist in the prod THE
database.

### TL;DR

1. get the datapump file, and copy it to `./data/dbp01-31-10-24.dmp`
2. first time load of database `docker compose up oracle-dp-import`  (this only needs to take place once)
3. starting database after load has been completed ``docker compose up oracle`

[more info/details...](docs/struct_db.md)

# B) Dependency Analysis / Migration Generation

This is a tool that given a seed object, will follow the foreign key dependencies
and generate a dependency tree for any given object.  The tool can then also
be used to generate migration files for all the objects required to maintain
referential integrity given the seed/start table that specified.

### TL;DR

The following will:
* navigate to the data-query-tool
* load environment variables from the env_sample file
* identify the dependencies of the `SEEDLOT` table
* generate migration files in the `my_migrations` folder which will all have
    the prefix of `seedlot_migs`
     a migration file for all the dependent tables of the table `SEEDLOT`.
If you don't have uv installed... `pip install uv`

```
cd data-query-tool; \
set -a; source env_sample; set +a; \
uv run python main.py create-migrations \
--schema THE \
--seed-object SEEDLOT \
--migration-name seedlot_migs
```

[more info on data query tool...](docs/data_query_tool.md)

# C) Load the migrations to a local TEST database

### TL;DR

Migrations contain the database structure (table/view/packages... definitions).
They are stored in files with sequential numbers and are prefixed with a `V`.
Flyway will run the migrations in the order specified by the names of the files.

Assuming you generated the migration files in the `data/migrations` folder, you
should be able to spin up the test database and run the migrations with the
following:

`docker compose up oracle-migrations-test`

# D) Data Extraction

The data extraction process queries the test database created in the previous
step, to identify which objects need to be extracted from an on prem database.

The idea is this step only needs to be performed once.  It will dump data files
to an object store bucket where they can be retrieved later.  The purpose is
to create "just enough oracle" structure to test things that we may want to do
with or against the on prem oracle databases without any risk of "breaking"
anything.

Extracting the data requries some configuration and therefor this is no "tl;dr"
section for this step.

[Detailed docs on data extraction process](docs/data_extraction.md)

# E) Data Load

This step will load the data, that was extracted in the previous step and
cached in object storage, to the test oracle database (created in step C).  This
is essentially the final step allowing teams to create a "just enough oracle"
database.

### TL;DR

* populate the object store environment variables
* load the data

`uv run python db_env_utils/main_ingest.py ORA PROD`

[Details on the data load script/process](docs/data_load.md)

Also you can view all the options for the main_ingest.py script by running:

`uv run python db_env_utils/main_ingest.py --help`















