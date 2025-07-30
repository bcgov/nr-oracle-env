# Overview

Doc is broken up into two sections... Likely you are only interested in the first
one that identifies how to go about generating a database for `Lexis`.

The second part of the docs describes how the data required to generate a `Lexis`
database was generated.

# Create a Lexis Database

## Required Env vars

The following env vars are required to be able to connect to object store to
pull the data that will be loaded in the next step.

* OBJECT_STORE_USER_PROD=<user id>
* OBJECT_STORE_SECRET_PROD=<secret>
* OBJECT_STORE_BUCKET_PROD=<bucket>
* OBJECT_STORE_HOST_PROD=nrs.objectstore.gov.bc.ca
* OBJECT_STORE_DATA_DIRECTORY=lexis/ora-db

The easiest way to do this is to take the `envfileSample` file and rename it to
`.env`, then open with your favorite editor and populate the missing values.

## Creating the database for the first time

`docker compose up oracle-data-load`

This will create the database, run the migrations, and then pull the data from
object store and load it to a local oracle database.  Once this has been run once
to completion you should wind up with the following docker volumes:

* lexis_lexus-data - This is where the object store duck db files are cached
* lexis_oracle-lexis - This is where the database storage is.  Don't delete this
    one unless you want to go through the 3-4 hours that it takes to re-load the
    data.

In order to bring the database back up with all the objects and data after a
reboot or some other event that may have shut it down...

`docker compose up oracle-lexis`

# Generating the dependent data files to create a Lexis database

This doc goes over the steps taken to generate a just enough oracle database for
the lexus app.

# Identify objects required

Ran the data query tool to identify tables dependent on:
* LEXUS_CODES
* LEXUS_POLICY
* LEXUS_REPORTING

Then compared this list with the list that Ron generated as a verification that
using the 'data-query' tool would capture all the objects required.

# Generate Migrations

Migrations were generated using the oracle data query tool, which builds a
dependency tree, then works from the outer edges of that tree inwards generating
migration files for each object.

[Docs that go over how this was done](./docs/create_migrations.md)


# Pull data

Data was extracted from the prod database for all the tables that are generated
by the migrations.  The data extraction reads the data classifications and masks
any data that is identified non public.  The data is cached to object storage
in duckdb format.

***Note**: Any table/column combinations that are deemed non public that are primary keys or foreign keys are treated as if they are public.*

[Docs that go over how extraction was completed](./docs/get_data.md)

# Test Load

Having extracted the data, the next step is to verify that the data can be loaded
and that the data shape allows the constraints to be enabled.

[Details on performing the data ingestion](./docs/testing_data_load.md)






