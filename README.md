# Background

When Nivana was still making music, the paradigm for multiple application integration
was at the database level.  During this dark era, if one application required access
to data that another application was responsible for, usually a grant was created to 
provide access at the database level.

One of the issues that arises with this approach is granting access to developers to 
database objects that are used by a single application becomes difficult.  As time 
progresses even knowing what objects are used by different applications can become
muddy.

Debugging applications typically requires wholesale access to the database, however 
getting this level of access due to sentivity of data, and security concerns becomes
difficult and time consuming.

The objective of this repo is to create local oracle databases to support development
and load the structure from oracle datapump files.

# Getting the database up and running

## Current Pre-requisites

1. you have a datapump export of an oracle database


## Bring up the database.

look for this line in the docker-compose.yml file:

``` 
    volumes:
        ...
        - ./data/dbp01-31-10-24.dmp:/dpdata/dump_file.dmp # add the dump file to the container
        ...
```

and modify if required so that the path `./data/dbp01-31-10-24.dmp` refers to the datapump 
export file that you would like to have loaded.

finally bring up the database and start the datapump import:

`docker compose up oracle-dp-import`

## Connecting to the database

### as sysdba

`sqlplus system/default@localhost:1522/DBDOCK_STRUCT_01`

### as user

`sqlplus the/default@localhost:1522/DBDOCK_STRUCT_01`

