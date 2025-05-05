# Structural Database

In order to use this you need to get a copy of the datapump export, which is
NOT part of the repository.

## Bring up the database.

1. Copy the datapump file to `./data/dbp01-31-10-24.dmp`
2. Bring up the database and load the datapump file `docker compose up oracle-dp-import`

The `oracle-dp-import` only needs to be run once.  It is this step that will
load the data from the datapump file to oracle.  This typically takes between
15 minutes and half an hour.  The docker compose file is configured to create
a reuseable volume.  This means that the oracle database can be spun up later
much more quickly as it does not need to re-run the datapump import.

If you want to list existing volumes you can run:
```
docker volume ls
```

The volume `nr-fsa-orastruct_oracle-data-struct` is the one that contains the
struct database / datapump structural database.

After the data is loaded all subsequent starts of the database should use:

`docker compose up oracle`

This will re-use the volume that was created by the datapump load, and thus
should'nt take more than a minute.

## Rename the datapump file

If you want to change the name of the datapump file, you need to make sure that
you change all references to it in the `docker-compose.yml` file.


look for this line in the docker-compose.yml file:

```
    volumes:
        ...
        - ./data/dbp01-31-10-24.dmp:/dpdata/dump_file.dmp # add the dump file to
        the container
        ...
```

and modify the path `./data/dbp01-31-10-24.dmp` so that it refers to
the datapump export file that you would like to have loaded.


## Connecting to the database

### as sysdba

`sqlplus sys/default@localhost:1522/DBDOCK_STRUCT_01 as sysdba`

### as user

`sqlplus the/default@localhost:1522/DBDOCK_STRUCT_01`