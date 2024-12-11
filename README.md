# Background

<img src="https://lh3.googleusercontent.com/pw/AP1GczOiFckwC6jkI7XcZEJlDDQpFH0xLF5jajiijcry_9f7sn0O_1HSWMW8SQLLvtjXV_UMiSDiB5MSh_up4jE3DaqUbPs-ucSNq4P3IkT0YMAlvwtsKH_T1YY3xQeG1LTniYcc4L5f16DlkFeQTuP9Hq_Gcw=w1306-h589-s-no-gm?authuser=0" width="700px">

When Nivana was still making music, the paradigm for multiple application integration
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

