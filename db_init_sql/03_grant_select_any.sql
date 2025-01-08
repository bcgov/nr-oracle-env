ALTER SESSION SET CONTAINER=DBDOCK_STRUCT_01;
CREATE USER "THE" IDENTIFIED BY "default"  ;


grant select any table to THE ;
grant select on sys.dba_objects to THE;
grant READ on sys.dba_objects to THE;
grant select_catalog_role to THE;
grant execute on dbms_metadata to THE;

-- permissions to change default directory used by datapump

grant CREATE ANY DIRECTORY to THE;

GRANT SELECT ANY DICTIONARY TO THE;