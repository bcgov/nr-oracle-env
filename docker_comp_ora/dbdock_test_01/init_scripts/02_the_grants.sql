
ALTER SESSION SET CONTAINER=DBDOCK_TEST_01;

BEGIN
    EXECUTE IMMEDIATE 'CREATE USER "THE" IDENTIFIED BY default';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -01920 THEN -- Ignore "user already exists" error
            RAISE;
        END IF;
END;
/

-- CREATE USER "THE" IDENTIFIED BY "default"  ;
-- GRANT CREATE SESSION TO THE; -- Added this line to fix the login issue


grant select any table to THE ;
grant select on sys.dba_objects to THE;
grant READ on sys.dba_objects to THE;
grant select_catalog_role to THE;
grant execute on dbms_metadata to THE;

GRANT SELECT ANY DICTIONARY TO THE;


