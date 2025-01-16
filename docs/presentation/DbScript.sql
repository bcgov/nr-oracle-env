--query dba tables
SELECT * FROM DBA_DEPENDENCIES
WHERE name = 'SPR_SEEDLOT_AR_IUD_TRG';

--call get_ddl function
SELECT 
dbms_metadata.get_ddl('TABLE', 'SEEDLOT_AUDIT', 'THE') 
FROM dual

