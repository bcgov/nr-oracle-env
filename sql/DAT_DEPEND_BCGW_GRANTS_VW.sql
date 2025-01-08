--------------------------------------------------------
--  File created - Tuesday-January-07-2025   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for View DAT_DEPEND_BCGW_GRANTS_VW
--------------------------------------------------------

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "APP_DAT"."DAT_DEPEND_BCGW_GRANTS_VW" ("INPUT_OBJECT", "GRANTEE", "PRIVILEGE") AS 
  SELECT i.INPUT_OBJECT, p.GRANTEE, p.PRIVILEGE
        FROM APP_DAT.DAT_INPUT_BCGW_OBJECTS_VW i
             LEFT JOIN DBA_TAB_PRIVS@APP_UTILITY_IDWPROD1.BCGOV p
                 ON     p.OWNER || '.' || p.TABLE_NAME = i.INPUT_OBJECT
                    AND p.GRANTEE != 'SDE'
    ORDER BY i.INPUT_OBJECT;

   COMMENT ON COLUMN "APP_DAT"."DAT_DEPEND_BCGW_GRANTS_VW"."INPUT_OBJECT" IS 'The full name of the object in idwprod1.bcgov, formatted as SCHEMA.OBJECT, i.e., WHSE_WATER_MANAGEMENT.GW_WATER_WELLS_SP.';
   COMMENT ON COLUMN "APP_DAT"."DAT_DEPEND_BCGW_GRANTS_VW"."GRANTEE" IS 'The database role that the object is granted to.';
   COMMENT ON COLUMN "APP_DAT"."DAT_DEPEND_BCGW_GRANTS_VW"."PRIVILEGE" IS 'The type of access the granted role has for the object.';
   COMMENT ON TABLE "APP_DAT"."DAT_DEPEND_BCGW_GRANTS_VW"  IS 'This non-spatial table lists BC Geographic Warehouse object dependencies based on objects in APP_DAT.DAT_INPUT_OBJECTS.'
;
