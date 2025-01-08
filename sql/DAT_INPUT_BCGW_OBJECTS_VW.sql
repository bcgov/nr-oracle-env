--------------------------------------------------------
--  File created - Tuesday-January-07-2025   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for View DAT_INPUT_BCGW_OBJECTS_VW
--------------------------------------------------------

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "APP_DAT"."DAT_INPUT_BCGW_OBJECTS_VW" ("OWNER", "OBJECT_NAME", "INPUT_OBJECT", "INPUT_OBJECT_TYPE", "INPUT_OBJECT_CREATED_DATE") AS 
  SELECT d.OWNER,
             d.OBJECT_NAME,
             d.INPUT_OBJECT,
             b.OBJECT_TYPE AS INPUT_OBJECT_TYPE,
             b.CREATED   AS INPUT_OBJECT_CREATED_DATE
        FROM DAT_INPUT_OBJECTS d
             LEFT JOIN DBA_OBJECTS@APP_UTILITY_IDWPROD1.BCGOV b
                 ON (d.OWNER = b.OWNER AND d.OBJECT_NAME = b.OBJECT_NAME)
       WHERE b.OBJECT_TYPE IN ('TABLE', 'VIEW')
    ORDER BY d.INPUT_OBJECT;

   COMMENT ON COLUMN "APP_DAT"."DAT_INPUT_BCGW_OBJECTS_VW"."OWNER" IS 'The schema owner of the object in idwprod1.bcgov, i.e., WHSE_WATER_MANAGEMENT.';
   COMMENT ON COLUMN "APP_DAT"."DAT_INPUT_BCGW_OBJECTS_VW"."OBJECT_NAME" IS 'The object name in idwprod1.bcgov, i.e., GW_WATER_WELLS_SP.';
   COMMENT ON COLUMN "APP_DAT"."DAT_INPUT_BCGW_OBJECTS_VW"."INPUT_OBJECT" IS 'The full name of the object in idwprod1.bcgov, formatted as SCHEMA.OBJECT, i.e., WHSE_WATER_MANAGEMENT.GW_WATER_WELLS_SP.';
   COMMENT ON COLUMN "APP_DAT"."DAT_INPUT_BCGW_OBJECTS_VW"."INPUT_OBJECT_TYPE" IS 'The type of database object, i.e., TABLE, VIEW.';
   COMMENT ON COLUMN "APP_DAT"."DAT_INPUT_BCGW_OBJECTS_VW"."INPUT_OBJECT_CREATED_DATE" IS 'The date the object was last created in the database, i.e., 2017-09-06 15:02:42.';
   COMMENT ON TABLE "APP_DAT"."DAT_INPUT_BCGW_OBJECTS_VW"  IS 'This non-spatial table contains metadata for input objects used in dependency analysis queries for datasets in the BC Geographic Warehouse.'
;
