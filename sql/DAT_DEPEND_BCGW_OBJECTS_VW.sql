--------------------------------------------------------
--  File created - Tuesday-January-07-2025   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for View DAT_DEPEND_BCGW_OBJECTS_VW
--------------------------------------------------------

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "APP_DAT"."DAT_DEPEND_BCGW_OBJECTS_VW" ("INPUT_OBJECT", "INPUT_OBJECT_TYPE", "DEPENDENT_USE", "DEPENDENT_OBJECT", "DEPENDENT_OBJECT_TYPE") AS 
  SELECT DISTINCT
             i.INPUT_OBJECT,
             i.INPUT_OBJECT_TYPE,
             CASE
                 WHEN i.INPUT_OBJECT = d.OWNER || '.' || d.NAME
                 THEN
                     'Sourced from'
                 ELSE
                     'Referenced by'
             END
                 AS DEPENDENT_USE,
             CASE
                 WHEN i.INPUT_OBJECT =
                      d.REFERENCED_OWNER || '.' || d.REFERENCED_NAME
                 THEN
                     d.OWNER || '.' || d.NAME
                 ELSE
                     d.REFERENCED_OWNER || '.' || d.REFERENCED_NAME
             END
                 AS DEPENDENT_OBJECT,
             CASE
                 WHEN i.INPUT_OBJECT =
                      d.REFERENCED_OWNER || '.' || d.REFERENCED_NAME
                 THEN
                     d.TYPE
                 ELSE
                     d.REFERENCED_TYPE
             END
                 AS DEPENDENT_OBJECT_TYPE
        FROM DAT_INPUT_BCGW_OBJECTS_VW i
             LEFT JOIN DBA_DEPENDENCIES@APP_UTILITY_IDWPROD1.BCGOV d
                 ON (   (i.INPUT_OBJECT = d.OWNER || '.' || d.NAME)
                     OR (i.INPUT_OBJECT =
                         d.REFERENCED_OWNER || '.' || d.REFERENCED_NAME))
       WHERE d.REFERENCED_OWNER NOT IN ('SYS', 'MDSYS')
    ORDER BY i.INPUT_OBJECT;

   COMMENT ON COLUMN "APP_DAT"."DAT_DEPEND_BCGW_OBJECTS_VW"."INPUT_OBJECT" IS 'The full name of the object in idwprod1.bcgov, formatted as SCHEMA.OBJECT, i.e., WHSE_WATER_MANAGEMENT.GW_WATER_WELLS_SP.';
   COMMENT ON COLUMN "APP_DAT"."DAT_DEPEND_BCGW_OBJECTS_VW"."INPUT_OBJECT_TYPE" IS 'The type of database object, i.e., TABLE, VIEW.';
   COMMENT ON COLUMN "APP_DAT"."DAT_DEPEND_BCGW_OBJECTS_VW"."DEPENDENT_USE" IS 'The type of dependency, i.e., Sourced from, Referenced by.';
   COMMENT ON COLUMN "APP_DAT"."DAT_DEPEND_BCGW_OBJECTS_VW"."DEPENDENT_OBJECT" IS 'The dependent object name in idwprod1.bcgov, i.e., GW_WATER_WELLS_SVW.';
   COMMENT ON COLUMN "APP_DAT"."DAT_DEPEND_BCGW_OBJECTS_VW"."DEPENDENT_OBJECT_TYPE" IS 'The type of dependent database object, i.e., TABLE, VIEW.';
   COMMENT ON TABLE "APP_DAT"."DAT_DEPEND_BCGW_OBJECTS_VW"  IS 'This non-spatial table lists BC Geographic Warehouse object dependencies based on objects in APP_DAT.DAT_INPUT_OBJECTS.'
;
