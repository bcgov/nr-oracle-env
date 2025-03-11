
  CREATE OR REPLACE EDITIONABLE TYPE "THE"."SIL_ERROR_PARAMS" AS
VARRAY(50) OF VARCHAR2(500)
/


  CREATE OR REPLACE EDITIONABLE TYPE "THE"."SIL_ERROR_MESSAGE" AS OBJECT
(
  DATABASE_FIELD                 VARCHAR2(50),
  MESSAGE                        VARCHAR2(250),
  PARAMS                         SIL_ERROR_PARAMS,
  WARNING_FLAG                   VARCHAR2(1)
)
/


  CREATE OR REPLACE EDITIONABLE TYPE "THE"."SIL_ERROR_MESSAGES" AS
VARRAY(100) OF SIL_ERROR_MESSAGE
/


  CREATE OR REPLACE EDITIONABLE TYPE "THE"."MSD_SITE_INFO_OBJECT" FORCE AS OBJECT (
    SCALE_SITE_ID                            VARCHAR2(4),
    SCALE_SITE_NAME                          VARCHAR2(40),
    SCALE_SITE_LOCATION                      VARCHAR(200))
/


  CREATE OR REPLACE EDITIONABLE TYPE "THE"."MSD_SITE_INFO_TABLE" AS TABLE OF MSD_SITE_INFO_OBJECT
/


  CREATE OR REPLACE EDITIONABLE TYPE "THE"."MSD_FILE_TYPE_OBJECT" FORCE AS OBJECT (
    FILE_TYPE_CODE                            VARCHAR2(3),
    FILE_TYPE_DESCRIPTION                     VARCHAR2(120))
/


  CREATE OR REPLACE EDITIONABLE TYPE "THE"."MSD_FILE_TYPE_TABLE" AS TABLE OF MSD_FILE_TYPE_OBJECT
/


  CREATE OR REPLACE EDITIONABLE TYPE "THE"."MSD_TYPE_SITE_OBJECT" FORCE AS OBJECT (
    HAULING_MARK_AUTHORIZATION_ID             NUMBER(15),
    ESTIMATED_VOLUME                          NUMBER(6),
    ESTIMATED_TRUCK_LOAD_NO                   NUMBER(6),
    EFFECTIVE_DATE                            DATE,
    EXPIRY_DATE                               DATE,
    TIMBER_MARK                               VARCHAR2(6),
    SCALE_SITE_ID_NUMBER                      VARCHAR2(4),
    SITE_NAME                                 VARCHAR2(40))
/


  CREATE OR REPLACE EDITIONABLE TYPE "THE"."MSD_TYPE_SITE_TABLE" AS TABLE OF MSD_TYPE_SITE_OBJECT
/


  CREATE OR REPLACE EDITIONABLE TYPE "THE"."MSD_TYPE_MARK_OBJECT" FORCE AS OBJECT (
    HAULING_MARK_AUTHORIZATION_ID             NUMBER(15),
    SCALE_SITE_ID_NUMBER                      VARCHAR2(4),
    TIMBER_MARK                               VARCHAR2(6),
    EXPIRY_DATE                               DATE,
    FOREST_DISTRICT                           NUMBER(10),
    ORG_UNIT_CODE                             VARCHAR2(6),
    ORG_UNIT_NAME                             VARCHAR2(100),
    FILE_TYPE_CODE                            VARCHAR2(3),
    FILE_TYPE_DESCRIPTION                     VARCHAR2(120),
    FOREST_FILE_ID                            VARCHAR2(10),
    CLIENT_NUMBER                             VARCHAR2(8),
    CLIENT_LOCATION_CODE                      VARCHAR2(2),
    CLIENT_NAME                               VARCHAR2(100))
/


  CREATE OR REPLACE EDITIONABLE TYPE "THE"."MSD_TYPE_MARK_TABLE" AS TABLE OF MSD_TYPE_MARK_OBJECT
/


  CREATE OR REPLACE EDITIONABLE TYPE "THE"."MSD_TYPE_MARK_INFO_OBJECT" FORCE AS OBJECT (
    TIMBER_MARK                                    VARCHAR2(6),
    MARK_STATUS_ST                                 VARCHAR2(3),
    MARK_STATUS_DESCRIPTION                        VARCHAR2(120),
    EXPIRY_DATE                                    DATE,
    CRUISE_OR_SCALE                                VARCHAR2(10),
    FOREST_DISTRICT                                NUMBER(10),
    ORG_UNIT_CODE                                  VARCHAR2(6),
    ORG_UNIT_NAME                                  VARCHAR2(100),
    FILE_TYPE_CODE                                 VARCHAR2(3),
    FILE_TYPE_DESCRIPTION                          VARCHAR2(120),
    FOREST_FILE_ID                                 VARCHAR2(10),
    CLIENT_NUMBER                                  VARCHAR2(8),
    CLIENT_LOCN_CODE                               VARCHAR2(2),
    CLIENT_NAME                                    VARCHAR2(60),
    LEGAL_FIRST_NAME                               VARCHAR2(30))
/


  CREATE OR REPLACE EDITIONABLE TYPE "THE"."MSD_TYPE_MARK_INFO_TABLE" AS TABLE OF MSD_TYPE_MARK_INFO_OBJECT
/

