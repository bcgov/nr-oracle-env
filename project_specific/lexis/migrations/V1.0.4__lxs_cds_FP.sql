
  CREATE OR REPLACE EDITIONABLE FUNCTION "THE"."CLIENT_GET_CLIENT_NAME" 
/******************************************************************************
Purpose:   Retrieve Client Name, return null if none found

REVISIONS:
Ver          Date        Author           Description
---------   ----------   ---------------  ------------------------------------
1.0        July 25/2008  T. Blanchard    Original.- created to replace
                                         sil_get_client_name which has a defect

******************************************************************************/
(
  p_client_number                  IN       VARCHAR2)
  RETURN VARCHAR2
IS
  v_client_name                         VARCHAR2(150);
  v_first_name                          VARCHAR2(60);
  v_middle_name                         VARCHAR2(60);
BEGIN
  SELECT client_name
       , legal_first_name
       , legal_middle_name
    INTO v_client_name
       , v_first_name
       , v_middle_name
    FROM v_client_public
   WHERE client_number = p_client_number;

  IF    (TRIM(v_first_name) IS NOT NULL)
     OR (TRIM(v_middle_name) IS NOT NULL) THEN
    v_client_name := v_client_name || ', ' || v_first_name || ' ' || v_middle_name;
  END IF;

  RETURN(v_client_name);
EXCEPTION
  WHEN NO_DATA_FOUND THEN
    RETURN(NULL);                                                                  -- no name found
END client_get_client_name;
/

  CREATE OR REPLACE EDITIONABLE FUNCTION "THE"."SIL_STD_CLIENT_NAME" 
/******************************************************************************
Purpose:   Returns standard client name given:
             FOREST_CLIENT.CLIENT_NAME
             FOREST_CLIENT.LEGAL_FIRST_NAME
             FOREST_CLIENT.LEGAL_MIDDLE_NAME

REVISIONS:
Date        Author           Description
----------  ---------------  --------------------------------------------------
2006-08-08 R.A.Rob           Taken from SIL_GET_CLIENT_NAME
******************************************************************************/
(
  p_client_name                    IN       VARCHAR2
, p_legal_first_name               IN       VARCHAR2
, p_legal_middle_name              IN       VARCHAR2)
  RETURN VARCHAR2
IS
  v_client_name                      VARCHAR2(200);
BEGIN
  IF (TRIM(p_legal_first_name) IS NOT NULL)
     OR(TRIM(p_legal_middle_name) IS NOT NULL) THEN
    v_client_name := p_client_name || ', ' || p_legal_first_name || ' ' || p_legal_middle_name;
  ELSE
    v_client_name := p_client_name;
  END IF;

  RETURN(v_client_name);
EXCEPTION
  WHEN NO_DATA_FOUND THEN
    RETURN(NULL);   -- no name found
END sil_std_client_name;
/

  CREATE OR REPLACE EDITIONABLE FUNCTION "THE"."SIL_CONVERT_TO_CHAR" ( p_value IN DATE, p_format IN VARCHAR2) RETURN VARCHAR2 IS

dateStr VARCHAR2(25);


BEGIN

    dateStr := TO_CHAR(p_value, p_format);
    RETURN (dateStr);


EXCEPTION
    WHEN OTHERS THEN
      RAISE;
END;
/

  CREATE OR REPLACE EDITIONABLE FUNCTION "THE"."FTA_VALID_MINOR_TSL_FILE_TYPE" 
( p_file_type_code IN VARCHAR2)
/******************************************************************************
Purpose:   Check if code is a valid minor TSL file type
REVISIONS:
Ver        Date        Author           Description
---------  ----------  ---------------  ------------------------------------
1.0        03/01/2003 M.Dellaviola     Original.
******************************************************************************/
RETURN VARCHAR2 IS
l_exists VARCHAR2(1);
BEGIN
      SELECT DECODE(COUNT(*),0,'N','Y')
      INTO l_exists
      FROM MINOR_TSL_FILE_TYPE_CODE
      WHERE MINOR_TSL_FILE_TYPE_CODE = p_file_type_code;

    RETURN (l_exists);
EXCEPTION
   WHEN OTHERS THEN
        RETURN('N');
END FTA_VALID_MINOR_TSL_FILE_TYPE;
/

  CREATE OR REPLACE EDITIONABLE PROCEDURE "THE"."FTA_GET_FSJ_HVA_TRIGGER_INFO" (
  p_hva_skey                                harvesting_authority.hva_skey%TYPE
, p_earliest_expiry_hva_skey       IN OUT   harvesting_authority.hva_skey%TYPE
, p_forest_district                IN OUT   timber_mark.forest_district%TYPE
, p_cascade_split_code             IN OUT   timber_mark.cascade_split_code%TYPE
, p_geographic_distrct             IN OUT   timber_mark.geographic_distrct%TYPE
, p_mark_status_st                 IN OUT   timber_mark.mark_status_st%TYPE
, p_tenure_term                    IN OUT   timber_mark.tenure_term%TYPE
, p_mark_status_date               IN OUT   timber_mark.mark_status_date%TYPE
, p_mark_issue_date                IN OUT   timber_mark.mark_issue_date%TYPE
, p_mark_expiry_date               IN OUT   timber_mark.mark_expiry_date%TYPE
, p_district_admn_zone             IN OUT   timber_mark.district_admn_zone%TYPE
, p_mark_extend_date               IN OUT   timber_mark.mark_extend_date%TYPE
, p_mark_extend_count              IN OUT   timber_mark.mark_extend_count%TYPE
, p_mark_extend_rsn_cd             IN OUT   timber_mark.mark_extend_rsn_cd%TYPE
, p_quota_type_code                IN OUT   timber_mark.quota_type_code%TYPE
, p_lands_region                   IN OUT   timber_mark.lands_region%TYPE
, p_salvage_type_code              IN OUT   timber_mark.salvage_type_code%TYPE
, p_catastrophic_ind               IN OUT   timber_mark.catastrophic_ind%TYPE
, p_crown_granted_ind              IN OUT   timber_mark.crown_granted_ind%TYPE
, p_cruise_based_ind               IN OUT   timber_mark.cruise_based_ind%TYPE
, p_deciduous_ind                  IN OUT   timber_mark.deciduous_ind%TYPE
, p_bcaa_folio_number              IN OUT   timber_mark.bcaa_folio_number%TYPE
, p_revision_count                 IN OUT   timber_mark.revision_count%TYPE)
IS
/******************************************************************************
Purpose:   Gets harvesting authority information for trigger that propogates
           FSJ harvesting info to timber_mark.
           A given FSJ harvesting authority can have a mark assigned to
           another FSJ harvesting authority (via the blocks).
           This can result in a given timber mark being assigned to more than
           one FSJ hva. Business indicates that the hva information for the
           single FSJ hva having the earliest expiry date should be that
           propogated to timber mark.
           The fta_sync_hva_tm trigger cannot query harvesting_authority
           to determine the one with earliest expiry date (due to mutating
           table exception). As a result, this AUTONOMOUS_TRANSACTION procedure
           will retrieve the correct hva information called by the trigger.

           This procedures should be dropped when TIMBER_MARK is no longer
           required. I.e. When fta_sync_hva_tm is dropped.

REVISIONS:
Ver        Date        Author           Description
---------  ----------  ---------------  ------------------------------------
1.0        06/04/2007  R.Nanton         Original.
******************************************************************************/
  PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
  SELECT DISTINCT hva_skey
        , forest_district
        , cascade_split_code
        , geographic_district
        , harvest_auth_status_code
        , tenure_term
        , status_date
        , issue_date
        , expiry_date
        , district_admn_zone
        , extend_date
        , NVL(extend_count, 0)
        , harvest_auth_extend_reas_code
        , quota_type_code
        , crown_lands_region_code
        , salvage_type_code
        , catastrophic_ind
        , crown_granted_ind
        , cruise_based_ind
        , deciduous_ind
        , bcaa_folio_number
        , hva.revision_count + 1
    INTO p_earliest_expiry_hva_skey
       , p_forest_district
       , p_cascade_split_code
       , p_geographic_distrct
       , p_mark_status_st
       , p_tenure_term
       , p_mark_status_date
       , p_mark_issue_date
       , p_mark_expiry_date
       , p_district_admn_zone
       , p_mark_extend_date
       , p_mark_extend_count
       , p_mark_extend_rsn_cd
       , p_quota_type_code
       , p_lands_region
       , p_salvage_type_code
       , p_catastrophic_ind
       , p_crown_granted_ind
       , p_cruise_based_ind
       , p_deciduous_ind
       , p_bcaa_folio_number
       , p_revision_count
    FROM harvesting_authority hva
   WHERE hva.hva_skey =
           (SELECT hva_skey
              FROM (SELECT hva.hva_skey
                         , hva.harvesting_authority_id
                         , hva.expiry_date
                         , hhx.timber_mark
                      FROM harvesting_authority hva
                         , harvesting_hauling_xref hhx
                     WHERE hva.hva_skey = hhx.hva_skey
                       AND hhx.timber_mark IN(
                            SELECT DISTINCT timber_mark
                              FROM harvesting_authority hva
                                 , harvesting_hauling_xref hhx
                             WHERE hva.hva_skey = hhx.hva_skey
                               AND hva.hva_skey = p_hva_skey)
                    ORDER BY hva.expiry_date)
             WHERE ROWNUM = 1);
EXCEPTION
  WHEN NO_DATA_FOUND THEN
    RETURN;
END fta_get_fsj_hva_trigger_info;
/

  CREATE OR REPLACE EDITIONABLE FUNCTION "THE"."SIL_GET_ORG_UNIT_CODE" 
/******************************************************************************
Purpose:   Get associated org unit code for org unit number
REVISIONS:
Ver        Date        Author           Description
---------  ----------  ---------------  ------------------------------------
1.0        Jan 10, 2003 m.dellaviola    original
******************************************************************************/
(p_org_unit_no IN VARCHAR2)
RETURN VARCHAR2 IS
ReturnVal    ORG_UNIT.org_unit_code%TYPE;
BEGIN

SELECT ORG_UNIT_CODE
INTO ReturnVal
FROM   ORG_UNIT
WHERE  org_unit_no = p_org_unit_no;

RETURN(ReturnVal);
EXCEPTION
    WHEN OTHERS THEN
         RETURN(NULL);
END;
/

  CREATE OR REPLACE EDITIONABLE FUNCTION "THE"."SIL_GET_ORG_DESC" 
/******************************************************************************
Purpose:   Retrieve Org Unit Description
           Can handle number or code
REVISIONS:
Ver        Date        Author           Description
---------  ----------  ---------------  ------------------------------------
1.0        02/01/2001  M.Dellaviola     Original.
******************************************************************************/
(p_org_unit IN VARCHAR2)

RETURN VARCHAR2 IS
     OrgName    ORG_UNIT.ORG_UNIT_NAME%TYPE;

BEGIN
     SELECT  org_unit_name
     INTO OrgName
     FROM    ORG_UNIT
     WHERE  org_unit_no = p_org_unit;
     RETURN(OrgName);
EXCEPTION

  WHEN NO_DATA_FOUND THEN
     RETURN ('');
  WHEN OTHERS THEN
     SELECT  org_unit_name
     INTO OrgName
     FROM    ORG_UNIT
     WHERE  org_unit_code = p_org_unit;
     RETURN(OrgName);
END Sil_Get_Org_Desc;
/
