
  CREATE OR REPLACE EDITIONABLE PACKAGE "THE"."RESULTS_OPENING" 
IS
--
--    Package:    RESULTS_OPENING
--    Purpose:    Performs operations on a RESULTS Opening
--
--    Revision History
--    Person               Date       Comments
--    -----------------    ---------  --------------------------------
--   R.A.Robb             2004-11-18 Created
--    R.A.Robb             2005-01-24 PT#25599 - Add ability to add cut block
--    R.A.Robb             2005-02-28 Added user_can_update_opening()
--                                          opening_can_be_updated()
--                                          opening_can_be_amalgamated()
--                                          delete_opening()
--                                          opening_tenure_mandatory()
--                                          get_next_amendment_number()
--    R.A.Robb             2005-09-01 modified parse_opening_mapsheet() to remove
--                                    leading zero from mapsheet grid
--   R.A.Robb             2005-09-19 Added overloaded update_audit_info to update
--                                    based on provided Status
--
--    R.A.Robb             2005-12-07 PT#28437 - modify parms in call to su validator
--                                   - add new functions to determine sub attributes
--    E.Wong               2006-02-03 Rls 3.0 - Modified package to handle the
--                                    new amalgamation structure
--    E.Wong               2006-03-21 Rls 3.0 - Modified delete_opening to
--                                    delete geometry
--    R.A.Robb             2007-01-20 PT#34008 - add get_approved_date
--    R.Pardo Figueroa     2007-04-27 PT#34409 - Calculate offset dates for milestones:
--                                    The procedure delete_opening() was moved to the
--                                    results_opening_amalgamation package, which is the
--                                    only place where is called from. The logic for this
--                                    PT was built in the new package, removing the
--                                    separate deletes and calling results_atu.remove()
--    R.Pardo Figueroa     2007-11-08 Avoid increasing amendment numbers when SPA actions
--                                    are submitted within the last 24 hours
--    M.Shaw               2008-05-14 addressed pl/sql warnings
--    C.Chen               2008-05-23 Using table forest_file_client,cut_block_client
--                                    instead of for_client_link
--    C.Chen               2008-06-09 Using the data model to lookup viewable opening categories
--                                    and remove the hard coded values
--    D.Zwiers             2011-09-22 Updates for BCTS users; SQL didn't compile and results were
--                                    not checked.
--    C.Devjayanth         2011-07-26 Alert 108929 - Duplicate Opening Numbers  - Leading 0s are
--                                    removed from the Opening Number using TRIM.
--    C.Devjayanth         2011-09-29 Alert 114896: Primary license is set to NO, for all CBOA records
--    C.Devjayanth         2014-03-21 CR13 - Support for 'R' client type
--    C.Devjayanth         2016-05-16 CR14 BCTS User Update of opening: Either Client number match OR bcts Org unit match

  FUNCTION get_approved_date
  (p_opening_id                 IN NUMBER)
  RETURN DATE;

  FUNCTION is_site_plan
  (p_opening_id                 IN NUMBER)
  RETURN BOOLEAN;

  FUNCTION get_opening_gross_area
  (p_opening_id                 IN VARCHAR2)
  RETURN NUMBER;

  FUNCTION is_gross_area_in_tolerance
  (p_opening_gross_area         IN NUMBER
  ,p_planned_gross_block_area   IN NUMBER)
  RETURN BOOLEAN;

  FUNCTION opening_can_be_updated(p_opening_status_code    IN VARCHAR2) RETURN BOOLEAN;
  FUNCTION opening_can_be_amalgamated(p_opening_status_code IN VARCHAR2) RETURN BOOLEAN;
  FUNCTION opening_tenure_mandatory(p_open_category_code IN VARCHAR2) RETURN BOOLEAN;

  FUNCTION user_can_update_opening(p_opening_id               IN NUMBER
                                 , p_user_is_ministry_ind     IN VARCHAR2
                                 , p_user_org_unit_no         IN VARCHAR2
                                 , p_user_client_number       IN VARCHAR2
                                 , p_user_client_locn_code    IN VARCHAR2) RETURN BOOLEAN;

  FUNCTION IS_P87(p_opening_id               IN NUMBER) RETURN NUMBER;

  FUNCTION get_next_amendment_number(p_opening_id       IN VARCHAR2) RETURN NUMBER;

  PROCEDURE update_audit_info(p_opening_id        IN NUMBER
                            , p_update_userid     IN VARCHAR2
                            , p_revision_count    IN NUMBER
                            , p_error_message     IN OUT VARCHAR2);

  PROCEDURE update_audit_info(p_opening_id        IN VARCHAR2
                            , p_update_userid     IN VARCHAR2
                            , p_revision_count    IN VARCHAR2
                            , p_error_message     IN OUT VARCHAR2);

  PROCEDURE update_audit_info(p_opening_id          IN NUMBER
                            , p_update_userid       IN VARCHAR2
                            , p_revision_count      IN NUMBER
                            , p_opening_status_code IN VARCHAR2
                            , p_error_message       IN OUT VARCHAR2);

  FUNCTION get_revision_count(p_opening_id       IN NUMBER) RETURN NUMBER;

  FUNCTION GET_NAR_AREA(P_OPENING_ID             IN VARCHAR2) RETURN NUMBER;

  FUNCTION get_opening_id_from_mapsheet(p_mapsheet_grid     IN VARCHAR2
                                      , p_mapsheet_letter   IN VARCHAR2
                                      , p_mapsheet_square   IN VARCHAR2
                                      , p_mapsheet_quad     IN VARCHAR2
                                      , p_mapsheet_sub_quad IN VARCHAR2
                                      , p_opening_number    IN VARCHAR2) RETURN NUMBER;

  PROCEDURE parse_opening_mapsheet(p_open1             IN VARCHAR2
                                 , p_open2             IN VARCHAR2
                                 , p_open3             IN VARCHAR2
                                 , p_open4             IN VARCHAR2
                                 , p_mapsheet_grid     OUT VARCHAR2
                                 , p_mapsheet_letter   OUT VARCHAR2
                                 , p_mapsheet_square   OUT VARCHAR2
                                 , p_mapsheet_quad     OUT VARCHAR2
                                 , p_mapsheet_sub_quad OUT VARCHAR2
                                 , p_opening_number    OUT VARCHAR2);

  PROCEDURE create_amendment_history(p_opening_id                 IN  NUMBER
                                   , p_update_userid              IN  VARCHAR2
                                   , p_audit_action_code          IN  VARCHAR2
                                   , p_opening_amendment_number   OUT NUMBER);

  PROCEDURE create_amendment(p_opening_id                 IN  NUMBER
                           , p_update_userid              IN  VARCHAR2
                           , p_opening_amendment_number   OUT NUMBER
                           , p_esf_submission             IN  BOOLEAN DEFAULT FALSE);
END results_opening;
/
CREATE OR REPLACE EDITIONABLE PACKAGE BODY "THE"."RESULTS_OPENING" AS

--
--     Procedure:  get_approved_date
--
--     Purpose:    Given:
--                   opening_id
--                 Returns:
--                   opening.approve_date
--
--
  FUNCTION get_approved_date
  (p_opening_id                 IN NUMBER)
  RETURN DATE
  IS

    CURSOR c_open
    IS
      SELECT approve_date
        FROM opening
       WHERE opening_id = p_opening_id;
    r_open    c_open%ROWTYPE;

  BEGIN

    OPEN c_open;
    FETCH c_open INTO r_open;
    CLOSE c_open;

    RETURN (r_open.approve_date);

  END get_approved_date;

--
--     Procedure:  is_site_plan
--
--     Purpose:    Given:
--                   opening_id
--                 Returns:
--                   TRUE if Opening is a Site Plan
--
--
  FUNCTION is_site_plan
  (p_opening_id                 IN NUMBER)
  RETURN BOOLEAN
  IS

    CURSOR c_su
    IS
      SELECT COUNT(1) su_count
           , COUNT(standards_regime_id) regime_count
        FROM stocking_standard_unit
       WHERE opening_id = p_opening_id;
    r_su    c_su%ROWTYPE;

  BEGIN

    OPEN c_su;
    FETCH c_su INTO r_su;
    CLOSE c_su;

    --RULE: Site Plan - all SU's have regime assigned and it is the same regime.
    --Because historical data does not meet this rule we will say an opening is
    --a Site Plan if it has at least one SU and at least one SU with a regime
    --(new rules will force setting of all SU's to same regime)
    RETURN (r_su.su_count > 0 AND r_su.regime_count > 0);

  END is_site_plan;

--
--     Procedure:  get_opening_gross_area
--
--     Purpose:    Given:
--                   opening_id
--                Returns:
--                   cut_block_open_admin.opening_gross_area for prime licence
--
--
  FUNCTION get_opening_gross_area
  (p_opening_id                 IN VARCHAR2)
  RETURN NUMBER
  IS

    v_opening_gross_area      cut_block_open_admin.opening_gross_area%TYPE;

  BEGIN

    BEGIN
      SELECT opening_gross_area
        INTO v_opening_gross_area
        FROM cut_block_open_admin
       WHERE opening_id = to_number(p_opening_id)
         AND opening_prime_licence_ind = 'Y';
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        NULL;
    END;

    RETURN v_opening_gross_area;

  END get_opening_gross_area;


--
--     Procedure:  is_gross_area_in_tolerance
--
--     Purpose:    Given:
--                   cut_block_open_admin.opening_gross_area
--                   cut_block_open_admin.planned_gross_block_area (aka Exhibit A Area)
--                 Returns:
--                   TRUE if Opening Gross Area is within tolerance
--                   FALSE otherwise
--
--

  FUNCTION is_gross_area_in_tolerance
  (p_opening_gross_area         IN NUMBER
  ,p_planned_gross_block_area   IN NUMBER)
  RETURN BOOLEAN
  IS

    C_FACTOR             CONSTANT NUMBER(3,2) := 1.25;
    C_LOWER_LIMIT        CONSTANT NUMBER(4) := 1000;
    b_within_tolerance   BOOLEAN;

  BEGIN
    b_within_tolerance := TRUE;

    IF p_opening_gross_area > C_LOWER_LIMIT
    AND p_opening_gross_area >= (p_planned_gross_block_area * C_FACTOR) THEN
      b_within_tolerance := FALSE;
    END IF;

    RETURN b_within_tolerance;

  END is_gross_area_in_tolerance;

--
--    Procedure:   opening_can_be_amalgamated
--
--    Purpose:     Returns true if the opening can be amalgamated based on status.
--
  FUNCTION opening_can_be_amalgamated(p_opening_status_code IN VARCHAR2) RETURN BOOLEAN
  IS
  BEGIN

    RETURN (p_opening_status_code = 'APP');

  END opening_can_be_amalgamated;

--
--    Procedure:   opening_can_be_updated
--
--    Purpose:     Returns true if the opening can be updated based on status.
--
  FUNCTION opening_can_be_updated(p_opening_status_code    IN VARCHAR2) RETURN BOOLEAN
  IS
  BEGIN

    RETURN (p_opening_status_code IN ('APP','AMD','SUB','AMG','FG'));

  END opening_can_be_updated;

--
--    Procedure:   opening_tenure_mandatory
--
--    Purpose:     Returns true if tenure is mandatory for an opening based on
--                 the opening category code.
--
  FUNCTION opening_tenure_mandatory(p_open_category_code IN VARCHAR2) RETURN BOOLEAN
  IS
    v_temp_num    NUMBER(1);

    CURSOR cur_sp
    IS
      SELECT COUNT(1)
        FROM open_category_site_plan_code
       WHERE open_category_site_plan_code = p_open_category_code;
  BEGIN

    OPEN cur_sp;
    FETCH cur_sp INTO v_temp_num;
    CLOSE cur_sp;

    RETURN (v_temp_num <> 0);

  END opening_tenure_mandatory;


  FUNCTION IS_P87(p_opening_id               IN NUMBER) RETURN NUMBER
  IS
    cnt number;
  BEGIN
    SELECT count(*) into cnt
    FROM opening t
    where t.opening_id = p_opening_id and t.open_category_code = 'P87';

    return cnt;
  END IS_P87;

--
--    Procedure:   USER_CAN_UPDATE_OPENING
--
--    Purpose:     Returns true if the user can update the opening.
--                 (in this context "update" connotes update and delete)
--
--                 NOTE: This code does NOT validate the user's authorization
--                       level.
--
--    Assumptions: As elsewhere in RESULTS we assume that the presence of org
--                 unit AND client information signifies that the user is BCTS.
--
--
  FUNCTION user_can_update_opening(p_opening_id               IN NUMBER
                                 , p_user_is_ministry_ind     IN VARCHAR2
                                 , p_user_org_unit_no         IN VARCHAR2
                                 , p_user_client_number       IN VARCHAR2
                                 , p_user_client_locn_code    IN VARCHAR2) RETURN BOOLEAN
  IS
    CURSOR cur_opening
    IS
      SELECT admin_district_no
           , open_category_code
           , opening_status_code
        FROM opening
       WHERE opening_id = p_opening_id;
    rec_opening    cur_opening%ROWTYPE;

    CURSOR cur_file
    IS
      SELECT 'Y' client_is_a_or_o
        FROM cut_block_open_admin cboa
           , forest_file_client   ffc
           , cut_block_client     cbc
       WHERE cboa.opening_id = p_opening_id
         AND ((ffc.forest_file_id = cboa.forest_file_id
         AND ffc.client_number = p_user_client_number
         AND ffc.forest_file_client_type_code = 'A')
         OR (cbc.cb_skey = cboa.cb_skey
         AND cbc.client_number = p_user_client_number
         AND (cbc.cut_block_client_type_code ='O' OR cbc.cut_block_client_type_code ='R')));
    rec_file      cur_file%ROWTYPE;

    --Where user is HQ or user is a Region and opening districts rolls up
    CURSOR cur_org
    IS
      SELECT 'Y' opening_is_within_users_org
        FROM org_unit
       WHERE (rollup_region_no = to_number(p_user_org_unit_no)
              AND org_unit_no = rec_opening.admin_district_no)
          OR (org_unit_no = to_number(p_user_org_unit_no)
              AND org_level_code = 'H');

    -- Check the opening is viewable or not
    CURSOR cur_viewable(v_open_category_code IN open_viewable_category.open_category_code%type
                       ,v_opening_id         IN NUMBER)
    IS
      SELECT count(*) opening_viewable
          FROM opening                 O
             , open_viewable_category  OVC
         WHERE O.open_category_code = OVC.open_category_code
           and O.opening_id = v_opening_id
           AND O.open_category_code = v_open_category_code
           AND ((O.opening_status_code = OVC.opening_status_code
            AND OVC.viewable_ind       = 'Y')
            OR (OVC.opening_status_code is null
            AND OVC.viewable_ind       = 'Y'));

    rec_org     cur_org%ROWTYPE;

    b_ok             BOOLEAN := FALSE;
    b_user_is_bcts   BOOLEAN;
    l_opening_viewable PLS_INTEGER;
    l_bcts_org_unit_no org_unit.org_unit_no%type;

  BEGIN
    --If ministry and client assume BCTS
    b_user_is_bcts := (p_user_is_ministry_ind = 'Y' AND p_user_client_number IS NOT NULL);

    --Get opening data
    OPEN cur_opening;
    FETCH cur_opening INTO rec_opening;
    CLOSE cur_opening;

    --Opening is a viewable - anyone can update
    OPEN  cur_viewable(rec_opening.open_category_code,p_opening_id);
    FETCH cur_viewable INTO l_opening_viewable;
    CLOSE cur_viewable;

    IF l_opening_viewable <> 0 THEN
      b_ok := TRUE;

    ELSIF b_user_is_bcts THEN

      -- CR14 - Checked for both Org No and Client No
      -- CR14 - Case for BCTS User as regular Client
      OPEN cur_file;
      FETCH cur_file INTO rec_file;
      CLOSE cur_file;
      b_ok := (rec_file.client_is_a_or_o = 'Y');

      -- CR14 - Case for BCTS User as Harvest Sales BCTS Org Unit
      IF NOT b_ok AND p_user_org_unit_no IS NOT NULL THEN
          l_bcts_org_unit_no := RESULTS_GET_BCTS_ORG_UNIT_NO(p_opening_id);
          b_ok := (p_user_org_unit_no = l_bcts_org_unit_no);
      END IF;

    --Ministry: can only update openings in their own district
    ELSIF p_user_is_ministry_ind = 'Y' THEN
      b_ok := (p_user_org_unit_no = rec_opening.admin_district_no);
      IF NOT b_ok THEN
        --Check if user is HQ or if openings rolls up to user's org
        OPEN cur_org;
        FETCH cur_org INTO rec_org;
        CLOSE cur_org;
        b_ok := (NVL(rec_org.opening_is_within_users_org,'N') = 'Y');
      END IF;

    --Clients: can only update openings for which they are the licensee (A) or
    --for which they hold the Silviculture Obligation (O) or rlienttype (R)
    ELSIF p_user_client_number IS NOT NULL THEN
      OPEN cur_file;
      FETCH cur_file INTO rec_file;
      CLOSE cur_file;
      b_ok := (rec_file.client_is_a_or_o = 'Y');

    END IF;

    RETURN (b_ok);

  END user_can_update_opening;

--
--    Function:   GET_NEXT_AMENDMENT_NUMBER
--
--    Purpose:    Derive the next opening amendment number
--

  FUNCTION get_next_amendment_number(p_opening_id       IN VARCHAR2) RETURN NUMBER
  IS

    v_opening_amendment_number   opening_amendment_history.opening_amendment_number%TYPE;

    CURSOR max_amend_cur IS
    SELECT MAX(opening_amendment_number)
      FROM opening_amendment_history
     WHERE opening_id = to_number(p_opening_id);

  BEGIN
    OPEN max_amend_cur;
    FETCH max_amend_cur INTO v_opening_amendment_number;
    CLOSE max_amend_cur;

    v_opening_amendment_number := NVL(v_opening_amendment_number,0) + 1;

    RETURN v_opening_amendment_number;

  END get_next_amendment_number;

--
--    Procedure:  GET_REVISION_COUNT
--
--    Purpose:    Given opening_id returns opening.revision_count
--

  FUNCTION get_revision_count(p_opening_id        IN NUMBER) RETURN NUMBER
  IS

    v_revision_count   opening.revision_count%TYPE;
  BEGIN

    BEGIN
      SELECT revision_count
        INTO v_revision_count
        FROM opening
       WHERE opening_id = p_opening_id;

    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        NULL;
    END;

    RETURN v_revision_count;

  END get_revision_count;

--
--    Function:  GET_NAR_AREA
--
--    Purpose:    To retrieve specific data based on criteria sent to proc.
--

  FUNCTION GET_NAR_AREA(P_OPENING_ID  IN VARCHAR2) RETURN NUMBER
  IS

    l_nar_area  NUMBER;

  BEGIN

    SELECT SUM(NET_AREA)
    INTO l_nar_area
    FROM STOCKING_STANDARD_UNIT
    WHERE OPENING_ID = to_number(P_OPENING_ID);

    RETURN NVL(l_nar_area, 0);

  END GET_NAR_AREA;

--
--    Procedure:  UPDATE_AUDIT_INFO
--
--                NOTE: Overloaded to allow all parms as VARCHAR2
--
--                If no rows updated, error message is appended to p_error_message
--
--    Purpose:    Update userid and timestamp info for OPENING
--

  PROCEDURE update_audit_info(p_opening_id        IN NUMBER
                            , p_update_userid     IN VARCHAR2
                            , p_revision_count    IN NUMBER
                            , p_error_message     IN OUT VARCHAR2)
  IS
  BEGIN

    UPDATE opening
       SET update_userid = p_update_userid
         , update_timestamp = SYSDATE
         , revision_count = revision_count + 1
     WHERE opening_id = p_opening_id
       AND revision_count = p_revision_count;

    IF SQL%ROWCOUNT = 0 THEN
      p_error_message := p_error_message || 'results.web.usr.database.record.modified:RESULTS_OPENING,UPDATE_AUDIT_INFO,OPENING;';
    END IF;

  END update_audit_info;


--
--    Procedure:  UPDATE_AUDIT_INFO
--
--                NOTE: Overloaded to check Status on Update and return a
--                      friendly message if update failed due to status.
--
--    Purpose:    Update userid and timestamp info for OPENING
--

  PROCEDURE update_audit_info(p_opening_id          IN NUMBER
                            , p_update_userid       IN VARCHAR2
                            , p_revision_count      IN NUMBER
                            , p_opening_status_code IN VARCHAR2
                            , p_error_message       IN OUT VARCHAR2)

  IS
    CURSOR c_status
    IS
       SELECT opening_status_code
         FROM opening
        WHERE opening_id = p_opening_id;
    v_opening_status_code     opening.opening_status_code%TYPE;

  BEGIN

    UPDATE opening
       SET update_userid = p_update_userid
         , update_timestamp = SYSDATE
         , revision_count = revision_count + 1
     WHERE opening_id = p_opening_id
       AND opening_status_code = p_opening_status_code
       AND revision_count = p_revision_count;

    IF SQL%ROWCOUNT = 0 THEN

      --Has opening status changed?
      OPEN c_status;
      FETCH c_status INTO v_opening_status_code;
      CLOSE c_status;
      IF v_opening_status_code != p_opening_status_code THEN
        p_error_message := p_error_message || 'results.web.usr.database.opening.status.changed;';
      ELSE
        p_error_message := p_error_message || 'results.web.usr.database.record.modified:RESULTS_OPENING,UPDATE_AUDIT_INFO,OPENING;';
      END IF;
    END IF;

  END update_audit_info;

  PROCEDURE update_audit_info(p_opening_id        IN VARCHAR2
                            , p_update_userid     IN VARCHAR2
                            , p_revision_count    IN VARCHAR2
                            , p_error_message     IN OUT VARCHAR2)
  IS
  BEGIN

    update_audit_info(TO_NUMBER(p_opening_id)
                    , p_update_userid
                    , TO_NUMBER(p_revision_count)
                    , p_error_message);

  END update_audit_info;


--
--    Procedure:  get_opening_id_from_mapsheet
--
--    Purpose:    Given opening mapsheet components, returns opening_id if
--                found, or NULL if not found
--

  FUNCTION get_opening_id_from_mapsheet(p_mapsheet_grid     IN VARCHAR2
                                      , p_mapsheet_letter   IN VARCHAR2
                                      , p_mapsheet_square   IN VARCHAR2
                                      , p_mapsheet_quad     IN VARCHAR2
                                      , p_mapsheet_sub_quad IN VARCHAR2
                                      , p_opening_number    IN VARCHAR2)
  RETURN NUMBER IS

    v_opening_id  opening.opening_id%TYPE;

  CURSOR c_open
  IS
    SELECT opening_id
      FROM opening
     WHERE mapsheet_grid  = p_mapsheet_grid
       AND mapsheet_letter = p_mapsheet_letter
       AND mapsheet_square = p_mapsheet_square
       AND mapsheet_quad = p_mapsheet_quad
       AND mapsheet_sub_quad = p_mapsheet_sub_quad
       AND opening_number = p_opening_number;

  BEGIN

    OPEN c_open;
    FETCH c_open INTO v_opening_id;
    CLOSE c_open;

    RETURN v_opening_id;

  END get_opening_id_from_mapsheet;

--
--    Procedure:  parse_opening_mapsheet
--
--    Purpose:    Given opening part 1,2,3,4
--                return individual mapsheet components
--

  PROCEDURE parse_opening_mapsheet(p_open1             IN VARCHAR2
                                 , p_open2             IN VARCHAR2
                                 , p_open3             IN VARCHAR2
                                 , p_open4             IN VARCHAR2
                                 , p_mapsheet_grid     OUT VARCHAR2
                                 , p_mapsheet_letter   OUT VARCHAR2
                                 , p_mapsheet_square   OUT VARCHAR2
                                 , p_mapsheet_quad     OUT VARCHAR2
                                 , p_mapsheet_sub_quad OUT VARCHAR2
                                 , p_opening_number    OUT VARCHAR2)

  IS
    v_quote               VARCHAR2(1) := '''';
    v_replace_quote       VARCHAR2(2) := '''''';
    v_open1               VARCHAR2(30);
    v_open2               VARCHAR2(30);
    v_open3               VARCHAR2(30);
    v_open4               VARCHAR2(30);

  BEGIN
    v_open1 := TRIM(p_open1);
    v_open2 := TRIM(p_open2);
    v_open3 := TRIM(p_open3);
    v_open4 := TRIM(leading '0' from TRIM(p_open4));

    IF v_open1 IS NOT NULL THEN
      v_open1 := REPLACE(LPAD(v_open1,4,' '),v_quote,v_replace_quote);
      p_mapsheet_grid := LPAD(LTRIM(SUBSTR(v_open1, 1, 3),'0'),3,' ');
      p_mapsheet_letter := SUBSTR(v_open1, 4, 1);
    END IF;

    IF v_open2 IS NOT NULL THEN
      v_open2 := REPLACE(LPAD(v_open2,3,'0'),v_quote,v_replace_quote);
      p_mapsheet_square := v_open2;
    END IF;

    IF v_open3 IS NOT NULL THEN
      p_mapsheet_quad := SUBSTR(v_open3, 1, 1);
      p_mapsheet_sub_quad := SUBSTR(v_open3, 3, 1);
    END IF;

    IF v_open4 IS NOT NULL THEN
      v_open4 := REPLACE(LPAD(v_open4,4,' '),v_quote,v_replace_quote);
      p_opening_number := v_open4;

      --default quad/sub quad to 0 if not entered
      p_mapsheet_quad := NVL(p_mapsheet_quad,'0');
      p_mapsheet_sub_quad := NVL(p_mapsheet_sub_quad,'0');
    END IF;

  END parse_opening_mapsheet;

--
--    Procedure:  CREATE_AMENDMENT_HISTORY
--
--    Purpose:    This version ofthe create_amendment simply inserts the
--                amendment history record
--
--

  PROCEDURE create_amendment_history(p_opening_id                 IN  NUMBER
                                   , p_update_userid              IN  VARCHAR2
                                   , p_audit_action_code          IN  VARCHAR2
                                   , p_opening_amendment_number   OUT NUMBER) IS

  v_sysdate_minus_one DATE := sysdate-1;

  BEGIN

    -- Only SPA should get this far, AMD events are created by the procedure create_amendment()
    IF p_audit_action_code = 'SPA' THEN
      BEGIN
        SELECT max(opening_amendment_number)
          INTO p_opening_amendment_number
          FROM opening_amendment_history oah
         WHERE opening_id      = p_opening_id
           AND entry_timestamp between v_sysdate_minus_one and SYSDATE
           AND not exists (SELECT stocking_event_history_id
                             FROM stocking_event_history
                            WHERE opening_amendment_id      = oah.opening_id
                              AND results_audit_action_code <> 'SPA'
                              AND entry_timestamp           > oah.entry_timestamp); -- The last record and the last 24 hours
      EXCEPTION
        WHEN NO_DATA_FOUND then p_opening_amendment_number := NULL;
      END;

      if p_opening_amendment_number IS NULL THEN -- No amendments or 24 timeframe has passed
        --Get next amendment number
        p_opening_amendment_number := get_next_amendment_number(p_opening_id);

        --Create amendment
        INSERT INTO opening_amendment_history(opening_id,
                                              opening_amendment_number,
                                              amendment_userid,
                                              amendment_date,
                                              submitted_by_userid,
                                              app_ent_by_userid,
                                              approve_date,
                                              entry_userid,
                                              entry_timestamp,
                                              update_userid,
                                              update_timestamp,
                                              revision_count)
        VALUES (p_opening_id,
                p_opening_amendment_number,
                p_update_userid,
                SYSDATE,
                NULL,
                NULL,
                NULL,
                p_update_userid,
                SYSDATE,
                p_update_userid,
                SYSDATE,
                1);
      else
        UPDATE opening_amendment_history
           SET update_userid = p_update_userid
             , update_timestamp = SYSDATE
             , revision_count = revision_count + 1
         WHERE opening_id = p_opening_id
           AND opening_amendment_number = p_opening_amendment_number;
      end if;
    end if;

  END create_amendment_history;

--
--    Procedure:  CREATE_AMENDMENT
--
--    Purpose:    For an opening, copies records from STOCKING_STANDARD_UNIT,
--                STOCKING_ECOLOGY, STOCKING_LAYER, and STOCKING_LAYER_SPECIES
--               into the appropriate amendment table (suffixed _amd).
--
--                Removed from the 310 package to make common.
--                Only used for creating major amendments (AMD)
--

  PROCEDURE create_amendment(p_opening_id                 IN  NUMBER
                           , p_update_userid              IN  VARCHAR2
                           , p_opening_amendment_number   OUT NUMBER
                           , p_esf_submission             IN  BOOLEAN DEFAULT FALSE)
  IS

    v_regen_delay_offset_yrs     stocking_standard_unit_amd.regen_delay_offset_yrs%TYPE;
    v_regen_obligation_ind       stocking_standard_unit_amd.regen_obligation_ind%TYPE;
    v_regen_declared_date        stocking_milestone.declared_date%TYPE;
    v_regen_declared_userid      stocking_milestone.declared_userid%TYPE;
    v_no_regen_early_offset_yrs  stocking_standard_unit_amd.no_regen_early_offset_yrs%TYPE;
    v_no_regen_late_offset_yrs   stocking_standard_unit_amd.no_regen_late_offset_yrs%TYPE;
    v_no_regen_declared_date     stocking_milestone.declared_date%TYPE;
    v_no_regen_declared_userid   stocking_milestone.declared_userid%TYPE;
    v_fg_early_offset_yrs        stocking_standard_unit_amd.free_growing_early_offset_yrs%TYPE;
    v_fg_late_offset_yrs         stocking_standard_unit_amd.free_growing_late_offset_yrs%TYPE;
    v_fg_declared_date           stocking_milestone.declared_date%TYPE;
    v_fg_declared_userid         stocking_milestone.declared_userid%TYPE;

    v_opening_amendment_number   opening_amendment_history.opening_amendment_number%TYPE;

    CURSOR cur_stocking_standard_id
    IS
      SELECT stocking_standard_unit_id
        FROM stocking_standard_unit
       WHERE opening_id = p_opening_id;

  BEGIN
    --Get next amendment number
    p_opening_amendment_number := get_next_amendment_number(p_opening_id);

    --Audit the amendment
    Results_Audit.insert_audit_event(p_opening_id => p_opening_id
                                   , p_audit_action_code => 'AMD'
                                   , p_audit_description => 'Opening Amended.'
                                   , p_results_audit_status => 'APP'
                                   , p_opening_amendment_number => p_opening_amendment_number
                                   , p_update_userid => p_update_userid);
	IF (p_esf_submission) THEN
	  -- for esf
      Results_Audit.insert_audit_event(p_opening_id => p_opening_id
                                     , p_audit_action_code => 'SUB'
                                     , p_audit_description => 'Opening is submitted(esf) for review.'
                                     , p_opening_amendment_number => p_opening_amendment_number
                                     , p_update_userid => p_update_userid);

	END IF;

    --Create amendment
    INSERT INTO opening_amendment_history(opening_id,
                                          opening_amendment_number,
                                          amendment_userid,
                                          amendment_date,
                                          submitted_by_userid,
                                          app_ent_by_userid,
                                          approve_date,
                                          entry_userid,
                                          entry_timestamp,
                                          update_userid,
                                          update_timestamp,
                                          revision_count)
    VALUES (p_opening_id,
            p_opening_amendment_number,
            p_update_userid,
            SYSDATE,
            NULL,
            NULL,
            NULL,
            p_update_userid,
            SYSDATE,
            p_update_userid,
            SYSDATE,
            1);

    --Loop through all the su's for the opening
    FOR rec_stocking_standard_id IN cur_stocking_standard_id LOOP
      --Get the derived information needed for the amendment table
      results_milestones.derive_stocking_standard_info(rec_stocking_standard_id.stocking_standard_unit_id
                                                     , v_regen_obligation_ind
                                                     , v_regen_delay_offset_yrs
                                                     , v_regen_declared_date
                                                     , v_regen_declared_userid
                                                     , v_fg_early_offset_yrs
                                                     , v_fg_late_offset_yrs
                                                     , v_fg_declared_date
                                                     , v_fg_declared_userid
                                                     , v_no_regen_early_offset_yrs
                                                     , v_no_regen_late_offset_yrs
                                                     , v_no_regen_declared_date
                                                     , v_no_regen_declared_userid);

      INSERT INTO stocking_standard_unit_amd(stocking_standard_unit_id
                                           , opening_id
                                           , standards_unit_id
                                           , standards_regime_id
                                           , net_area
                                           , max_allow_soil_disturbance_pct
                                           , variance_ind
                                           , regen_delay_offset_yrs
                                           , regen_obligation_ind
                                           , no_regen_early_offset_yrs
                                           , no_regen_late_offset_yrs
                                           , free_growing_early_offset_yrs
                                           , free_growing_late_offset_yrs
                                           , amendment_rationale_comment
                                           , entry_userid
                                           , entry_timestamp
                                           , update_userid
                                           , update_timestamp
                                           , revision_count)
      SELECT stocking_standard_unit_id,
             opening_id,
             standards_unit_id,
             standards_regime_id,
             net_area,
             max_allow_soil_disturbance_pct,
             variance_ind,
             v_regen_delay_offset_yrs,
             v_regen_obligation_ind,
             v_no_regen_early_offset_yrs,
             v_no_regen_late_offset_yrs,
             v_fg_early_offset_yrs,
             v_fg_late_offset_yrs,
             NULL,  -- amendment_rationale_comment
             entry_userid,
             entry_timestamp,
             update_userid,
             update_timestamp,
             revision_count
      FROM stocking_standard_unit
      WHERE stocking_standard_unit_id = rec_stocking_standard_id.stocking_standard_unit_id;

    END LOOP;

    INSERT INTO stocking_ecology_amd(stocking_ecology_id,
                                     opening_id,
                                     stocking_standard_unit_id,
                                     bec_region_code,
                                     bgc_zone_code,
                                     bgc_subzone_code,
                                     bgc_variant,
                                     bgc_phase,
                                     bec_site_series,
                                     bec_site_type,
                                     bec_seral,
                                     entry_userid,
                                     entry_timestamp,
                                     update_userid,
                                     update_timestamp,
                                     revision_count)
    SELECT stocking_ecology_id,
           opening_id,
           stocking_standard_unit_id,
           bec_region_code,
           bgc_zone_code,
           bgc_subzone_code,
           bgc_variant,
           bgc_phase,
           bec_site_series,
           bec_site_type,
           bec_seral,
           entry_userid,
           entry_timestamp,
           update_userid,
           update_timestamp,
           revision_count
      FROM stocking_ecology
     WHERE opening_id = p_opening_id;

    INSERT INTO stocking_layer_amd(stocking_layer_id,
                                   stocking_standard_unit_id,
                                   opening_id,
                                   stocking_layer_code,
                                   target_stocking,
                                   residual_basal_area,
                                   min_horizontal_distance,
                                   min_pref_stocking_standard,
                                   min_post_spacing,
                                   min_stocking_standard,
                                   max_post_spacing,
                                   max_conifer,
                                   hght_relative_to_comp,
                                   tree_size_unit_code,
                                   entry_userid,
                                   entry_timestamp,
                                   update_userid,
                                   update_timestamp,
                                   revision_count)
    SELECT stocking_layer_id,
           stocking_standard_unit_id,
           opening_id,
           stocking_layer_code,
           target_stocking,
           residual_basal_area,
           min_horizontal_distance,
           min_pref_stocking_standard,
           min_post_spacing,
           min_stocking_standard,
           max_post_spacing,
           max_conifer,
           hght_relative_to_comp,
           tree_size_unit_code,
           entry_userid,
           entry_timestamp,
           update_userid,
           update_timestamp,
           revision_count
      FROM stocking_layer
     WHERE opening_id = p_opening_id;

    INSERT INTO stocking_layer_species_amd(stocking_layer_id,
                                           species_order,
                                           silv_tree_species_code,
                                           preferred_ind,
                                           min_height,
                                           entry_userid,
                                           entry_timestamp,
                                           update_userid,
                                           update_timestamp,
                                           revision_count)
    SELECT stocking_layer_id,
           species_order,
           silv_tree_species_code,
           preferred_ind,
           min_height,
           entry_userid,
           entry_timestamp,
           update_userid,
           update_timestamp,
           revision_count
      FROM stocking_layer_species
     WHERE stocking_layer_id IN (SELECT stocking_layer_id
                                   FROM stocking_layer
                                  WHERE opening_id = p_opening_id);


    UPDATE opening
       SET opening_status_code = 'AMD'
         , update_userid = p_update_userid
         , update_timestamp = sysdate
         , revision_count = revision_count + 1
     WHERE opening_id = p_opening_id;

  END create_amendment;

END results_opening;
/

  CREATE OR REPLACE EDITIONABLE PACKAGE "THE"."RESULTS_AUDIT" 
IS
--
--    Package:    RESULTS_AUDIT
--    Purpose:    This package will be used to create Audit Events for
--    the following events:  Deletes, Data changes, After a document has
--    been approved (e.g. openings), Changes of statuses for openings
--    and standard regimes. The actual details of a change, addition,
--    and deletion will be captured by database triggers for each table tracked.
--
--    Revision History
--    Person               Date       Comments
--    -----------------    ---------  --------------------------------
--    wcound (Pangaea)    2003-05-03  Created
--    wcound              2003-09-04  Added additional code to handle the Action code
--                                    of SUB.
--    wcound              2003-06-26  Added the Action code DEL.
--                                    of SUB.
--    wcound              2003-08-14  Changed RJT to REJ
--    R.A.Robb            2004-05-02  CR#46 - only one audit event per day
--    R.A.Robb            2005-03-26  Added audit_regime_change()
--    STaylor             2005-05-30  Added MIN to list of action codes that we want to
--                                    ensure we only have one audit record per DAY
--    BKyle               2005-05-31  Added set_email_sent_ind
--    R.A.Robb            2005-08-01  Fix disabling of auditing in audit_regime_change().
--                                    Regime status not defaulting to passed-in value
--                                    in insert_audit_event().
--    R.A.Robb            2005-11-25  PT#28437 - add COR to onc-event-per-day auditing
--                                    - enable forcing of event creation
--                                    PT#28437 - add submission id, email sent ind to
--                                    audit_regime_change() parms
--    R.A.Robb            2006-03-30  CR#125 - add project auditing
--                        2006-04-23  CR#126 - add spatial auditing
--    R.Pardo Figueroa    2007-04-27  PT#36244 - Include MIL event for Milestones to
--                                    be audited once per day.
--    R.Pardo Figueroa    2007-11-07  Include SPA event, to solve errors during testing of 3.1
--    R.Pardo Figueroa    2008-11-14  PT#39940 - Create procedure to delete empty event
--                                    when required, as when updating milestones with no changes
--    M.Shaw              2008-05-13  addressed pl/sql warnings
--    T.Blanchard         2009-11-23  Fixed broken logic in audit_regime_change. Would never log
--                                    because condition for when to log was wrong.
--    T.Blanchard         2009-12-16  Added new procedure audit_frpa197_regime_change
--    J.Wiebe             2010-03-02  Ptweb 45228 add identifier.
--
  PROCEDURE insert_audit_event(
    p_opening_id                     IN       results_audit_event.opening_id%TYPE DEFAULT NULL
  , p_standards_regime_id            IN       results_audit_event.standards_regime_id%TYPE DEFAULT NULL
  , p_silviculture_project_id        IN       results_audit_event.silviculture_project_id%TYPE DEFAULT NULL
  , p_audit_action_code              IN       results_audit_event.results_audit_action_code%TYPE DEFAULT NULL
  , p_audit_description              IN       results_audit_event.description%TYPE DEFAULT NULL
  , p_email_sent_ind                 IN       results_audit_event.email_sent_ind%TYPE DEFAULT 'N'
  , p_user_id                        IN       results_audit_event.user_id%TYPE DEFAULT NULL
  , p_xml_submission_id              IN       results_audit_event.xml_submission_id%TYPE DEFAULT NULL
  , p_opening_amendment_number       IN       results_audit_event.opening_amendment_number%TYPE DEFAULT NULL
  , p_results_audit_status           IN       opening.opening_status_code%TYPE DEFAULT NULL
  , p_results_regime_status          IN       standards_regime.standards_regime_status_code%TYPE DEFAULT NULL
  , p_force_event_create             IN       BOOLEAN DEFAULT FALSE
  , p_update_userid                  IN       VARCHAR);

  PROCEDURE insert_audit_detail(
    p_table_name                     IN       results_audit_detail.table_name%TYPE DEFAULT NULL
  , p_column_name                    IN       results_audit_detail.column_name%TYPE DEFAULT NULL
  , p_business_identifier            IN       results_audit_detail.business_identifier%TYPE DEFAULT NULL
  , p_old_value                      IN       results_audit_detail.old_value%TYPE DEFAULT NULL
  , p_new_value                      IN       results_audit_detail.new_value%TYPE DEFAULT NULL);

  PROCEDURE insert_audit_geom_detail(
    p_table_name                     IN       results_audit_detail.table_name%TYPE DEFAULT NULL
  , p_column_name                    IN       results_audit_detail.column_name%TYPE DEFAULT NULL
  , p_business_identifier            IN       results_audit_detail.business_identifier%TYPE DEFAULT NULL
  , p_old_value                      IN       results_audit_detail.old_value%TYPE DEFAULT NULL
  , p_new_value                      IN       results_audit_detail.new_value%TYPE DEFAULT NULL
  , p_feature_area                   IN       results_audit_geometry.feature_area%TYPE DEFAULT NULL
  , p_feature_perimeter              IN       results_audit_geometry.feature_perimeter%TYPE DEFAULT NULL
  , p_capture_method_code            IN       results_audit_geometry.capture_method_code%TYPE DEFAULT NULL
  , p_data_source_code               IN       results_audit_geometry.data_source_code%TYPE DEFAULT NULL
  , p_feature_class_skey             IN       results_audit_geometry.feature_class_skey%TYPE DEFAULT NULL
  , p_observation_date               IN       results_audit_geometry.observation_date%TYPE DEFAULT NULL
  , p_data_quality_comment           IN       results_audit_geometry.data_quality_comment%TYPE DEFAULT NULL
  , p_entry_userid                   IN       results_audit_geometry.update_userid%TYPE DEFAULT NULL
  , p_entry_timestamp                IN       results_audit_geometry.update_timestamp%TYPE DEFAULT NULL
  , p_update_userid                  IN       results_audit_geometry.update_userid%TYPE DEFAULT NULL
  , p_update_timestamp               IN       results_audit_geometry.update_timestamp%TYPE DEFAULT NULL
  , p_revision_count                 IN       results_audit_geometry.revision_count%TYPE DEFAULT NULL
  , p_geometry                       IN       results_audit_geometry.geometry%TYPE DEFAULT NULL);

  PROCEDURE audit_regime_change(
    p_opening_id                     IN       NUMBER
  , p_new_standards_regime_id        IN       NUMBER
  , p_old_standards_regime_id        IN       NUMBER
  , p_audit_action_code              IN       VARCHAR2
  , p_xml_submission_id              IN       NUMBER
  , p_email_sent_ind                 IN       VARCHAR2
  , p_update_userid                  IN       VARCHAR2
  , p_disable_auditing_after         IN       BOOLEAN DEFAULT FALSE);

  PROCEDURE audit_frpa197_regime_change(
    p_opening_id                     IN       NUMBER
  , p_new_standards_regime_id        IN       NUMBER
  , p_old_standards_regime_id        IN       NUMBER
  , p_audit_action_code              IN       VARCHAR2
  , p_business_identifier            IN       VARCHAR2
  , p_xml_submission_id              IN       NUMBER
  , p_email_sent_ind                 IN       VARCHAR2
  , p_update_userid                  IN       VARCHAR2
  , p_disable_auditing_after         IN       BOOLEAN DEFAULT FALSE);

  PROCEDURE set_email_sent_ind(
    p_results_audit_event_id         IN       NUMBER
  , p_user_id                        IN       VARCHAR2
  , p_email_sent_ind                 IN       VARCHAR2);

  PROCEDURE delete_empty_event(
    p_results_audit_event_id         IN       NUMBER);

END results_audit;
/
CREATE OR REPLACE EDITIONABLE PACKAGE BODY "THE"."RESULTS_AUDIT" 
AS
--
--    Procedure:  INSERT_AUDIT_EVENT
--
--    Purpose:    This procedure will insert data into the RESULTS_AUDIT_EVENT
--                table based on the data that was passed in.
--
--                CR#46 (Release 1.3) Rules
--                - Only one UPD, VAR action will be created per day
--                - Only one AMD event will be created for an Amendment Number
--
  PROCEDURE insert_audit_event(
    p_opening_id                     IN       results_audit_event.opening_id%TYPE
        DEFAULT NULL
  , p_standards_regime_id            IN       results_audit_event.standards_regime_id%TYPE
        DEFAULT NULL
  , p_silviculture_project_id        IN       results_audit_event.silviculture_project_id%TYPE
        DEFAULT NULL
  , p_audit_action_code              IN       results_audit_event.results_audit_action_code%TYPE
        DEFAULT NULL
  , p_audit_description              IN       results_audit_event.description%TYPE
        DEFAULT NULL
  , p_email_sent_ind                 IN       results_audit_event.email_sent_ind%TYPE
        DEFAULT 'N'
  , p_user_id                        IN       results_audit_event.user_id%TYPE
        DEFAULT NULL
  , p_xml_submission_id              IN       results_audit_event.xml_submission_id%TYPE
        DEFAULT NULL
  , p_opening_amendment_number       IN       results_audit_event.opening_amendment_number%TYPE
        DEFAULT NULL
  , p_results_audit_status           IN       opening.opening_status_code%TYPE
        DEFAULT NULL
  , p_results_regime_status          IN       standards_regime.standards_regime_status_code%TYPE
        DEFAULT NULL
  , p_force_event_create             IN       BOOLEAN DEFAULT FALSE
  , p_update_userid                  IN       VARCHAR)
  IS
    l_table_name                       VARCHAR2(50);
    v_action_date                      DATE := SYSDATE;
    v_results_audit_event_id           results_audit_event.results_audit_event_id%TYPE;
    v_results_audit_status             opening.opening_status_code%TYPE;
    v_results_regime_status            standards_regime.standards_regime_status_code%TYPE;

    CURSOR c_exists_opening
    IS
      SELECT results_audit_event_id
        FROM results_audit_event
       WHERE Pkg_Sil_Date_Conversion.CONVERT_TO_CHAR_TRUNC_TIME(action_date,'RRRRMMDD') = Pkg_Sil_Date_Conversion.CONVERT_TO_CHAR_TRUNC_TIME(SYSDATE,'RRRRMMDD')--TRUNC(action_date) = TRUNC(SYSDATE)
         AND results_audit_action_code = p_audit_action_code
         AND opening_id = p_opening_id
         AND results_audit_event_id = (select max(results_audit_event_id)
                                         FROM results_audit_event
                                        WHERE opening_id = p_opening_id);

    CURSOR c_exists_project
    IS
      SELECT results_audit_event_id
        FROM results_audit_event
       WHERE Pkg_Sil_Date_Conversion.CONVERT_TO_CHAR_TRUNC_TIME(action_date,'RRRRMMDD') = Pkg_Sil_Date_Conversion.CONVERT_TO_CHAR_TRUNC_TIME(SYSDATE,'RRRRMMDD')--TRUNC(action_date) = TRUNC(SYSDATE)
         AND results_audit_action_code = p_audit_action_code
         AND silviculture_project_id = p_silviculture_project_id
         AND results_audit_event_id = (select max(results_audit_event_id)
                                         FROM results_audit_event
                                        WHERE silviculture_project_id = p_silviculture_project_id);

    CURSOR amd_exists_cur
    IS
      SELECT MAX(results_audit_event_id)
        FROM results_audit_event a
       WHERE results_audit_action_code = 'AMD'
         AND opening_id = p_opening_id
         AND NOT EXISTS(
              SELECT 1
                FROM results_audit_event b
               WHERE b.opening_id = a.opening_id
                 AND b.action_date > a.action_date
                 AND b.results_audit_action_code IN('SUB', 'REJ', 'DEL', 'APP'));

    CURSOR cor_exists_cur IS
      SELECT MAX(results_audit_event_id)
        FROM results_audit_event rae
       WHERE results_audit_action_code = 'COR'
         AND opening_id = p_opening_id
         AND Pkg_Sil_Date_Conversion.CONVERT_TO_CHAR_TRUNC_TIME(rae.action_date,'RRRRMMDD') = Pkg_Sil_Date_Conversion.CONVERT_TO_CHAR_TRUNC_TIME(SYSDATE,'RRRRMMDD');

  BEGIN
    v_results_audit_status := p_results_audit_status;
    v_results_regime_status := p_results_regime_status;

    --Get opening status if it was not passed
    IF p_opening_id IS NOT NULL
       AND v_results_audit_status IS NULL THEN
      BEGIN
        l_table_name := 'OPENING';

        SELECT opening_status_code
          INTO v_results_audit_status
          FROM opening
         WHERE opening_id = p_opening_id;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          --Key pre-generated for Electronic Submission
          IF p_audit_action_code != 'ES' THEN
            RAISE;
          END IF;
      END;
    END IF;

    --Get regime status if it was not passed
    IF p_standards_regime_id IS NOT NULL
       AND v_results_regime_status IS NULL THEN
      BEGIN
        l_table_name := 'STANDARDS_REGIME';

        SELECT standards_regime_status_code
          INTO v_results_regime_status
          FROM standards_regime
         WHERE standards_regime_id = p_standards_regime_id;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          --Key pre-generated for Electronic Submission
          IF p_audit_action_code != 'ES' THEN
            RAISE;
          END IF;
      END;
    END IF;

    --AMD - ensure we only have one amendent audit record until amendment is done
    IF p_audit_action_code = 'AMD'
       AND NOT p_force_event_create THEN
      --Determine if an AMD audit event is open (i.e. has not been SUB,APP,DEL,REJ)
      --This will miss Site Plan Amendments as they status is always APP - this is desired behaviour.
      OPEN amd_exists_cur;

      FETCH  amd_exists_cur
        INTO v_results_audit_event_id;

      CLOSE amd_exists_cur;

    ELSIF p_audit_action_code = 'COR'
      AND NOT p_force_event_create THEN
      -- Determine if there is a COR event already created in the last 24 hours
      -- to add all the changes audited underneath
      OPEN cor_exists_cur;

      FETCH cor_exists_cur
       INTO v_results_audit_event_id;

    --UPD, VAR, MIN, COR - ensure we only have one audit record per DAY
    ELSIF p_audit_action_code IN('UPD', 'VAR', 'MIN', 'COR','MIL','SPA', '197')
          AND NOT p_force_event_create THEN
      --Determine if an UPD audit event has already been created today
      IF p_opening_id IS NOT NULL THEN
        OPEN c_exists_opening;

        FETCH  c_exists_opening
          INTO v_results_audit_event_id;

        CLOSE c_exists_opening;
      ELSIF p_silviculture_project_id IS NOT NULL THEN
        OPEN c_exists_project;

        FETCH  c_exists_project
          INTO v_results_audit_event_id;

        CLOSE c_exists_project;
      END IF;
    END IF;

    --If we need to create a record
    IF v_results_audit_event_id IS NULL THEN
      SELECT rae_seq.NEXTVAL
        INTO v_results_audit_event_id
        FROM DUAL;

      l_table_name := 'RESULTS_AUDIT_EVENT';

      INSERT INTO results_audit_event
             (results_audit_event_id
            , standards_regime_id
            , silviculture_project_id
            , opening_id
            , opening_amendment_number
            , results_audit_action_code
            , action_date
            , email_sent_ind
            , user_id
            , xml_submission_id
            , description
            , entry_userid
            , entry_timestamp)
      VALUES (v_results_audit_event_id
            , p_standards_regime_id
            , p_silviculture_project_id
            , p_opening_id
            , p_opening_amendment_number
            , p_audit_action_code
            , v_action_date
            , p_email_sent_ind
            , p_user_id
            , p_xml_submission_id
            , p_audit_description
            , p_update_userid
            , SYSDATE);
    END IF;

    --Clear out and set the globals that will be used in the auditing triggers
    results_globals.reset_all;
    results_globals.set_opening_id(p_opening_id);
    results_globals.set_standards_regime_id(p_standards_regime_id);
    results_globals.set_opening_amendment_number(p_opening_amendment_number);
    results_globals.set_results_audit_status(v_results_audit_status);
    results_globals.set_results_regime_status(v_results_regime_status);
    results_globals.set_results_audit_event_id(v_results_audit_event_id);
    results_globals.set_update_userid(p_update_userid);
  END insert_audit_event;

--
--    Procedure:  INSERT_AUDIT_DETAIL
--
--    Purpose:   This procedure will insert data into the RESULTS_AUDIT_DETAILS
--               table. This procedure will be invoked by database triggers when
--               a table is inserted, updated or deleted. A caveat that must be
--               considered is the status of the opening or standards regime.
--               The database trigger should only call this procedure if the
--               standard regime status = 'APP' or 'AMD'
--
--                CR#46 (Release 1.3) Rules
--                - Only one UPD, VAR action will be created per day
--                - Only one AMD event will be created for an Amendment Number
--
  PROCEDURE insert_audit_detail(
    p_table_name                     IN       results_audit_detail.table_name%TYPE
        DEFAULT NULL
  , p_column_name                    IN       results_audit_detail.column_name%TYPE
        DEFAULT NULL
  , p_business_identifier            IN       results_audit_detail.business_identifier%TYPE
        DEFAULT NULL
  , p_old_value                      IN       results_audit_detail.old_value%TYPE
        DEFAULT NULL
  , p_new_value                      IN       results_audit_detail.new_value%TYPE
        DEFAULT NULL)
  IS
    l_table_name                       VARCHAR2(50);
    v_results_audit_event_id           results_audit_event.results_audit_event_id%TYPE;
    v_update_userid                    results_audit_detail.entry_userid%TYPE;
  BEGIN
    v_results_audit_event_id := results_globals.get_results_audit_event_id;
    v_update_userid := results_globals.get_update_userid;
    l_table_name := 'RESULTS_AUDIT_DETAIL';

    INSERT INTO results_audit_detail
           (results_audit_detail_id
          , results_audit_event_id
          , table_name
          , column_name
          , business_identifier
          , old_value
          , new_value
          , entry_userid
          , entry_timestamp)
    VALUES (rad_seq.NEXTVAL
          , v_results_audit_event_id
          , p_table_name
          , p_column_name
          , p_business_identifier
          , p_old_value
          , p_new_value
          , v_update_userid
          , SYSDATE);

  END insert_audit_detail;

--
--    Procedure: INSERT_AUDIT_GEOM_DETAIL
--
--    Purpose:   Audits change in geometry
--
  PROCEDURE insert_audit_geom_detail(
    p_table_name                     IN       results_audit_detail.table_name%TYPE DEFAULT NULL
  , p_column_name                    IN       results_audit_detail.column_name%TYPE DEFAULT NULL
  , p_business_identifier            IN       results_audit_detail.business_identifier%TYPE DEFAULT NULL
  , p_old_value                      IN       results_audit_detail.old_value%TYPE DEFAULT NULL
  , p_new_value                      IN       results_audit_detail.new_value%TYPE DEFAULT NULL
  , p_feature_area                   IN       results_audit_geometry.feature_area%TYPE DEFAULT NULL
  , p_feature_perimeter              IN       results_audit_geometry.feature_perimeter%TYPE DEFAULT NULL
  , p_capture_method_code            IN       results_audit_geometry.capture_method_code%TYPE DEFAULT NULL
  , p_data_source_code               IN       results_audit_geometry.data_source_code%TYPE DEFAULT NULL
  , p_feature_class_skey             IN       results_audit_geometry.feature_class_skey%TYPE DEFAULT NULL
  , p_observation_date               IN       results_audit_geometry.observation_date%TYPE DEFAULT NULL
  , p_data_quality_comment           IN       results_audit_geometry.data_quality_comment%TYPE DEFAULT NULL
  , p_entry_userid                   IN       results_audit_geometry.update_userid%TYPE DEFAULT NULL
  , p_entry_timestamp                IN       results_audit_geometry.update_timestamp%TYPE DEFAULT NULL
  , p_update_userid                  IN       results_audit_geometry.update_userid%TYPE DEFAULT NULL
  , p_update_timestamp               IN       results_audit_geometry.update_timestamp%TYPE DEFAULT NULL
  , p_revision_count                 IN       results_audit_geometry.revision_count%TYPE DEFAULT NULL
  , p_geometry                       IN       results_audit_geometry.geometry%TYPE DEFAULT NULL)
  IS
    l_table_name                       VARCHAR2(50);
    v_results_audit_event_id           results_audit_event.results_audit_event_id%TYPE;
    v_update_userid                    results_audit_detail.entry_userid%TYPE;
    v_results_audit_detail_id          results_audit_detail.results_audit_detail_id%TYPE;
  BEGIN
    v_results_audit_event_id := results_globals.get_results_audit_event_id;
    v_update_userid := results_globals.get_update_userid;
    l_table_name := 'RESULTS_AUDIT_DETAIL';

    INSERT INTO results_audit_detail
           (results_audit_detail_id
          , results_audit_event_id
          , table_name
          , column_name
          , business_identifier
          , old_value
          , new_value
          , entry_userid
          , entry_timestamp)
    VALUES (rad_seq.NEXTVAL
          , v_results_audit_event_id
          , p_table_name
          , p_column_name
          , p_business_identifier
          , p_old_value
          , p_new_value
          , v_update_userid
          , SYSDATE)
      RETURNING results_audit_detail_id INTO v_results_audit_detail_id;

    --audit the geometry (this is more of an archival process)
    results_audit_geom.init;
    results_audit_geom.set_results_audit_detail_id(v_results_audit_detail_id);
    results_audit_geom.set_geometry(p_geometry);
    results_audit_geom.set_feature_area(p_feature_area);
    results_audit_geom.set_feature_perimeter(p_feature_perimeter);
    results_audit_geom.set_feature_class_skey(p_feature_class_skey);
    results_audit_geom.set_capture_method_code(p_capture_method_code);
    results_audit_geom.set_data_source_code(p_data_source_code);
    results_audit_geom.set_observation_date(p_observation_date);
    results_audit_geom.set_data_quality_comment(p_data_quality_comment);
    results_audit_geom.set_entry_userid(p_entry_userid);
    results_audit_geom.set_entry_timestamp(p_entry_timestamp);
    results_audit_geom.set_update_userid(p_update_userid);
    results_audit_geom.set_update_timestamp(p_update_timestamp);
    results_audit_geom.set_revision_count(p_revision_count);
    results_audit_geom.add;

  END insert_audit_geom_detail;

--
--     Procedure:  AUDIT_REGIME_CHANGE
--
--     Purpose:    This procedure will audit a change in a Stocking Standard
--                 Unit Regime
--
--                 Pass p_disable_auditing_after=TRUE to disable auditing after
--                 the audit detail is created. NOTE: Auditing will only be
--                 disabled if the regime changed.
--
  PROCEDURE audit_regime_change(
    p_opening_id                     IN       NUMBER
  , p_new_standards_regime_id        IN       NUMBER
  , p_old_standards_regime_id        IN       NUMBER
  , p_audit_action_code              IN       VARCHAR2
  , p_xml_submission_id              IN       NUMBER
  , p_email_sent_ind                 IN       VARCHAR2
  , p_update_userid                  IN       VARCHAR2
  , p_disable_auditing_after         IN       BOOLEAN DEFAULT FALSE)
  IS
  BEGIN

  IF nvl(p_new_standards_regime_id, -1) != nvl(p_old_standards_regime_id, -1) THEN

        if p_audit_action_code = '197' then

          --Call the audit package
          results_audit.insert_audit_event
                                         (p_opening_id => p_opening_id
                                        , p_standards_regime_id => p_new_standards_regime_id
                                        , p_audit_action_code => '197'
                                        , p_audit_description => 'Standard was changed'
                                        , p_xml_submission_id => p_xml_submission_id
                                        , p_email_sent_ind => p_email_sent_ind
                                        , p_update_userid => p_update_userid);
       else

          --Call the audit package
          results_audit.insert_audit_event
                                         (p_opening_id => p_opening_id
                                        , p_standards_regime_id => p_new_standards_regime_id
                                        , p_audit_action_code => p_audit_action_code
                                        , p_audit_description => 'Standard was changed'
                                        , p_xml_submission_id => p_xml_submission_id
                                        , p_email_sent_ind => p_email_sent_ind
                                        , p_update_userid => p_update_userid);
       end if;

      results_audit.insert_audit_detail(p_table_name => 'STOCKING_STANDARD_UNIT'
                                      , p_column_name => 'STANDARDS_REGIME_ID'
                                      , p_old_value => p_old_standards_regime_id
                                      , p_new_value => p_new_standards_regime_id
                                      , p_business_identifier => 'Regime changed.');

      IF p_disable_auditing_after THEN
        results_globals.reset_all;
        results_globals.set_auditing_enabled_ind('N');
      END IF;
    END IF;
  END audit_regime_change;

--
--    Procedure:  set_email_sent_ind
--
--    Purpose:    Sets RESULTS_AUDIT_EVENT.EMAIL_SENT_IND to Y for the last
--                APP/REJ event for the given opening.
--


PROCEDURE audit_frpa197_regime_change(
    p_opening_id                     IN       NUMBER
  , p_new_standards_regime_id        IN       NUMBER
  , p_old_standards_regime_id        IN       NUMBER
  , p_audit_action_code              IN       VARCHAR2
  , p_business_identifier            IN       VARCHAR2
  , p_xml_submission_id              IN       NUMBER
  , p_email_sent_ind                 IN       VARCHAR2
  , p_update_userid                  IN       VARCHAR2
  , p_disable_auditing_after         IN       BOOLEAN DEFAULT FALSE)
  IS
  BEGIN

  IF nvl(p_new_standards_regime_id, -1) != nvl(p_old_standards_regime_id, -1) THEN
      --Call the audit package
      results_audit.insert_audit_event
                                     (p_opening_id => p_opening_id
                                    , p_standards_regime_id => p_new_standards_regime_id
                                    , p_audit_action_code => p_audit_action_code
                                    , p_audit_description => 'Standard was changed to FRPA 197'
                                    , p_xml_submission_id => p_xml_submission_id
                                    , p_email_sent_ind => p_email_sent_ind
                                    , p_update_userid => p_update_userid);

     -- audit detail is inserted by trigger when regime is changed

      IF p_disable_auditing_after THEN
        results_globals.reset_all;
        results_globals.set_auditing_enabled_ind('N');
      END IF;
    END IF;
  END audit_frpa197_regime_change;

--
--    Procedure:  set_email_sent_ind
--
--    Purpose:    Sets RESULTS_AUDIT_EVENT.EMAIL_SENT_IND to Y for the last
--                APP/REJ event for the given opening.
--

  PROCEDURE set_email_sent_ind(
    p_results_audit_event_id         IN       NUMBER
  , p_user_id                        IN       VARCHAR2
  , p_email_sent_ind                 IN       VARCHAR2)
  IS
  BEGIN
    UPDATE results_audit_event rae
       SET rae.email_sent_ind = p_email_sent_ind
         , rae.user_id = p_user_id
     WHERE rae.results_audit_event_id = p_results_audit_event_id;
  END set_email_sent_ind;

--
--    Procedure:  delete_empty_event
--
--    Purpose:    Delete an audit event with no records saved underneath
--
  PROCEDURE delete_empty_event(
    p_results_audit_event_id         IN       NUMBER)
  IS
  BEGIN
    DELETE results_audit_event rae
     WHERE rae.results_audit_event_id = p_results_audit_event_id
       AND NOT EXISTS (SELECT results_audit_detail_id
                         FROM results_audit_detail rad
                        WHERE results_audit_event_id = rae.results_audit_event_id);
  END delete_empty_event;

END results_audit;
/

  CREATE OR REPLACE EDITIONABLE PACKAGE "THE"."RESULTS_GLOBALS" 
IS

--
--    Package:    RESULTS_GLOBALS
--    Purpose:    This package will be used to set globals used in event tracking.
--
--    Revision History
--    Person               Date       Comments
--    -----------------    ---------  --------------------------------
--    wcound (Pangaea)     2003-03-06 Created
--    R.A.Robb             2004-05-13 CR#46 - Added opening_amendment_number and
--                                    reset_all
--    E.Wong               2006-05-01 Addition of global variable of silviculture
--                                    project id for audit.
--    R.A.Robb             2006-05-25 Don't default auditing_enabled_ind to 'Y'
--                                    except in RESET_ALL
--    R.Pardo Figueroa     2007-04-27 PT#36244 - Create record to load audit constants,
--                                    as well as set() and get() functions for these
--                                    constants, to prevent audit values to be overwritten
--                                    when audit is performed
--

   TYPE ref_cur_t IS REF CURSOR;

   type audit_constants_rec is record (
      opening_id               NUMBER(10)
     ,standards_regime_id      NUMBER(10)
     ,silviculture_project_id  NUMBER(10)
     ,results_audit_event_id   NUMBER(10)
     ,update_userid            VARCHAR2(30)
     ,results_regime_status    VARCHAR2(3)
     ,results_audit_status     VARCHAR2(3)
     ,opening_amendment_number NUMBER(5)
     ,auditing_enabled_ind     VARCHAR2(1)
   );

   PROCEDURE RESET_ALL;

   PROCEDURE SET_OPENING_ID (P_OPENING_ID IN OPENING.OPENING_ID%TYPE);

   FUNCTION GET_OPENING_ID RETURN NUMBER;

   PROCEDURE SET_STANDARDS_REGIME_ID (P_STANDARDS_REGIME_ID IN STANDARDS_REGIME.STANDARDS_REGIME_ID%TYPE);

   FUNCTION GET_STANDARDS_REGIME_ID RETURN NUMBER;

   PROCEDURE SET_SILVICULTURE_PROJECT_ID (P_SILVICULTURE_PROJECT_ID IN SILVICULTURE_PROJECT.SILVICULTURE_PROJECT_ID%TYPE);

   FUNCTION GET_SILVICULTURE_PROJECT_ID RETURN NUMBER;

   PROCEDURE SET_UPDATE_USERID (P_UPDATE_USERID IN OPENING.UPDATE_USERID%TYPE);

   FUNCTION GET_UPDATE_USERID RETURN VARCHAR2;

   PROCEDURE SET_RESULTS_AUDIT_EVENT_ID (P_RESULTS_AUDIT_EVENT_ID IN RESULTS_AUDIT_EVENT.RESULTS_AUDIT_EVENT_ID%TYPE);

   FUNCTION GET_RESULTS_AUDIT_EVENT_ID RETURN NUMBER;

   PROCEDURE SET_RESULTS_AUDIT_STATUS (P_RESULTS_AUDIT_STATUS IN VARCHAR2);

   FUNCTION GET_RESULTS_AUDIT_STATUS RETURN VARCHAR2;

   PROCEDURE SET_RESULTS_REGIME_STATUS (P_RESULTS_REGIME_STATUS IN VARCHAR2);

   FUNCTION GET_RESULTS_REGIME_STATUS RETURN VARCHAR2;

   PROCEDURE SET_OPENING_AMENDMENT_NUMBER (P_OPENING_AMENDMENT_NUMBER IN RESULTS_AUDIT_EVENT.OPENING_AMENDMENT_NUMBER%TYPE);

   FUNCTION GET_OPENING_AMENDMENT_NUMBER RETURN NUMBER;

   PROCEDURE SET_AUDITING_ENABLED_IND(P_AUDITING_ENABLED_IND IN VARCHAR2);

   FUNCTION GET_AUDITING_ENABLED_IND RETURN VARCHAR2;

   PROCEDURE PRESERVE_AUDIT_SETTINGS;

   PROCEDURE RECOVER_PREV_AUDIT_SETTINGS;

END Results_Globals;
/
CREATE OR REPLACE EDITIONABLE PACKAGE BODY "THE"."RESULTS_GLOBALS" AS

  g_opening_id               RESULTS_AUDIT_EVENT.OPENING_ID%TYPE;
  g_standards_regime_id      RESULTS_AUDIT_EVENT.STANDARDS_REGIME_ID%TYPE;
  g_silviculture_project_id  RESULTS_AUDIT_EVENT.SILVICULTURE_PROJECT_ID%TYPE;
  g_results_audit_event_id   RESULTS_AUDIT_EVENT.RESULTS_AUDIT_EVENT_ID%TYPE;
  g_update_userid            RESULTS_AUDIT_DETAIL.ENTRY_USERID%TYPE;
  g_results_regime_status    VARCHAR2(3);
  g_results_audit_status     VARCHAR2(3);
  g_opening_amendment_number RESULTS_AUDIT_EVENT.OPENING_AMENDMENT_NUMBER%TYPE;
  g_auditing_enabled_ind     VARCHAR2(1);

  g_audit_record audit_constants_rec;

--
--    Procedure:  RESET_ALL
--
--    Purpose:    To reset all session globals
--

  PROCEDURE RESET_ALL
  IS

  BEGIN
    g_opening_id := NULL;
    g_standards_regime_id  := NULL;
    g_silviculture_project_id  := NULL;
    g_results_audit_event_id  := NULL;
    g_update_userid  := NULL;
    g_results_regime_status  := NULL;
    g_results_audit_status  := NULL;
    g_opening_amendment_number  := NULL;
    g_auditing_enabled_ind := 'Y';

  END RESET_ALL;

--
--    Procedure:  SET_OPENING_ID
--
--    Purpose:   To set the global for the opening id.
--

  PROCEDURE SET_OPENING_ID(p_opening_id IN OPENING.OPENING_ID%TYPE)
  IS

  BEGIN
    g_opening_id := p_opening_id;

  END SET_OPENING_ID;

--
--    Function:  GET_OPENING_ID
--
--    Purpose:    To retrieve the global for the opening id.
--

  FUNCTION GET_OPENING_ID RETURN NUMBER
  IS

  BEGIN
    RETURN  g_opening_id;

  END GET_OPENING_ID;

--
--    Procedure:  SET_STANDARDS_REGIME_ID
--
--    Purpose:    To set the global for the standards regime id.
--

  PROCEDURE SET_STANDARDS_REGIME_ID (P_STANDARDS_REGIME_ID IN STANDARDS_REGIME.STANDARDS_REGIME_ID%TYPE)
  IS

  BEGIN
    g_standards_regime_id := p_standards_regime_id;

  END SET_STANDARDS_REGIME_ID;

--
--    Function: GET_STANDARDS_REGIME_ID
--
--    Purpose:    To retrieve the global for the standards_regime_id.
--

  FUNCTION GET_STANDARDS_REGIME_ID RETURN NUMBER
  IS

  BEGIN
    RETURN g_standards_regime_id;

  END GET_STANDARDS_REGIME_ID;

--
--    Procedure:  SET_SILVICULTURE_PROJECT_ID
--
--    Purpose:    To set the global for the silviculture project id.
--

  PROCEDURE SET_SILVICULTURE_PROJECT_ID (P_SILVICULTURE_PROJECT_ID IN SILVICULTURE_PROJECT.SILVICULTURE_PROJECT_ID%TYPE)
  IS

  BEGIN
    g_silviculture_project_id := p_silviculture_project_id;

  END SET_SILVICULTURE_PROJECT_ID;

--
--    Function: GET_SILVICULTURE_PROJECT_ID
--
--    Purpose:    To retrieve the global for the silviculture_project_id.
--

  FUNCTION GET_SILVICULTURE_PROJECT_ID RETURN NUMBER
  IS

  BEGIN
    RETURN g_silviculture_project_id;

  END GET_SILVICULTURE_PROJECT_ID;

--
--    Procedure:  SET_UPDATE_USERID
--
--    Purpose:   To set the global for the update_userid.
--

  PROCEDURE SET_UPDATE_USERID(P_UPDATE_USERID IN OPENING.UPDATE_USERID%TYPE)
  IS

  BEGIN
    g_update_userid  := P_update_userid;

  END SET_UPDATE_USERID;

--
--    Function:  GET_OPENING_ID
--
--    Purpose:    To retrieve the global for the update_userid.
--

  FUNCTION GET_UPDATE_USERID RETURN VARCHAR2
  IS

  BEGIN
    RETURN  g_update_userid;

  END GET_UPDATE_USERID;

--
--    Procedure:  SET_RESULTS_AUDIT_EVENT_ID
--
--    Purpose:   To set the global for the opening id.
--

  PROCEDURE SET_RESULTS_AUDIT_EVENT_ID (P_RESULTS_AUDIT_EVENT_ID IN RESULTS_AUDIT_EVENT.RESULTS_AUDIT_EVENT_ID%TYPE)
  IS

  BEGIN
    g_results_audit_event_id := p_results_audit_event_id;

  END SET_RESULTS_AUDIT_EVENT_ID;

--
--    Function:  GET_RESULTS_AUDIT_EVENT_ID
--
--    Purpose:    To retrieve the global for the opening id.
--

  FUNCTION GET_RESULTS_AUDIT_EVENT_ID RETURN NUMBER
  IS

  BEGIN
    RETURN  g_results_audit_event_id;

  END GET_RESULTS_AUDIT_EVENT_ID;

--
--    Procedure:  SET_RESULTS_AUDIT_STATUS
--
--    Purpose:   To set the global for the opening id.
--

  PROCEDURE SET_RESULTS_AUDIT_STATUS (P_RESULTS_AUDIT_STATUS IN VARCHAR2)
  IS

  BEGIN
    g_results_audit_status := p_results_audit_status;

  END SET_RESULTS_AUDIT_STATUS;

--
--    Function:  GET_RESULTS_AUDIT_STATUS
--
--    Purpose:    To retrieve the global for the opening id.
--

  FUNCTION GET_RESULTS_AUDIT_STATUS RETURN VARCHAR2
  IS

  BEGIN
    RETURN  g_results_audit_status;

  END GET_RESULTS_AUDIT_STATUS;

--
--    Procedure:  SET_RESULTS_REGIME_STATUS
--
--    Purpose:   To set the global for the regime status.
--

  PROCEDURE SET_RESULTS_REGIME_STATUS (P_RESULTS_REGIME_STATUS IN VARCHAR2)
  IS

  BEGIN
    g_results_regime_status := p_results_regime_status;

  END SET_RESULTS_REGIME_STATUS;

--
--    Function:  GET_RESULTS_REGIME_STATUS
--
--    Purpose:    To retrieve the global for the regime status.
--

  FUNCTION GET_RESULTS_REGIME_STATUS RETURN VARCHAR2
  IS

  BEGIN
    RETURN  g_results_regime_status;

  END GET_RESULTS_REGIME_STATUS;

--
--    Procedure:  SET_OPENING_AMENDMENT_NUMBER
--
--    Purpose:   To set the global for the Opening Amendment Number
--

  PROCEDURE SET_OPENING_AMENDMENT_NUMBER(P_OPENING_AMENDMENT_NUMBER IN RESULTS_AUDIT_EVENT.OPENING_AMENDMENT_NUMBER%TYPE)
  IS

  BEGIN
    g_opening_amendment_number := p_opening_amendment_number;

  END SET_OPENING_AMENDMENT_NUMBER;

--
--    Function:  GET_OPENING_AMENDMENT_NUMBER
--
--    Purpose:    To retrieve the global for the Opening Amendment Number
--

  FUNCTION GET_OPENING_AMENDMENT_NUMBER RETURN NUMBER
  IS

  BEGIN
    RETURN g_opening_amendment_number;

  END GET_OPENING_AMENDMENT_NUMBER;

--
--    Procedure:  SET_AUDITING_ENABLED_IND
--
--    Purpose:   To set the global for the Auditing Enabled Indicator
--

  PROCEDURE SET_AUDITING_ENABLED_IND(P_AUDITING_ENABLED_IND IN VARCHAR2)
  IS

  BEGIN
    g_auditing_enabled_ind := p_auditing_enabled_ind;

  END SET_AUDITING_ENABLED_IND;

--
--    Function:  GET_AUDITING_ENABLED_IND
--
--    Purpose:    To retrieve the global for the Opening Amendment Number
--

  FUNCTION GET_AUDITING_ENABLED_IND RETURN VARCHAR2
  IS

  BEGIN
    RETURN NVL(g_auditing_enabled_ind,'N');

  END GET_AUDITING_ENABLED_IND;

--
--    Procedure:  PRESERVE_AUDIT_SETTINGS
--
--    Purpose:   Keep current values of audit variables
--

  PROCEDURE PRESERVE_AUDIT_SETTINGS IS
  begin
    g_audit_record.opening_id               := g_opening_id;
    g_audit_record.standards_regime_id      := g_standards_regime_id;
    g_audit_record.silviculture_project_id  := g_silviculture_project_id;
    g_audit_record.results_audit_event_id   := g_results_audit_event_id;
    g_audit_record.update_userid            := g_update_userid;
    g_audit_record.results_regime_status    := g_results_regime_status;
    g_audit_record.results_audit_status     := g_results_audit_status;
    g_audit_record.opening_amendment_number := g_opening_amendment_number;
    g_audit_record.auditing_enabled_ind     := g_auditing_enabled_ind;
  END;

--
--    Procedure:  RECOVER_PREV_AUDIT_SETTINGS
--
--    Purpose:   To set the global for the Auditing Enabled Indicator
--

  PROCEDURE RECOVER_PREV_AUDIT_SETTINGS IS
  begin
    g_opening_id               := g_audit_record.opening_id;
    g_standards_regime_id      := g_audit_record.standards_regime_id;
    g_silviculture_project_id  := g_audit_record.silviculture_project_id;
    g_results_audit_event_id   := g_audit_record.results_audit_event_id;
    g_update_userid            := g_audit_record.update_userid;
    g_results_regime_status    := g_audit_record.results_regime_status;
    g_results_audit_status     := g_audit_record.results_audit_status;
    g_opening_amendment_number := g_audit_record.opening_amendment_number;
    g_auditing_enabled_ind     := g_audit_record.auditing_enabled_ind;
  END;
END Results_Globals;
/

  CREATE OR REPLACE EDITIONABLE PACKAGE "THE"."PKG_SIL_DATE_CONVERSION" AS
/******************************************************************************
    Package:    PKG_SIL_DATE_CONVERSION

    Purpose:     Procedures to convert date characteristics to either dates from
                 characters or from characters to dates based on passed parms.
                 If any raised error is wanted to be hidden then pass the
                 p_error_ind = 'N' and it will hide any Oracle error conditions.
                 If the date format is not provided for character conversion then
                 the format of 'YYYY-MM-DD' will be used.

    Revision History

    Person             Date        Comments
    -----------------  ----------  --------------------------------
    PDS                2001-10-15  Created
    R.A.Robb           2004-05-14  Added CONVERT_TO_CHAR_TRUNC_TIME and
                                   set defaults for all

******************************************************************************/
    FUNCTION CONVERT_TO_CHAR ( p_value        IN     DATE
                             , p_format       IN     VARCHAR2 DEFAULT 'YYYY-MM-DD'
                             , p_error_ind    IN     VARCHAR2 DEFAULT 'Y') RETURN VARCHAR2;

    FUNCTION CONVERT_TO_CHAR_TRUNC_TIME
                             ( p_value        IN     DATE
                             , p_date_format  IN     VARCHAR2 DEFAULT 'YYYY-MM-DD'
                             , p_time_format  IN     VARCHAR2 DEFAULT 'HH:MI:SS AM'
                             , p_error_ind    IN     VARCHAR2 DEFAULT 'Y') RETURN VARCHAR2;

    FUNCTION CONVERT_TO_DATE (p_value           IN VARCHAR2
                            , p_error_ind       IN VARCHAR2 DEFAULT 'Y') RETURN DATE;

END PKG_SIL_DATE_CONVERSION;
/
CREATE OR REPLACE EDITIONABLE PACKAGE BODY "THE"."PKG_SIL_DATE_CONVERSION" AS

    FUNCTION CONVERT_TO_CHAR_TRUNC_TIME
                             ( p_value        IN     DATE
                             , p_date_format  IN     VARCHAR2 DEFAULT 'YYYY-MM-DD'
                             , p_time_format  IN     VARCHAR2 DEFAULT 'HH:MI:SS AM'
                             , p_error_ind    IN     VARCHAR2 DEFAULT 'Y') RETURN VARCHAR2 IS
       v_return_value   VARCHAR2(50) := NULL;
    BEGIN
       --Don't return time portion if date is same as trunc(date)
       IF TRUNC(p_value) = p_value THEN
          v_return_value := TO_CHAR(p_value,p_date_format);
       ELSE
          v_return_value := TO_CHAR(p_value,p_date_format||' '||p_time_format);
       END IF;
       RETURN (v_return_value);
    EXCEPTION
      WHEN OTHERS THEN
        IF p_error_ind = 'Y' THEN
           RAISE;
        ELSE
           RETURN (NULL);
        END IF;

    END CONVERT_TO_CHAR_TRUNC_TIME;

    FUNCTION CONVERT_TO_CHAR ( p_value        IN     DATE
                             , p_format       IN     VARCHAR2 DEFAULT 'YYYY-MM-DD'
                             , p_error_ind    IN     VARCHAR2 DEFAULT 'Y') RETURN VARCHAR2 IS
       v_date_format    VARCHAR2(50) := 'YYYY-MM-DD';
       p_return_value   VARCHAR2(50) := NULL;
    BEGIN
       IF p_format IS NOT NULL THEN
          v_date_format := p_format;
       ELSE
          v_date_format := 'YYYY-MM-DD';
       END IF;
       p_return_value := TO_CHAR(p_value, v_date_format);
       RETURN (p_return_value);
    EXCEPTION
      WHEN OTHERS THEN
        IF p_error_ind = 'Y' THEN
           RAISE;
        ELSE
           RETURN (NULL);
        END IF;
    END CONVERT_TO_CHAR;

    FUNCTION CONVERT_TO_DATE (p_value           IN VARCHAR2
                            , p_error_ind       IN VARCHAR2 DEFAULT 'Y') RETURN DATE IS
      p_return_date   DATE  := NULL;
      dateFormat      VARCHAR2(50);
    BEGIN
      IF LENGTH(p_value) > 10 THEN
        dateFormat := 'YYYY-MM-DD HH24:MI:SS';
      ELSE
        dateFormat := 'YYYY-MM-DD';
      END IF;

      p_return_date := TO_DATE(p_value, dateFormat);
      RETURN (p_return_date);
    EXCEPTION
      WHEN OTHERS THEN
        IF p_error_ind = 'Y' THEN
           RAISE;
        ELSE
           RETURN (NULL);
        END IF;
    END CONVERT_TO_DATE;
END PKG_SIL_DATE_CONVERSION ;
/

  CREATE OR REPLACE EDITIONABLE PACKAGE "THE"."SIL_DATE_CONVERSION" AS
/******************************************************************************
    Package:    PKG_SIL_DATE_CONVERSION

    Purpose:     Procedures to convert date characteristics to either dates from
                 characters or from characters to dates based on passed parms.
                 If any raised error is wanted to be hidden then pass the
                 p_error_ind = 'N' and it will hide any Oracle error conditions.
                 If the date format is not provided for character conversion then
                 the format of 'YYYY-MM-DD' will be used.

    Revision History

    Person             Date        Comments
    -----------------  ----------  --------------------------------
    PDS                2001-10-15  Created
    R.A.Robb           2004-05-14  Added CONVERT_TO_CHAR_TRUNC_TIME and
                                   set defaults for all
******************************************************************************/
    FUNCTION CONVERT_TO_CHAR ( p_value        IN     DATE
                             , p_format       IN     VARCHAR2 DEFAULT 'YYYY-MM-DD'
                             , p_error_ind    IN     VARCHAR2 DEFAULT 'Y') RETURN VARCHAR2;

    FUNCTION CONVERT_TO_CHAR_TRUNC_TIME
                             ( p_value        IN     DATE
                             , p_date_format  IN     VARCHAR2 DEFAULT 'YYYY-MM-DD'
                             , p_time_format  IN     VARCHAR2 DEFAULT 'HH:MI:SS AM'
                             , p_error_ind    IN     VARCHAR2 DEFAULT 'Y') RETURN VARCHAR2;

    FUNCTION CONVERT_TO_DATE (p_value           IN VARCHAR2
                            , p_error_ind       IN VARCHAR2 DEFAULT 'Y') RETURN DATE;

END Sil_Date_Conversion;
/
CREATE OR REPLACE EDITIONABLE PACKAGE BODY "THE"."SIL_DATE_CONVERSION" AS

    FUNCTION CONVERT_TO_CHAR_TRUNC_TIME
                             ( p_value        IN     DATE
                             , p_date_format  IN     VARCHAR2 DEFAULT 'YYYY-MM-DD'
                             , p_time_format  IN     VARCHAR2 DEFAULT 'HH:MI:SS AM'
                             , p_error_ind    IN     VARCHAR2 DEFAULT 'Y') RETURN VARCHAR2 IS
       v_return_value   VARCHAR2(50) := NULL;
    BEGIN
       --Don't return time portion if date is same as trunc(date)
       IF TRUNC(p_value) = p_value THEN
          v_return_value := TO_CHAR(p_value,p_date_format);
       ELSE
          v_return_value := TO_CHAR(p_value,p_date_format||' '||p_time_format);
       END IF;
       RETURN (v_return_value);
    EXCEPTION
      WHEN OTHERS THEN
        IF p_error_ind = 'Y' THEN
           RAISE;
        ELSE
           RETURN (NULL);
        END IF;

    END CONVERT_TO_CHAR_TRUNC_TIME;

    FUNCTION CONVERT_TO_CHAR ( p_value        IN     DATE
                             , p_format       IN     VARCHAR2 DEFAULT 'YYYY-MM-DD'
                             , p_error_ind    IN     VARCHAR2 DEFAULT 'Y') RETURN VARCHAR2 IS
       v_date_format    VARCHAR2(50) := 'YYYY-MM-DD';
       p_return_value   VARCHAR2(50) := NULL;
    BEGIN
       IF p_format IS NOT NULL THEN
          v_date_format := p_format;
       ELSE
          v_date_format := 'YYYY-MM-DD';
       END IF;
       p_return_value := TO_CHAR(p_value, v_date_format);
       RETURN (p_return_value);
    EXCEPTION
      WHEN OTHERS THEN
        IF p_error_ind = 'Y' THEN
           RAISE;
        ELSE
           RETURN (NULL);
        END IF;
    END CONVERT_TO_CHAR;

    FUNCTION CONVERT_TO_DATE (p_value           IN VARCHAR2
                            , p_error_ind       IN VARCHAR2 DEFAULT 'Y') RETURN DATE IS
      p_return_date   DATE  := NULL;
      dateFormat      VARCHAR2(50);
    BEGIN
      IF LENGTH(p_value) > 10 THEN
        dateFormat := 'YYYY-MM-DD HH24:MI:SS';
      ELSE
        dateFormat := 'YYYY-MM-DD';
      END IF;

      p_return_date := TO_DATE(p_value, dateFormat);
      RETURN (p_return_date);
    EXCEPTION
      WHEN OTHERS THEN
        IF p_error_ind = 'Y' THEN
           RAISE;
        ELSE
           RETURN (NULL);
        END IF;
    END CONVERT_TO_DATE;
END Sil_Date_Conversion ;
/
