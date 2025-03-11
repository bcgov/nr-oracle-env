
  CREATE OR REPLACE EDITIONABLE TRIGGER "THE"."RESULTS_CBOA_AR_IUD_TRG"
/******************************************************************************
   Trigger: RESULTS_CBOA_AR_IUD_TRG
   Purpose: This trigger will call the  RESULTS_AUDIT.INSERT_AUDIT_DETAIL
            procedure when the status of the opening or standards regime is
            either  'APP' or 'AMD'. The procedure inserts data into the
            RESULTS_AUDIT_DETAIL table based on the data that was passed in.
   Revision History
   Person               Date       Comments
   -----------------    ---------  --------------------------------
   wcound (Pangaea)     2003-03-05 Created
   wcound               2003-03-27 Added NVL to Update to capture null values
   R.A.Robb             2004-05-14 CR#46 - updated auditing
******************************************************************************/
AFTER INSERT OR UPDATE OR DELETE ON CUT_BLOCK_OPEN_ADMIN FOR EACH ROW
DECLARE
 v_opening_id             RESULTS_AUDIT_EVENT.OPENING_ID%TYPE;
 v_results_audit_event_id RESULTS_AUDIT_EVENT.RESULTS_AUDIT_EVENT_ID%type;
 v_update_userid          RESULTS_AUDIT_DETAIL.ENTRY_USERID%TYPE;
 v_status                 VARCHAR2(3);
 v_table_name             RESULTS_AUDIT_DETAIL.TABLE_NAME%TYPE;
 v_column_name            RESULTS_AUDIT_DETAIL.COLUMN_NAME%TYPE;
 v_old_value              RESULTS_AUDIT_DETAIL.OLD_VALUE%TYPE;
 v_new_value              RESULTS_AUDIT_DETAIL.NEW_VALUE%TYPE;
 v_business_identifier    RESULTS_AUDIT_DETAIL.BUSINESS_IDENTIFIER%TYPE;
 v_audit_enabled_ind      VARCHAR2(1);

 BEGIN
 v_table_name := 'CUT_BLOCK_OPEN_ADMIN';
 v_opening_id := RESULTS_GLOBALS.GET_OPENING_ID;
 v_status := RESULTS_GLOBALS.GET_RESULTS_AUDIT_STATUS;
 v_audit_enabled_ind := RESULTS_GLOBALS.GET_AUDITING_ENABLED_IND;

IF v_audit_enabled_ind = 'Y'
AND results_opening.opening_can_be_updated(v_status) THEN
 -----------------------------------
 --UPDATING
 -----------------------------------
 IF UPDATING THEN
   v_business_identifier := 'Opening Id: '||TO_CHAR(:NEW.opening_id);


IF NVL(:NEW.FOREST_FILE_ID,0) != NVL(:OLD.FOREST_FILE_ID,0) THEN
  v_column_name := 'FOREST_FILE_ID';
  v_old_value := :OLD.FOREST_FILE_ID;
  v_new_value := :NEW.FOREST_FILE_ID;
  RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                   , p_column_name => v_column_name
                                   , p_business_identifier => v_business_identifier
                                   , p_old_value => v_old_value
                                   , p_new_value => v_new_value);
END IF;
IF NVL(:NEW.TIMBER_MARK,0) != NVL(:OLD.TIMBER_MARK,0) THEN
  v_column_name := 'TIMBER_MARK';
  v_old_value := :OLD.TIMBER_MARK;
  v_new_value := :NEW.TIMBER_MARK;
  RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                   , p_column_name => v_column_name
                                   , p_business_identifier => v_business_identifier
                                   , p_old_value => v_old_value
                                   , p_new_value => v_new_value);
END IF;
IF NVL(:NEW.CUT_BLOCK_ID,0) != NVL(:OLD.CUT_BLOCK_ID,0) THEN
  v_column_name := 'CUT_BLOCK_ID';
  v_old_value := :OLD.CUT_BLOCK_ID;
  v_new_value := :NEW.CUT_BLOCK_ID;
  RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                   , p_column_name => v_column_name
                                   , p_business_identifier => v_business_identifier
                                   , p_old_value => v_old_value
                                   , p_new_value => v_new_value);
END IF;
IF NVL(:NEW.CUTTING_PERMIT_ID,0) != NVL(:OLD.CUTTING_PERMIT_ID,0) THEN
  v_column_name := 'CUTTING_PERMIT_ID';
  v_old_value := :OLD.CUTTING_PERMIT_ID;
  v_new_value := :NEW.CUTTING_PERMIT_ID;
  RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                   , p_column_name => v_column_name
                                   , p_business_identifier => v_business_identifier
                                   , p_old_value => v_old_value
                                   , p_new_value => v_new_value);
END IF;
IF NVL(:NEW.DISTURBANCE_GROSS_AREA,0) != NVL(:OLD.DISTURBANCE_GROSS_AREA,0) THEN
  v_column_name := 'DISTURBANCE_GROSS_AREA';
  v_old_value := :OLD.DISTURBANCE_GROSS_AREA;
  v_new_value := :NEW.DISTURBANCE_GROSS_AREA;
  RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                   , p_column_name => v_column_name
                                   , p_business_identifier => v_business_identifier
                                   , p_old_value => v_old_value
                                   , p_new_value => v_new_value);
END IF;
IF NVL(PKG_SIL_DATE_CONVERSION.CONVERT_TO_CHAR(:NEW.DISTURBANCE_START_DATE,NULL,'Y'),0)
!= NVL(PKG_SIL_DATE_CONVERSION.CONVERT_TO_CHAR(:OLD.DISTURBANCE_START_DATE,NULL,'Y'),0) THEN
  v_column_name := 'DISTURBANCE_START_DATE';
  v_old_value := sil_date_conversion.convert_to_char_trunc_time(:OLD.DISTURBANCE_START_DATE);
  v_new_value := sil_date_conversion.convert_to_char_trunc_time(:NEW.DISTURBANCE_START_DATE);
  RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                   , p_column_name => v_column_name
                                   , p_business_identifier => v_business_identifier
                                   , p_old_value => v_old_value
                                   , p_new_value => v_new_value);
END IF;
IF NVL(PKG_SIL_DATE_CONVERSION.CONVERT_TO_CHAR(:NEW.DISTURBANCE_END_DATE,NULL,'Y'),0)
 != NVL(PKG_SIL_DATE_CONVERSION.CONVERT_TO_CHAR(:OLD.DISTURBANCE_END_DATE,NULL,'Y'),0) THEN
  v_column_name := 'DISTURBANCE_END_DATE';
  v_old_value := sil_date_conversion.convert_to_char_trunc_time(:OLD.DISTURBANCE_END_DATE);
  v_new_value := sil_date_conversion.convert_to_char_trunc_time(:NEW.DISTURBANCE_END_DATE);
  RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                   , p_column_name => v_column_name
                                   , p_business_identifier => v_business_identifier
                                   , p_old_value => v_old_value
                                   , p_new_value => v_new_value);
END IF;
IF NVL(:NEW.OPENING_ID,0) != NVL(:OLD.OPENING_ID,0) THEN
  v_column_name := 'OPENING_ID';
  v_old_value := :OLD.OPENING_ID;
  v_new_value := :NEW.OPENING_ID;
  RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                   , p_column_name => v_column_name
                                   , p_business_identifier => v_business_identifier
                                   , p_old_value => v_old_value
                                   , p_new_value => v_new_value);
END IF;
IF NVL(:NEW.OPENING_GROSS_AREA,0) != NVL(:OLD.OPENING_GROSS_AREA,0) THEN
  v_column_name := 'OPENING_GROSS_AREA';
  v_old_value := :OLD.OPENING_GROSS_AREA;
  v_new_value := :NEW.OPENING_GROSS_AREA;
  RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                   , p_column_name => v_column_name
                                   , p_business_identifier => v_business_identifier
                                   , p_old_value => v_old_value
                                   , p_new_value => v_new_value);
END IF;
IF NVL(PKG_SIL_DATE_CONVERSION.CONVERT_TO_CHAR(:NEW.PLANNED_HARVEST_DATE,NULL,'Y'),0)
!= NVL(PKG_SIL_DATE_CONVERSION.CONVERT_TO_CHAR(:OLD.PLANNED_HARVEST_DATE,NULL,'Y'),0) THEN
  v_column_name := 'PLANNED_HARVEST_DATE';
  v_old_value := sil_date_conversion.convert_to_char_trunc_time(:OLD.PLANNED_HARVEST_DATE);
  v_new_value := sil_date_conversion.convert_to_char_trunc_time(:NEW.PLANNED_HARVEST_DATE);
  RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                   , p_column_name => v_column_name
                                   , p_business_identifier => v_business_identifier
                                   , p_old_value => v_old_value
                                   , p_new_value => v_new_value);
END IF;
IF NVL(:NEW.PLANNED_GROSS_BLOCK_AREA,0) != NVL(:OLD.PLANNED_GROSS_BLOCK_AREA,0) THEN
  v_column_name := 'PLANNED_GROSS_BLOCK_AREA';
  v_old_value := :OLD.PLANNED_GROSS_BLOCK_AREA;
  v_new_value := :NEW.PLANNED_GROSS_BLOCK_AREA;
  RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                   , p_column_name => v_column_name
                                   , p_business_identifier => v_business_identifier
                                   , p_old_value => v_old_value
                                   , p_new_value => v_new_value);
END IF;
IF NVL(:NEW.PLANNED_NET_BLOCK_AREA,0) != NVL(:OLD.PLANNED_NET_BLOCK_AREA,0) THEN
  v_column_name := 'PLANNED_NET_BLOCK_AREA';
  v_old_value := :OLD.PLANNED_NET_BLOCK_AREA;
  v_new_value := :NEW.PLANNED_NET_BLOCK_AREA;
  RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                   , p_column_name => v_column_name
                                   , p_business_identifier => v_business_identifier
                                   , p_old_value => v_old_value
                                   , p_new_value => v_new_value);
END IF;
IF NVL(:NEW.OPENING_PRIME_LICENCE_IND,0) != NVL(:OLD.OPENING_PRIME_LICENCE_IND,0) THEN
  v_column_name := 'OPENING_PRIME_LICENCE_IND';
  v_old_value := :OLD.OPENING_PRIME_LICENCE_IND;
  v_new_value := :NEW.OPENING_PRIME_LICENCE_IND;
  RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                   , p_column_name => v_column_name
                                   , p_business_identifier => v_business_identifier
                                   , p_old_value => v_old_value
                                   , p_new_value => v_new_value);
END IF;
 -----------------------------------
 --DELETING
 -----------------------------------
ELSIF DELETING THEN
  v_business_identifier := 'Opening Id: '||TO_CHAR(:OLD.opening_id);


  v_column_name := 'FOREST_FILE_ID';
  v_old_value := :OLD.FOREST_FILE_ID;
  v_new_value := NULL;
 IF v_old_value IS NOT NULL THEN
   RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                     , p_column_name => v_column_name
                                     , p_business_identifier => v_business_identifier
                                     , p_old_value => v_old_value
                                     , p_new_value => v_new_value);
   END IF;
  v_column_name := 'TIMBER_MARK';
  v_old_value := :OLD.TIMBER_MARK;
  v_new_value := NULL;
 IF v_old_value IS NOT NULL THEN
   RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                     , p_column_name => v_column_name
                                     , p_business_identifier => v_business_identifier
                                     , p_old_value => v_old_value
                                     , p_new_value => v_new_value);
   END IF;
  v_column_name := 'CUT_BLOCK_ID';
  v_old_value := :OLD.CUT_BLOCK_ID;
  v_new_value := NULL;
 IF v_old_value IS NOT NULL THEN
   RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                     , p_column_name => v_column_name
                                     , p_business_identifier => v_business_identifier
                                     , p_old_value => v_old_value
                                     , p_new_value => v_new_value);
   END IF;
  v_column_name := 'CUTTING_PERMIT_ID';
  v_old_value := :OLD.CUTTING_PERMIT_ID;
  v_new_value := NULL;
 IF v_old_value IS NOT NULL THEN
   RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                     , p_column_name => v_column_name
                                     , p_business_identifier => v_business_identifier
                                     , p_old_value => v_old_value
                                     , p_new_value => v_new_value);
   END IF;
  v_column_name := 'DISTURBANCE_GROSS_AREA';
  v_old_value := :OLD.DISTURBANCE_GROSS_AREA;
  v_new_value := NULL;
 IF v_old_value IS NOT NULL THEN
   RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                     , p_column_name => v_column_name
                                     , p_business_identifier => v_business_identifier
                                     , p_old_value => v_old_value
                                     , p_new_value => v_new_value);
   END IF;
  v_column_name := 'DISTURBANCE_START_DATE';
  v_old_value := sil_date_conversion.convert_to_char_trunc_time(:OLD.DISTURBANCE_START_DATE);
  v_new_value := NULL;
 IF v_old_value IS NOT NULL THEN
   RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                     , p_column_name => v_column_name
                                     , p_business_identifier => v_business_identifier
                                     , p_old_value => v_old_value
                                     , p_new_value => v_new_value);
   END IF;
  v_column_name := 'DISTURBANCE_END_DATE';
  v_old_value := sil_date_conversion.convert_to_char_trunc_time(:OLD.DISTURBANCE_END_DATE);
  v_new_value := NULL;
 IF v_old_value IS NOT NULL THEN
   RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                     , p_column_name => v_column_name
                                     , p_business_identifier => v_business_identifier
                                     , p_old_value => v_old_value
                                     , p_new_value => v_new_value);
   END IF;
  v_column_name := 'OPENING_ID';
  v_old_value := :OLD.OPENING_ID;
  v_new_value := NULL;
 IF v_old_value IS NOT NULL THEN
   RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                     , p_column_name => v_column_name
                                     , p_business_identifier => v_business_identifier
                                     , p_old_value => v_old_value
                                     , p_new_value => v_new_value);
   END IF;
  v_column_name := 'OPENING_GROSS_AREA';
  v_old_value := :OLD.OPENING_GROSS_AREA;
  v_new_value := NULL;
 IF v_old_value IS NOT NULL THEN
   RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                     , p_column_name => v_column_name
                                     , p_business_identifier => v_business_identifier
                                     , p_old_value => v_old_value
                                     , p_new_value => v_new_value);
   END IF;
  v_column_name := 'PLANNED_HARVEST_DATE';
  v_old_value := sil_date_conversion.convert_to_char_trunc_time(:OLD.PLANNED_HARVEST_DATE);
  v_new_value := NULL;
 IF v_old_value IS NOT NULL THEN
   RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                     , p_column_name => v_column_name
                                     , p_business_identifier => v_business_identifier
                                     , p_old_value => v_old_value
                                     , p_new_value => v_new_value);
   END IF;
  v_column_name := 'PLANNED_GROSS_BLOCK_AREA';
  v_old_value := :OLD.PLANNED_GROSS_BLOCK_AREA;
  v_new_value := NULL;
 IF v_old_value IS NOT NULL THEN
   RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                     , p_column_name => v_column_name
                                     , p_business_identifier => v_business_identifier
                                     , p_old_value => v_old_value
                                     , p_new_value => v_new_value);
   END IF;
  v_column_name := 'PLANNED_NET_BLOCK_AREA';
  v_old_value := :OLD.PLANNED_NET_BLOCK_AREA;
  v_new_value := NULL;
 IF v_old_value IS NOT NULL THEN
   RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                     , p_column_name => v_column_name
                                     , p_business_identifier => v_business_identifier
                                     , p_old_value => v_old_value
                                     , p_new_value => v_new_value);
   END IF;
  v_column_name := 'OPENING_PRIME_LICENCE_IND';
  v_old_value := :OLD.OPENING_PRIME_LICENCE_IND;
  v_new_value := NULL;
 IF v_old_value IS NOT NULL THEN
   RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                     , p_column_name => v_column_name
                                     , p_business_identifier => v_business_identifier
                                     , p_old_value => v_old_value
                                     , p_new_value => v_new_value);
   END IF;
  v_column_name := 'ENTRY_USERID';
  v_old_value := :OLD.ENTRY_USERID;
  v_new_value := NULL;
 IF v_old_value IS NOT NULL THEN
   RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                     , p_column_name => v_column_name
                                     , p_business_identifier => v_business_identifier
                                     , p_old_value => v_old_value
                                     , p_new_value => v_new_value);
   END IF;
  v_column_name := 'ENTRY_TIMESTAMP';
  v_old_value := sil_date_conversion.convert_to_char_trunc_time(:OLD.ENTRY_TIMESTAMP);
  v_new_value := NULL;
 IF v_old_value IS NOT NULL THEN
   RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                     , p_column_name => v_column_name
                                     , p_business_identifier => v_business_identifier
                                     , p_old_value => v_old_value
                                     , p_new_value => v_new_value);
   END IF;
  v_column_name := 'UPDATE_USERID';
  v_old_value := :OLD.UPDATE_USERID;
  v_new_value := NULL;
 IF v_old_value IS NOT NULL THEN
   RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                     , p_column_name => v_column_name
                                     , p_business_identifier => v_business_identifier
                                     , p_old_value => v_old_value
                                     , p_new_value => v_new_value);
   END IF;
  v_column_name := 'UPDATE_TIMESTAMP';
  v_old_value := sil_date_conversion.convert_to_char_trunc_time(:OLD.UPDATE_TIMESTAMP);
  v_new_value := NULL;
 IF v_old_value IS NOT NULL THEN
   RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                     , p_column_name => v_column_name
                                     , p_business_identifier => v_business_identifier
                                     , p_old_value => v_old_value
                                     , p_new_value => v_new_value);
   END IF;



 -----------------------------------
 --INSERTING
 -----------------------------------
ELSIF INSERTING THEN
  v_business_identifier := 'Opening Id: '||TO_CHAR(:NEW.opening_id);


  v_column_name := 'FOREST_FILE_ID';
  v_new_value := :NEW.FOREST_FILE_ID;
  v_old_value := NULL;
 IF v_new_value IS NOT NULL THEN
   RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                     , p_column_name => v_column_name
                                     , p_business_identifier => v_business_identifier
                                     , p_old_value => v_old_value
                                     , p_new_value => v_new_value);
   END IF;
  v_column_name := 'TIMBER_MARK';
  v_new_value := :NEW.TIMBER_MARK;
  v_old_value := NULL;
 IF v_new_value IS NOT NULL THEN
   RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                     , p_column_name => v_column_name
                                     , p_business_identifier => v_business_identifier
                                     , p_old_value => v_old_value
                                     , p_new_value => v_new_value);
   END IF;
  v_column_name := 'CUT_BLOCK_ID';
  v_new_value := :NEW.CUT_BLOCK_ID;
  v_old_value := NULL;
 IF v_new_value IS NOT NULL THEN
   RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                     , p_column_name => v_column_name
                                     , p_business_identifier => v_business_identifier
                                     , p_old_value => v_old_value
                                     , p_new_value => v_new_value);
   END IF;
  v_column_name := 'CUTTING_PERMIT_ID';
  v_new_value := :NEW.CUTTING_PERMIT_ID;
  v_old_value := NULL;
 IF v_new_value IS NOT NULL THEN
   RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                     , p_column_name => v_column_name
                                     , p_business_identifier => v_business_identifier
                                     , p_old_value => v_old_value
                                     , p_new_value => v_new_value);
   END IF;
  v_column_name := 'DISTURBANCE_GROSS_AREA';
  v_new_value := :NEW.DISTURBANCE_GROSS_AREA;
  v_old_value := NULL;
 IF v_new_value IS NOT NULL THEN
   RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                     , p_column_name => v_column_name
                                     , p_business_identifier => v_business_identifier
                                     , p_old_value => v_old_value
                                     , p_new_value => v_new_value);
   END IF;
  v_column_name := 'DISTURBANCE_START_DATE';
  v_new_value := sil_date_conversion.convert_to_char_trunc_time(:NEW.DISTURBANCE_START_DATE);
  v_old_value := NULL;
 IF v_new_value IS NOT NULL THEN
   RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                     , p_column_name => v_column_name
                                     , p_business_identifier => v_business_identifier
                                     , p_old_value => v_old_value
                                     , p_new_value => v_new_value);
   END IF;
  v_column_name := 'DISTURBANCE_END_DATE';
  v_new_value := sil_date_conversion.convert_to_char_trunc_time(:NEW.DISTURBANCE_END_DATE);
  v_old_value := NULL;
 IF v_new_value IS NOT NULL THEN
   RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                     , p_column_name => v_column_name
                                     , p_business_identifier => v_business_identifier
                                     , p_old_value => v_old_value
                                     , p_new_value => v_new_value);
   END IF;
  v_column_name := 'OPENING_ID';
  v_new_value := :NEW.OPENING_ID;
  v_old_value := NULL;
 IF v_new_value IS NOT NULL THEN
   RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                     , p_column_name => v_column_name
                                     , p_business_identifier => v_business_identifier
                                     , p_old_value => v_old_value
                                     , p_new_value => v_new_value);
   END IF;
  v_column_name := 'OPENING_GROSS_AREA';
  v_new_value := :NEW.OPENING_GROSS_AREA;
  v_old_value := NULL;
 IF v_new_value IS NOT NULL THEN
   RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                     , p_column_name => v_column_name
                                     , p_business_identifier => v_business_identifier
                                     , p_old_value => v_old_value
                                     , p_new_value => v_new_value);
   END IF;
  v_column_name := 'PLANNED_HARVEST_DATE';
  v_new_value := sil_date_conversion.convert_to_char_trunc_time(:NEW.PLANNED_HARVEST_DATE);
  v_old_value := NULL;
 IF v_new_value IS NOT NULL THEN
   RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                     , p_column_name => v_column_name
                                     , p_business_identifier => v_business_identifier
                                     , p_old_value => v_old_value
                                     , p_new_value => v_new_value);
   END IF;
  v_column_name := 'PLANNED_GROSS_BLOCK_AREA';
  v_new_value := :NEW.PLANNED_GROSS_BLOCK_AREA;
  v_old_value := NULL;
 IF v_new_value IS NOT NULL THEN
   RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                     , p_column_name => v_column_name
                                     , p_business_identifier => v_business_identifier
                                     , p_old_value => v_old_value
                                     , p_new_value => v_new_value);
   END IF;
  v_column_name := 'PLANNED_NET_BLOCK_AREA';
  v_new_value := :NEW.PLANNED_NET_BLOCK_AREA;
  v_old_value := NULL;
 IF v_new_value IS NOT NULL THEN
   RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                     , p_column_name => v_column_name
                                     , p_business_identifier => v_business_identifier
                                     , p_old_value => v_old_value
                                     , p_new_value => v_new_value);
   END IF;
  v_column_name := 'OPENING_PRIME_LICENCE_IND';
  v_new_value := :NEW.OPENING_PRIME_LICENCE_IND;
  v_old_value := NULL;
 IF v_new_value IS NOT NULL THEN
   RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                     , p_column_name => v_column_name
                                     , p_business_identifier => v_business_identifier
                                     , p_old_value => v_old_value
                                     , p_new_value => v_new_value);
  END IF;
  END IF;

  END IF;

END RESULTS_CBOA_AR_IUD_TRG;




/
ALTER TRIGGER "THE"."RESULTS_CBOA_AR_IUD_TRG" ENABLE;

  CREATE OR REPLACE EDITIONABLE TRIGGER "THE"."FTA_CPFILL_CBOA"
  BEFORE INSERT OR UPDATE
  ON cut_block_open_admin
  FOR EACH ROW
BEGIN
  IF :NEW.forest_file_id IS NOT NULL
     AND :NEW.cut_block_id IS NOT NULL
     AND :NEW.cutting_permit_id IS NULL THEN
    :NEW.cutting_permit_id := ' ';
  END IF;
END fta_cpfill_cboa;



/
ALTER TRIGGER "THE"."FTA_CPFILL_CBOA" ENABLE;


  CREATE OR REPLACE EDITIONABLE TRIGGER "THE"."RESULTS_JUNK_TRIG"
/******************************************************************************
   Trigger: RESULTS_CBOA_AR_IUD_TRG
   Purpose: This trigger will call the  RESULTS_AUDIT.INSERT_AUDIT_DETAIL
            procedure when the status of the opening or standards regime is
            either  'APP' or 'AMD'. The procedure inserts data into the
            RESULTS_AUDIT_DETAIL table based on the data that was passed in.
   Revision History
   Person               Date       Comments
   -----------------    ---------  --------------------------------
   wcound (Pangaea)     2003-03-05 Created
   wcound               2003-03-27 Added NVL to Update to capture null values
   R.A.Robb             2004-05-14 CR#46 - updated auditing
******************************************************************************/
AFTER INSERT OR UPDATE OR DELETE ON CUT_BLOCK_OPEN_ADMIN FOR EACH ROW
DECLARE
 v_opening_id             RESULTS_AUDIT_EVENT.OPENING_ID%TYPE;
 v_results_audit_event_id RESULTS_AUDIT_EVENT.RESULTS_AUDIT_EVENT_ID%type;
 v_update_userid          RESULTS_AUDIT_DETAIL.ENTRY_USERID%TYPE;
 v_status                 VARCHAR2(3);
 v_table_name             RESULTS_AUDIT_DETAIL.TABLE_NAME%TYPE;
 v_column_name            RESULTS_AUDIT_DETAIL.COLUMN_NAME%TYPE;
 v_old_value              RESULTS_AUDIT_DETAIL.OLD_VALUE%TYPE;
 v_new_value              RESULTS_AUDIT_DETAIL.NEW_VALUE%TYPE;
 v_business_identifier    RESULTS_AUDIT_DETAIL.BUSINESS_IDENTIFIER%TYPE;
 v_audit_enabled_ind      VARCHAR2(1);

 BEGIN
 v_table_name := 'CUT_BLOCK_OPEN_ADMIN';
 v_opening_id := RESULTS_GLOBALS.GET_OPENING_ID;
 v_status := RESULTS_GLOBALS.GET_RESULTS_AUDIT_STATUS;
 v_audit_enabled_ind := RESULTS_GLOBALS.GET_AUDITING_ENABLED_IND;

IF v_audit_enabled_ind = 'Y'
AND results_opening.opening_can_be_updated(v_status) THEN
 -----------------------------------
 --UPDATING
 -----------------------------------
 IF UPDATING THEN
   v_business_identifier := 'Opening Id: '||TO_CHAR(:NEW.opening_id);


IF NVL(:NEW.FOREST_FILE_ID,0) != NVL(:OLD.FOREST_FILE_ID,0) THEN
  v_column_name := 'FOREST_FILE_ID';
  v_old_value := :OLD.FOREST_FILE_ID;
  v_new_value := :NEW.FOREST_FILE_ID;
  RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                   , p_column_name => v_column_name
                                   , p_business_identifier => v_business_identifier
                                   , p_old_value => v_old_value
                                   , p_new_value => v_new_value);
END IF;
IF NVL(:NEW.TIMBER_MARK,0) != NVL(:OLD.TIMBER_MARK,0) THEN
  v_column_name := 'TIMBER_MARK';
  v_old_value := :OLD.TIMBER_MARK;
  v_new_value := :NEW.TIMBER_MARK;
  RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                   , p_column_name => v_column_name
                                   , p_business_identifier => v_business_identifier
                                   , p_old_value => v_old_value
                                   , p_new_value => v_new_value);
END IF;
IF NVL(:NEW.CUT_BLOCK_ID,0) != NVL(:OLD.CUT_BLOCK_ID,0) THEN
  v_column_name := 'CUT_BLOCK_ID';
  v_old_value := :OLD.CUT_BLOCK_ID;
  v_new_value := :NEW.CUT_BLOCK_ID;
  RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                   , p_column_name => v_column_name
                                   , p_business_identifier => v_business_identifier
                                   , p_old_value => v_old_value
                                   , p_new_value => v_new_value);
END IF;
IF NVL(:NEW.CUTTING_PERMIT_ID,0) != NVL(:OLD.CUTTING_PERMIT_ID,0) THEN
  v_column_name := 'CUTTING_PERMIT_ID';
  v_old_value := :OLD.CUTTING_PERMIT_ID;
  v_new_value := :NEW.CUTTING_PERMIT_ID;
  RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                   , p_column_name => v_column_name
                                   , p_business_identifier => v_business_identifier
                                   , p_old_value => v_old_value
                                   , p_new_value => v_new_value);
END IF;
IF NVL(:NEW.DISTURBANCE_GROSS_AREA,0) != NVL(:OLD.DISTURBANCE_GROSS_AREA,0) THEN
  v_column_name := 'DISTURBANCE_GROSS_AREA';
  v_old_value := :OLD.DISTURBANCE_GROSS_AREA;
  v_new_value := :NEW.DISTURBANCE_GROSS_AREA;
  RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                   , p_column_name => v_column_name
                                   , p_business_identifier => v_business_identifier
                                   , p_old_value => v_old_value
                                   , p_new_value => v_new_value);
END IF;
IF NVL(PKG_SIL_DATE_CONVERSION.CONVERT_TO_CHAR(:NEW.DISTURBANCE_START_DATE,NULL,'Y'),0)
!= NVL(PKG_SIL_DATE_CONVERSION.CONVERT_TO_CHAR(:OLD.DISTURBANCE_START_DATE,NULL,'Y'),0) THEN
  v_column_name := 'DISTURBANCE_START_DATE';
  v_old_value := sil_date_conversion.convert_to_char_trunc_time(:OLD.DISTURBANCE_START_DATE);
  v_new_value := sil_date_conversion.convert_to_char_trunc_time(:NEW.DISTURBANCE_START_DATE);
  RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                   , p_column_name => v_column_name
                                   , p_business_identifier => v_business_identifier
                                   , p_old_value => v_old_value
                                   , p_new_value => v_new_value);
END IF;
IF NVL(PKG_SIL_DATE_CONVERSION.CONVERT_TO_CHAR(:NEW.DISTURBANCE_END_DATE,NULL,'Y'),0)
 != NVL(PKG_SIL_DATE_CONVERSION.CONVERT_TO_CHAR(:OLD.DISTURBANCE_END_DATE,NULL,'Y'),0) THEN
  v_column_name := 'DISTURBANCE_END_DATE';
  v_old_value := sil_date_conversion.convert_to_char_trunc_time(:OLD.DISTURBANCE_END_DATE);
  v_new_value := sil_date_conversion.convert_to_char_trunc_time(:NEW.DISTURBANCE_END_DATE);
  RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                   , p_column_name => v_column_name
                                   , p_business_identifier => v_business_identifier
                                   , p_old_value => v_old_value
                                   , p_new_value => v_new_value);
END IF;
IF NVL(:NEW.OPENING_ID,0) != NVL(:OLD.OPENING_ID,0) THEN
  v_column_name := 'OPENING_ID';
  v_old_value := :OLD.OPENING_ID;
  v_new_value := :NEW.OPENING_ID;
  RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                   , p_column_name => v_column_name
                                   , p_business_identifier => v_business_identifier
                                   , p_old_value => v_old_value
                                   , p_new_value => v_new_value);
END IF;
IF NVL(:NEW.OPENING_GROSS_AREA,0) != NVL(:OLD.OPENING_GROSS_AREA,0) THEN
  v_column_name := 'OPENING_GROSS_AREA';
  v_old_value := :OLD.OPENING_GROSS_AREA;
  v_new_value := :NEW.OPENING_GROSS_AREA;
  RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                   , p_column_name => v_column_name
                                   , p_business_identifier => v_business_identifier
                                   , p_old_value => v_old_value
                                   , p_new_value => v_new_value);
END IF;
IF NVL(PKG_SIL_DATE_CONVERSION.CONVERT_TO_CHAR(:NEW.PLANNED_HARVEST_DATE,NULL,'Y'),0)
!= NVL(PKG_SIL_DATE_CONVERSION.CONVERT_TO_CHAR(:OLD.PLANNED_HARVEST_DATE,NULL,'Y'),0) THEN
  v_column_name := 'PLANNED_HARVEST_DATE';
  v_old_value := sil_date_conversion.convert_to_char_trunc_time(:OLD.PLANNED_HARVEST_DATE);
  v_new_value := sil_date_conversion.convert_to_char_trunc_time(:NEW.PLANNED_HARVEST_DATE);
  RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                   , p_column_name => v_column_name
                                   , p_business_identifier => v_business_identifier
                                   , p_old_value => v_old_value
                                   , p_new_value => v_new_value);
END IF;
IF NVL(:NEW.PLANNED_GROSS_BLOCK_AREA,0) != NVL(:OLD.PLANNED_GROSS_BLOCK_AREA,0) THEN
  v_column_name := 'PLANNED_GROSS_BLOCK_AREA';
  v_old_value := :OLD.PLANNED_GROSS_BLOCK_AREA;
  v_new_value := :NEW.PLANNED_GROSS_BLOCK_AREA;
  RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                   , p_column_name => v_column_name
                                   , p_business_identifier => v_business_identifier
                                   , p_old_value => v_old_value
                                   , p_new_value => v_new_value);
END IF;
IF NVL(:NEW.PLANNED_NET_BLOCK_AREA,0) != NVL(:OLD.PLANNED_NET_BLOCK_AREA,0) THEN
  v_column_name := 'PLANNED_NET_BLOCK_AREA';
  v_old_value := :OLD.PLANNED_NET_BLOCK_AREA;
  v_new_value := :NEW.PLANNED_NET_BLOCK_AREA;
  RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                   , p_column_name => v_column_name
                                   , p_business_identifier => v_business_identifier
                                   , p_old_value => v_old_value
                                   , p_new_value => v_new_value);
END IF;
IF NVL(:NEW.OPENING_PRIME_LICENCE_IND,0) != NVL(:OLD.OPENING_PRIME_LICENCE_IND,0) THEN
  v_column_name := 'OPENING_PRIME_LICENCE_IND';
  v_old_value := :OLD.OPENING_PRIME_LICENCE_IND;
  v_new_value := :NEW.OPENING_PRIME_LICENCE_IND;
  RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                   , p_column_name => v_column_name
                                   , p_business_identifier => v_business_identifier
                                   , p_old_value => v_old_value
                                   , p_new_value => v_new_value);
END IF;
 -----------------------------------
 --DELETING
 -----------------------------------
ELSIF DELETING THEN
  v_business_identifier := 'Opening Id: '||TO_CHAR(:OLD.opening_id);


  v_column_name := 'FOREST_FILE_ID';
  v_old_value := :OLD.FOREST_FILE_ID;
  v_new_value := NULL;
 IF v_old_value IS NOT NULL THEN
   RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                     , p_column_name => v_column_name
                                     , p_business_identifier => v_business_identifier
                                     , p_old_value => v_old_value
                                     , p_new_value => v_new_value);
   END IF;
  v_column_name := 'TIMBER_MARK';
  v_old_value := :OLD.TIMBER_MARK;
  v_new_value := NULL;
 IF v_old_value IS NOT NULL THEN
   RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                     , p_column_name => v_column_name
                                     , p_business_identifier => v_business_identifier
                                     , p_old_value => v_old_value
                                     , p_new_value => v_new_value);
   END IF;
  v_column_name := 'CUT_BLOCK_ID';
  v_old_value := :OLD.CUT_BLOCK_ID;
  v_new_value := NULL;
 IF v_old_value IS NOT NULL THEN
   RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                     , p_column_name => v_column_name
                                     , p_business_identifier => v_business_identifier
                                     , p_old_value => v_old_value
                                     , p_new_value => v_new_value);
   END IF;
  v_column_name := 'CUTTING_PERMIT_ID';
  v_old_value := :OLD.CUTTING_PERMIT_ID;
  v_new_value := NULL;
 IF v_old_value IS NOT NULL THEN
   RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                     , p_column_name => v_column_name
                                     , p_business_identifier => v_business_identifier
                                     , p_old_value => v_old_value
                                     , p_new_value => v_new_value);
   END IF;
  v_column_name := 'DISTURBANCE_GROSS_AREA';
  v_old_value := :OLD.DISTURBANCE_GROSS_AREA;
  v_new_value := NULL;
 IF v_old_value IS NOT NULL THEN
   RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                     , p_column_name => v_column_name
                                     , p_business_identifier => v_business_identifier
                                     , p_old_value => v_old_value
                                     , p_new_value => v_new_value);
   END IF;
  v_column_name := 'DISTURBANCE_START_DATE';
  v_old_value := sil_date_conversion.convert_to_char_trunc_time(:OLD.DISTURBANCE_START_DATE);
  v_new_value := NULL;
 IF v_old_value IS NOT NULL THEN
   RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                     , p_column_name => v_column_name
                                     , p_business_identifier => v_business_identifier
                                     , p_old_value => v_old_value
                                     , p_new_value => v_new_value);
   END IF;
  v_column_name := 'DISTURBANCE_END_DATE';
  v_old_value := sil_date_conversion.convert_to_char_trunc_time(:OLD.DISTURBANCE_END_DATE);
  v_new_value := NULL;
 IF v_old_value IS NOT NULL THEN
   RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                     , p_column_name => v_column_name
                                     , p_business_identifier => v_business_identifier
                                     , p_old_value => v_old_value
                                     , p_new_value => v_new_value);
   END IF;
  v_column_name := 'OPENING_ID';
  v_old_value := :OLD.OPENING_ID;
  v_new_value := NULL;
 IF v_old_value IS NOT NULL THEN
   RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                     , p_column_name => v_column_name
                                     , p_business_identifier => v_business_identifier
                                     , p_old_value => v_old_value
                                     , p_new_value => v_new_value);
   END IF;
  v_column_name := 'OPENING_GROSS_AREA';
  v_old_value := :OLD.OPENING_GROSS_AREA;
  v_new_value := NULL;
 IF v_old_value IS NOT NULL THEN
   RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                     , p_column_name => v_column_name
                                     , p_business_identifier => v_business_identifier
                                     , p_old_value => v_old_value
                                     , p_new_value => v_new_value);
   END IF;
  v_column_name := 'PLANNED_HARVEST_DATE';
  v_old_value := sil_date_conversion.convert_to_char_trunc_time(:OLD.PLANNED_HARVEST_DATE);
  v_new_value := NULL;
 IF v_old_value IS NOT NULL THEN
   RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                     , p_column_name => v_column_name
                                     , p_business_identifier => v_business_identifier
                                     , p_old_value => v_old_value
                                     , p_new_value => v_new_value);
   END IF;
  v_column_name := 'PLANNED_GROSS_BLOCK_AREA';
  v_old_value := :OLD.PLANNED_GROSS_BLOCK_AREA;
  v_new_value := NULL;
 IF v_old_value IS NOT NULL THEN
   RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                     , p_column_name => v_column_name
                                     , p_business_identifier => v_business_identifier
                                     , p_old_value => v_old_value
                                     , p_new_value => v_new_value);
   END IF;
  v_column_name := 'PLANNED_NET_BLOCK_AREA';
  v_old_value := :OLD.PLANNED_NET_BLOCK_AREA;
  v_new_value := NULL;
 IF v_old_value IS NOT NULL THEN
   RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                     , p_column_name => v_column_name
                                     , p_business_identifier => v_business_identifier
                                     , p_old_value => v_old_value
                                     , p_new_value => v_new_value);
   END IF;
  v_column_name := 'OPENING_PRIME_LICENCE_IND';
  v_old_value := :OLD.OPENING_PRIME_LICENCE_IND;
  v_new_value := NULL;
 IF v_old_value IS NOT NULL THEN
   RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                     , p_column_name => v_column_name
                                     , p_business_identifier => v_business_identifier
                                     , p_old_value => v_old_value
                                     , p_new_value => v_new_value);
   END IF;
  v_column_name := 'ENTRY_USERID';
  v_old_value := :OLD.ENTRY_USERID;
  v_new_value := NULL;
 IF v_old_value IS NOT NULL THEN
   RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                     , p_column_name => v_column_name
                                     , p_business_identifier => v_business_identifier
                                     , p_old_value => v_old_value
                                     , p_new_value => v_new_value);
   END IF;
  v_column_name := 'ENTRY_TIMESTAMP';
  v_old_value := sil_date_conversion.convert_to_char_trunc_time(:OLD.ENTRY_TIMESTAMP);
  v_new_value := NULL;
 IF v_old_value IS NOT NULL THEN
   RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                     , p_column_name => v_column_name
                                     , p_business_identifier => v_business_identifier
                                     , p_old_value => v_old_value
                                     , p_new_value => v_new_value);
   END IF;
  v_column_name := 'UPDATE_USERID';
  v_old_value := :OLD.UPDATE_USERID;
  v_new_value := NULL;
 IF v_old_value IS NOT NULL THEN
   RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                     , p_column_name => v_column_name
                                     , p_business_identifier => v_business_identifier
                                     , p_old_value => v_old_value
                                     , p_new_value => v_new_value);
   END IF;
  v_column_name := 'UPDATE_TIMESTAMP';
  v_old_value := sil_date_conversion.convert_to_char_trunc_time(:OLD.UPDATE_TIMESTAMP);
  v_new_value := NULL;
 IF v_old_value IS NOT NULL THEN
   RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                     , p_column_name => v_column_name
                                     , p_business_identifier => v_business_identifier
                                     , p_old_value => v_old_value
                                     , p_new_value => v_new_value);
   END IF;



 -----------------------------------
 --INSERTING
 -----------------------------------
ELSIF INSERTING THEN
  v_business_identifier := 'Opening Id: '||TO_CHAR(:NEW.opening_id);


  v_column_name := 'FOREST_FILE_ID';
  v_new_value := :NEW.FOREST_FILE_ID;
  v_old_value := NULL;
 IF v_new_value IS NOT NULL THEN
   RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                     , p_column_name => v_column_name
                                     , p_business_identifier => v_business_identifier
                                     , p_old_value => v_old_value
                                     , p_new_value => v_new_value);
   END IF;
  v_column_name := 'TIMBER_MARK';
  v_new_value := :NEW.TIMBER_MARK;
  v_old_value := NULL;
 IF v_new_value IS NOT NULL THEN
   RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                     , p_column_name => v_column_name
                                     , p_business_identifier => v_business_identifier
                                     , p_old_value => v_old_value
                                     , p_new_value => v_new_value);
   END IF;
  v_column_name := 'CUT_BLOCK_ID';
  v_new_value := :NEW.CUT_BLOCK_ID;
  v_old_value := NULL;
 IF v_new_value IS NOT NULL THEN
   RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                     , p_column_name => v_column_name
                                     , p_business_identifier => v_business_identifier
                                     , p_old_value => v_old_value
                                     , p_new_value => v_new_value);
   END IF;
  v_column_name := 'CUTTING_PERMIT_ID';
  v_new_value := :NEW.CUTTING_PERMIT_ID;
  v_old_value := NULL;
 IF v_new_value IS NOT NULL THEN
   RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                     , p_column_name => v_column_name
                                     , p_business_identifier => v_business_identifier
                                     , p_old_value => v_old_value
                                     , p_new_value => v_new_value);
   END IF;
  v_column_name := 'DISTURBANCE_GROSS_AREA';
  v_new_value := :NEW.DISTURBANCE_GROSS_AREA;
  v_old_value := NULL;
 IF v_new_value IS NOT NULL THEN
   RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                     , p_column_name => v_column_name
                                     , p_business_identifier => v_business_identifier
                                     , p_old_value => v_old_value
                                     , p_new_value => v_new_value);
   END IF;
  v_column_name := 'DISTURBANCE_START_DATE';
  v_new_value := sil_date_conversion.convert_to_char_trunc_time(:NEW.DISTURBANCE_START_DATE);
  v_old_value := NULL;
 IF v_new_value IS NOT NULL THEN
   RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                     , p_column_name => v_column_name
                                     , p_business_identifier => v_business_identifier
                                     , p_old_value => v_old_value
                                     , p_new_value => v_new_value);
   END IF;
  v_column_name := 'DISTURBANCE_END_DATE';
  v_new_value := sil_date_conversion.convert_to_char_trunc_time(:NEW.DISTURBANCE_END_DATE);
  v_old_value := NULL;
 IF v_new_value IS NOT NULL THEN
   RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                     , p_column_name => v_column_name
                                     , p_business_identifier => v_business_identifier
                                     , p_old_value => v_old_value
                                     , p_new_value => v_new_value);
   END IF;
  v_column_name := 'OPENING_ID';
  v_new_value := :NEW.OPENING_ID;
  v_old_value := NULL;
 IF v_new_value IS NOT NULL THEN
   RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                     , p_column_name => v_column_name
                                     , p_business_identifier => v_business_identifier
                                     , p_old_value => v_old_value
                                     , p_new_value => v_new_value);
   END IF;
  v_column_name := 'OPENING_GROSS_AREA';
  v_new_value := :NEW.OPENING_GROSS_AREA;
  v_old_value := NULL;
 IF v_new_value IS NOT NULL THEN
   RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                     , p_column_name => v_column_name
                                     , p_business_identifier => v_business_identifier
                                     , p_old_value => v_old_value
                                     , p_new_value => v_new_value);
   END IF;
  v_column_name := 'PLANNED_HARVEST_DATE';
  v_new_value := sil_date_conversion.convert_to_char_trunc_time(:NEW.PLANNED_HARVEST_DATE);
  v_old_value := NULL;
 IF v_new_value IS NOT NULL THEN
   RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                     , p_column_name => v_column_name
                                     , p_business_identifier => v_business_identifier
                                     , p_old_value => v_old_value
                                     , p_new_value => v_new_value);
   END IF;
  v_column_name := 'PLANNED_GROSS_BLOCK_AREA';
  v_new_value := :NEW.PLANNED_GROSS_BLOCK_AREA;
  v_old_value := NULL;
 IF v_new_value IS NOT NULL THEN
   RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                     , p_column_name => v_column_name
                                     , p_business_identifier => v_business_identifier
                                     , p_old_value => v_old_value
                                     , p_new_value => v_new_value);
   END IF;
  v_column_name := 'PLANNED_NET_BLOCK_AREA';
  v_new_value := :NEW.PLANNED_NET_BLOCK_AREA;
  v_old_value := NULL;
 IF v_new_value IS NOT NULL THEN
   RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                     , p_column_name => v_column_name
                                     , p_business_identifier => v_business_identifier
                                     , p_old_value => v_old_value
                                     , p_new_value => v_new_value);
   END IF;
  v_column_name := 'OPENING_PRIME_LICENCE_IND';
  v_new_value := :NEW.OPENING_PRIME_LICENCE_IND;
  v_old_value := NULL;
 IF v_new_value IS NOT NULL THEN
   RESULTS_AUDIT.INSERT_AUDIT_DETAIL( p_table_name => v_table_name
                                     , p_column_name => v_column_name
                                     , p_business_identifier => v_business_identifier
                                     , p_old_value => v_old_value
                                     , p_new_value => v_new_value);
  END IF;
  END IF;

  END IF;

END RESULTS_CBOA_AR_IUD_TRG;

