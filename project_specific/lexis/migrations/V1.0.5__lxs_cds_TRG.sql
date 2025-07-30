
  CREATE OR REPLACE EDITIONABLE TRIGGER "THE"."TB1$_MAX_CLIENT_NMBR" BEFORE INSERT OR UPDATE ON THE.MAX_CLIENT_NMBR
FOR EACH ROW
BEGIN
:NEW.DUMMY_ACCESS_KEY :=            NVL ( RTRIM ( :NEW.DUMMY_ACCESS_KEY ), ' ' ) ;
:NEW.CLIENT_NUMBER :=            NVL ( RTRIM ( :NEW.CLIENT_NUMBER ), ' ' ) ;
END ;




/
ALTER TRIGGER "THE"."TB1$_MAX_CLIENT_NMBR" ENABLE;

  CREATE OR REPLACE EDITIONABLE TRIGGER "THE"."CLIENT_FOR_CLIENT_AR_IUD_TRG" 
/******************************************************************************
   Trigger: CLIENT_FOR_CLIENT_AR_IUD_TRG
   Purpose: This trigger audits changes to the FOREST_CLIENT table
   Revision History
   Person               Date       Comments
   -----------------    ---------  --------------------------------
   R.A.Robb             2006-12-27 Created
   TMcClelland          2007-08-31 Added client_type_code to trigger insert
******************************************************************************/
AFTER INSERT OR UPDATE OR DELETE
  OF client_number
   , client_name
   , legal_first_name
   , legal_middle_name
   , client_status_code
   , client_type_code
   , birthdate
   , client_id_type_code
   , client_identification
   , registry_company_type_code
   , corp_regn_nmbr
   , client_acronym
   , wcb_firm_number
   , ocg_supplier_nmbr
   , client_comment
  ON forest_client
  FOR EACH ROW
DECLARE
  v_client_audit_code                for_cli_audit.client_audit_code%TYPE;
  v_client_update_action_code        client_update_action_code.client_update_action_code%TYPE;
  v_forest_client_audit_id           for_cli_audit.forest_client_audit_id%TYPE;
BEGIN
  IF INSERTING THEN
    v_client_audit_code := client_constants.c_audit_insert;
  ELSIF UPDATING THEN
    v_client_audit_code := client_constants.c_audit_update;
  ELSE
    v_client_audit_code := client_constants.c_audit_delete;
  END IF;

  IF    INSERTING
     OR UPDATING THEN
    --Put the new row into the audit table
    INSERT INTO for_cli_audit
           (forest_client_audit_id
          , client_audit_code
          , client_number
          , client_name
          , legal_first_name
          , legal_middle_name
          , client_status_code
          , client_type_code
          , birthdate
          , client_id_type_code
          , client_identification
          , registry_company_type_code
          , corp_regn_nmbr
          , client_acronym
          , wcb_firm_number
          , ocg_supplier_nmbr
          , client_comment
          , add_timestamp
          , add_userid
          , add_org_unit
          , update_timestamp
          , update_userid
          , update_org_unit)
    VALUES (forest_client_audit_seq.NEXTVAL
          , v_client_audit_code
          , :NEW.client_number
          , :NEW.client_name
          , :NEW.legal_first_name
          , :NEW.legal_middle_name
          , :NEW.client_status_code
          , :NEW.client_type_code
          , :NEW.birthdate
          , :NEW.client_id_type_code
          , :NEW.client_identification
          , :NEW.registry_company_type_code
          , :NEW.corp_regn_nmbr
          , :NEW.client_acronym
          , :NEW.wcb_firm_number
          , :NEW.ocg_supplier_nmbr
          , :NEW.client_comment
          , :NEW.add_timestamp
          , :NEW.add_userid
          , :NEW.add_org_unit
          , :NEW.update_timestamp
          , :NEW.update_userid
          , :NEW.update_org_unit)
       RETURNING forest_client_audit_id INTO v_forest_client_audit_id;
    --Process update reasons
    IF UPDATING THEN
      --Status Change
      v_client_update_action_code := NULL;
      v_client_update_action_code := client_client_update_reason.check_status
                                    (:OLD.client_status_code
                                     ,:NEW.client_status_code);
      IF v_client_update_action_code IS NOT NULL THEN
        client_client_update_reason.init;
        client_client_update_reason.set_forest_client_audit_id(v_forest_client_audit_id);
        client_client_update_reason.set_client_update_action_code(v_client_update_action_code);
        --get reason from client pkg
        client_client_update_reason.set_client_update_reason_code(client_forest_client.get_ur_reason_status);
        client_client_update_reason.set_client_type_code(:NEW.client_type_code);
        client_client_update_reason.set_add_timestamp(:NEW.update_timestamp);
        client_client_update_reason.set_add_userid(:NEW.update_userid);
        client_client_update_reason.set_update_timestamp(:NEW.update_timestamp);
        client_client_update_reason.set_update_userid(:NEW.update_userid);
        client_client_update_reason.validate;
        IF NOT client_client_update_reason.error_raised THEN
          client_client_update_reason.add;
        END IF;
        IF client_client_update_reason.error_raised THEN
          RAISE_APPLICATION_ERROR(-20400,'Error writing update reason (Status) in audit trigger.');
        END IF;
      END IF;

      --Name Change
      v_client_update_action_code := NULL;
      v_client_update_action_code := client_client_update_reason.check_client_name
                                    (:OLD.client_name
                                    ,:OLD.legal_first_name
                                    ,:OLD.legal_middle_name
                                    ,:NEW.client_name
                                    ,:NEW.legal_first_name
                                    ,:NEW.legal_middle_name);
      IF v_client_update_action_code IS NOT NULL THEN
        client_client_update_reason.init;
        client_client_update_reason.set_forest_client_audit_id(v_forest_client_audit_id);
        client_client_update_reason.set_client_update_action_code(v_client_update_action_code);
        --get reason from client pkg
        client_client_update_reason.set_client_update_reason_code(client_forest_client.get_ur_reason_name);
        client_client_update_reason.set_client_type_code(:NEW.client_type_code);
        client_client_update_reason.set_add_timestamp(:NEW.update_timestamp);
        client_client_update_reason.set_add_userid(:NEW.update_userid);
        client_client_update_reason.set_update_timestamp(:NEW.update_timestamp);
        client_client_update_reason.set_update_userid(:NEW.update_userid);
        client_client_update_reason.validate;
        IF NOT client_client_update_reason.error_raised THEN
          client_client_update_reason.add;
        END IF;
        IF client_client_update_reason.error_raised THEN
          RAISE_APPLICATION_ERROR(-20400,'Error writing update reason (Name) in audit trigger.');
        END IF;
      END IF;

      --ID Change
      v_client_update_action_code := NULL;
      v_client_update_action_code := client_client_update_reason.check_id
                                    (:OLD.client_identification
                                    ,:OLD.client_id_type_code
                                    ,:NEW.client_identification
                                    ,:NEW.client_id_type_code);
      IF v_client_update_action_code IS NOT NULL THEN
        client_client_update_reason.init;
        client_client_update_reason.set_forest_client_audit_id(v_forest_client_audit_id);
        client_client_update_reason.set_client_update_action_code(v_client_update_action_code);
        --get reason from client pkg
        client_client_update_reason.set_client_update_reason_code(client_forest_client.get_ur_reason_id);
        client_client_update_reason.set_client_type_code(:NEW.client_type_code);
        client_client_update_reason.set_add_timestamp(:NEW.update_timestamp);
        client_client_update_reason.set_add_userid(:NEW.update_userid);
        client_client_update_reason.set_update_timestamp(:NEW.update_timestamp);
        client_client_update_reason.set_update_userid(:NEW.update_userid);
        client_client_update_reason.validate;
        IF NOT client_client_update_reason.error_raised THEN
          client_client_update_reason.add;
        END IF;
        IF client_client_update_reason.error_raised THEN
          RAISE_APPLICATION_ERROR(-20400,'Error writing update reason (Id) in audit trigger.');
        END IF;
      END IF;

    END IF;
  ELSE
    --DELETING: Put the last row into the audit table before deleting
    --          replacing update userid/timestamp/org
    -->check PK to make sure we are deleting the record in progress
    IF  client_forest_client.get_client_number = :OLD.client_number
    -->check that userid and timestamp are available
    AND client_forest_client.get_update_timestamp IS NOT NULL
    AND client_forest_client.get_update_userid IS NOT NULL
    AND client_forest_client.get_update_org_unit IS NOT NULL THEN
      INSERT INTO for_cli_audit
             (forest_client_audit_id
            , client_audit_code
            , client_number
            , client_name
            , legal_first_name
            , legal_middle_name
            , client_status_code
            , client_type_code
            , birthdate
            , client_id_type_code
            , client_identification
            , registry_company_type_code
            , corp_regn_nmbr
            , client_acronym
            , wcb_firm_number
            , ocg_supplier_nmbr
            , client_comment
            , add_timestamp
            , add_userid
            , add_org_unit
            , update_timestamp
            , update_userid
            , update_org_unit)
      VALUES (forest_client_audit_seq.NEXTVAL
            , v_client_audit_code
            , :OLD.client_number
            , :OLD.client_name
            , :OLD.legal_first_name
            , :OLD.legal_middle_name
            , :OLD.client_status_code
            , :OLD.client_type_code
            , :OLD.birthdate
            , :OLD.client_id_type_code
            , :OLD.client_identification
            , :OLD.registry_company_type_code
            , :OLD.corp_regn_nmbr
            , :OLD.client_acronym
            , :OLD.wcb_firm_number
            , :OLD.ocg_supplier_nmbr
            , :OLD.client_comment
            , :OLD.add_timestamp
            , :OLD.add_userid
            , :OLD.add_org_unit
              , client_forest_client.get_update_timestamp
              , client_forest_client.get_update_userid
              , client_forest_client.get_update_org_unit);
    ELSE
      RAISE_APPLICATION_ERROR(-20500,'Data consistency error in auditing deletion of FOREST_CLIENT');
    END IF;
  END IF;
END client_for_client_ar_iud_trg;




/
ALTER TRIGGER "THE"."CLIENT_FOR_CLIENT_AR_IUD_TRG" ENABLE;

  CREATE OR REPLACE EDITIONABLE TRIGGER "THE"."CLIENT_CLIENT_LOCN_AR_IUD_TRG" 
/******************************************************************************
   Trigger: CLIENT_CLIENT_LOCN_AR_IUD_TRG
   Purpose: This trigger audits changes to the CLIENT_LOCATION table
   Revision History
   Person               Date       Comments
   -----------------    ---------  --------------------------------
   R.A.Robb             2006-12-27 Created
   TMcClelland          2007-08-31 Added client_type_code to trigger insert
******************************************************************************/
AFTER INSERT OR UPDATE OR DELETE
  OF client_number
   , client_locn_code
   , client_locn_name
   , hdbs_company_code
   , address_1
   , address_2
   , address_3
   , city
   , province
   , postal_code
   , country
   , business_phone
   , home_phone
   , cell_phone
   , fax_number
   , email_address
   , locn_expired_ind
   , returned_mail_date
   , trust_location_ind
   , cli_locn_comment
  ON client_location
  FOR EACH ROW
DECLARE
  v_client_audit_code                cli_locn_audit.client_audit_code%TYPE;
  v_client_update_action_code        client_update_action_code.client_update_action_code%TYPE;
  v_client_update_reason_code        client_update_reason_code.client_update_reason_code%TYPE;
BEGIN
  IF INSERTING THEN
    v_client_audit_code := client_constants.c_audit_insert;
  ELSIF UPDATING THEN
    v_client_audit_code := client_constants.c_audit_update;
  ELSE
    v_client_audit_code := client_constants.c_audit_delete;
  END IF;

  IF    INSERTING
     OR UPDATING THEN

    --Process update reasons
    IF UPDATING THEN
      --Address Change
      v_client_update_action_code := NULL;
      v_client_update_action_code := client_client_update_reason.check_address
                                    (:OLD.address_1
                                    ,:OLD.address_2
                                    ,:OLD.address_3
                                    ,:OLD.city
                                    ,:OLD.province
                                    ,:OLD.postal_code
                                    ,:OLD.country
                                    ,:NEW.address_1
                                    ,:NEW.address_2
                                    ,:NEW.address_3
                                    ,:NEW.city
                                    ,:NEW.province
                                    ,:NEW.postal_code
                                    ,:NEW.country);
      IF v_client_update_action_code IS NOT NULL THEN
        --get reason from client locn pkg
        v_client_update_reason_code := client_client_location.get_ur_reason_addr;
      END IF;
    END IF;

    --Put the new row into the audit table
    INSERT INTO cli_locn_audit
           (client_location_audit_id
          , client_audit_code
          , client_number
          , client_locn_code
          , client_locn_name
          , hdbs_company_code
          , address_1
          , address_2
          , address_3
          , city
          , province
          , postal_code
          , country
          , business_phone
          , home_phone
          , cell_phone
          , fax_number
          , email_address
          , locn_expired_ind
          , returned_mail_date
          , trust_location_ind
          , cli_locn_comment
          , client_update_action_code
          , client_update_reason_code
          , client_type_code
          , update_timestamp
          , update_userid
          , update_org_unit
          , add_timestamp
          , add_userid
          , add_org_unit)
    SELECT client_location_audit_seq.nextval
          , v_client_audit_code
          , :NEW.client_number
          , :NEW.client_locn_code
          , :NEW.client_locn_name
          , :NEW.hdbs_company_code
          , :NEW.address_1
          , :NEW.address_2
          , :NEW.address_3
          , :NEW.city
          , :NEW.province
          , :NEW.postal_code
          , :NEW.country
          , :NEW.business_phone
          , :NEW.home_phone
          , :NEW.cell_phone
          , :NEW.fax_number
          , :NEW.email_address
          , :NEW.locn_expired_ind
          , :NEW.returned_mail_date
          , :NEW.trust_location_ind
          , :NEW.cli_locn_comment
          , v_client_update_action_code
          , v_client_update_reason_code
          , client_type_code
          , :NEW.update_timestamp
          , :NEW.update_userid
          , :NEW.update_org_unit
          , :NEW.add_timestamp
          , :NEW.add_userid
          , :NEW.add_org_unit
    FROM forest_client
    WHERE client_number = :NEW.client_number;

  ELSE
    --DELETING: Put the last row into the audit table before deleting
    --          replacing update userid/timestamp/org
    -->check PK to make sure we are deleting the record in progress
    IF  client_client_location.get_client_number = :OLD.client_number
    AND client_client_location.get_client_locn_code = :OLD.client_locn_code
    -->check that userid and timestamp are available
    AND client_client_location.get_update_timestamp IS NOT NULL
    AND client_client_location.get_update_userid IS NOT NULL
    AND client_client_location.get_update_org_unit IS NOT NULL THEN
       INSERT INTO cli_locn_audit
             (client_location_audit_id
            , client_audit_code
            , client_number
            , client_locn_code
            , client_locn_name
            , hdbs_company_code
            , address_1
            , address_2
            , address_3
            , city
            , province
            , postal_code
            , country
            , business_phone
            , home_phone
            , cell_phone
            , fax_number
            , email_address
            , locn_expired_ind
            , returned_mail_date
            , trust_location_ind
            , cli_locn_comment
            , update_timestamp
            , update_userid
            , update_org_unit
            , add_timestamp
            , add_userid
            , add_org_unit)
      VALUES (client_location_audit_seq.nextval
            , v_client_audit_code
            , :OLD.client_number
            , :OLD.client_locn_code
            , :OLD.client_locn_name
            , :OLD.hdbs_company_code
            , :OLD.address_1
            , :OLD.address_2
            , :OLD.address_3
            , :OLD.city
            , :OLD.province
            , :OLD.postal_code
            , :OLD.country
            , :OLD.business_phone
            , :OLD.home_phone
            , :OLD.cell_phone
            , :OLD.fax_number
            , :OLD.email_address
            , :OLD.locn_expired_ind
            , :OLD.returned_mail_date
            , :OLD.trust_location_ind
            , :OLD.cli_locn_comment
            , client_client_location.get_update_timestamp
            , client_client_location.get_update_userid
            , client_client_location.get_update_org_unit
            , :OLD.add_timestamp
            , :OLD.add_userid
            , :OLD.add_org_unit);
    ELSE
      RAISE_APPLICATION_ERROR(-20500,'Data consistency error in auditing deletion of CLIENT_LOCATION');
    END IF;
  END IF;
END client_client_locn_ar_iud_trg;




/
ALTER TRIGGER "THE"."CLIENT_CLIENT_LOCN_AR_IUD_TRG" ENABLE;

  CREATE OR REPLACE EDITIONABLE TRIGGER "THE"."TB1$_FOREST_INVC_TXN" 
  BEFORE INSERT OR UPDATE ON THE.FOREST_INVC_TXN
  FOR EACH ROW
BEGIN
  :NEW.INVOICE_NUMBER     := NVL(RTRIM(:NEW.INVOICE_NUMBER), ' ');
  :NEW.BILLING_STATUS_ST  := NVL(RTRIM(:NEW.BILLING_STATUS_ST), ' ');
  :NEW.INVOICE_CLASS_CODE := NVL(RTRIM(:NEW.INVOICE_CLASS_CODE), ' ');
  :NEW.INVOICE_TYPE_CODE  := NVL(RTRIM(:NEW.INVOICE_TYPE_CODE), ' ');
  :NEW.INVC_SUB_TYPE_CODE := NVL(RTRIM(:NEW.INVC_SUB_TYPE_CODE), ' ');
  :NEW.RELATED_INVOICE    := NVL(RTRIM(:NEW.RELATED_INVOICE), ' ');
  :NEW.CLIENT_NUMBER      := NVL(RTRIM(:NEW.CLIENT_NUMBER), ' ');
  :NEW.CLIENT_LOCN_CODE   := NVL(RTRIM(:NEW.CLIENT_LOCN_CODE), ' ');
  :NEW.ENTRY_USERID       := NVL(RTRIM(:NEW.ENTRY_USERID), ' ');
  :NEW.UPDATE_USERID      := NVL(RTRIM(:NEW.UPDATE_USERID), ' ');
END;




/
ALTER TRIGGER "THE"."TB1$_FOREST_INVC_TXN" ENABLE;

  CREATE OR REPLACE EDITIONABLE TRIGGER "THE"."SCS_SCST_BR_IU_TRG" 
 BEFORE INSERT OR UPDATE
 ON SCALE_SITE
 FOR EACH ROW
BEGIN
  IF INSERTING THEN
    -- Find the next available free SCALE_SITE_ID_NMBR
     select min(X) INTO :NEW.scale_site_id_nmbr
       from (SELECT TO_CHAR(INTEGER_VALUE) X
                    FROM INTEGER_NUMBER where integer_value > 1000
                   MINUS SELECT SCALE_SITE_ID_NMBR FROM SCALE_SITE) ;

    :NEW.entry_userid := NVL(:NEW.entry_userid, USER);
    :NEW.entry_timestamp := SYSDATE;
    :NEW.update_userid := NVL(:NEW.update_userid, USER);
    :NEW.update_timestamp := SYSDATE;
    :NEW.revision_count := 0;
  ELSIF UPDATING THEN
    :NEW.update_userid := NVL(:NEW.update_userid, USER);
    :NEW.update_timestamp := SYSDATE;
    :NEW.revision_count := :OLD.revision_count + 1;
  END IF;
END;



/
ALTER TRIGGER "THE"."SCS_SCST_BR_IU_TRG" ENABLE;

  CREATE OR REPLACE EDITIONABLE TRIGGER "THE"."TB1$_FOREST_INVOICE" 
   BEFORE INSERT OR UPDATE ON THE.FOREST_INVOICE
   FOR EACH ROW
BEGIN
   --If HBS is inserting a blank invc_sub_type_code, then replace it with HVS
   if :NEW.INVOICE_TYPE_CODE IN ('PSI','WSI') then
      :NEW.INVC_SUB_TYPE_CODE :=   NVL ( RTRIM ( :NEW.INVC_SUB_TYPE_CODE ), 'HVS' );
   end if;
   :NEW.INVOICE_NUMBER :=       NVL ( RTRIM ( :NEW.INVOICE_NUMBER ), ' ' );
   :NEW.CANCELLATION_IND :=     NVL ( RTRIM ( :NEW.CANCELLATION_IND ), ' ' );
   :NEW.PAYABLE_BY_CLIENT :=    NVL ( RTRIM ( :NEW.PAYABLE_BY_CLIENT ), ' ' );
   :NEW.PAYABLE_BY_CLI_LOC :=   NVL ( RTRIM ( :NEW.PAYABLE_BY_CLI_LOC ), ' ' );
   :NEW.INVOICE_CLASS_CODE :=   NVL ( RTRIM ( :NEW.INVOICE_CLASS_CODE ), ' ' );
   :NEW.INVOICE_TYPE_CODE :=    NVL ( RTRIM ( :NEW.INVOICE_TYPE_CODE ), ' ' );
   :NEW.INVC_SUB_TYPE_CODE :=   NVL ( RTRIM ( :NEW.INVC_SUB_TYPE_CODE ), ' ' );
   :NEW.CANCELLED_BY_INVC :=        RTRIM ( :NEW.CANCELLED_BY_INVC );
   :NEW.RELATED_INVOICE :=          RTRIM ( :NEW.RELATED_INVOICE );
   :NEW.REPLACED_BY_INVC :=         RTRIM ( :NEW.REPLACED_BY_INVC );
   :NEW.SCALE_SITE_ID_NMBR :=       RTRIM ( :NEW.SCALE_SITE_ID_NMBR );
   :NEW.SCALER_LICENCE_NUM :=       RTRIM ( :NEW.SCALER_LICENCE_NUM );
   :NEW.VOLUME_ONLY_IND :=      NVL ( RTRIM ( :NEW.VOLUME_ONLY_IND ), ' ' );
   :NEW.COPY_TO_CLIENT :=           RTRIM ( :NEW.COPY_TO_CLIENT );
   :NEW.COPY_TO_CLI_LOC :=          RTRIM ( :NEW.COPY_TO_CLI_LOC );
   :NEW.ENTRY_USERID :=         NVL ( RTRIM ( SUBSTR(:NEW.ENTRY_USERID,0,8) ), ' ' );
   :NEW.UPDATE_USERID :=        NVL ( RTRIM ( SUBSTR(:NEW.UPDATE_USERID,0,8) ), ' ' );
   :NEW.SCALED_AT :=            NVL ( RTRIM ( :NEW.SCALED_AT ), ' ' );
END;




/
ALTER TRIGGER "THE"."TB1$_FOREST_INVOICE" ENABLE;

  CREATE OR REPLACE EDITIONABLE TRIGGER "THE"."PFU_B_I_U_D" 
  BEFORE DELETE OR INSERT OR UPDATE
  ON PROV_FOREST_USE
  REFERENCING NEW AS NEW OLD AS OLD
  FOR EACH ROW
DECLARE
/******************************************************************************
   PURPOSE:    Audits changes to column values.

   REVISIONS:
   Ver        Date        Author           Description
   ---------  ----------  ---------------  ------------------------------------
   1.0        2/3/2006    Pangaea          1. Created this trigger.
              15/02/2018  CGI              Added FOREST_TENURE_GUID
			  22/OCt/2018 CGI              Modified to comment AUDIT_TABLE_SEQUENCE.

******************************************************************************/

  v_action                           prov_forest_use_audit.action%TYPE;
  v_db_user                          VARCHAR2(120);
BEGIN
  IF INSERTING THEN
    v_action := 'INSERT';
  ELSIF UPDATING THEN
    v_action := 'UPDATE';
  ELSIF DELETING THEN
    v_action := 'DELETE';
  END IF;

  --Get machinename and db userid.
  SELECT SYS_CONTEXT('USERENV', 'TERMINAL') || '\' || SYS_CONTEXT('USERENV', 'OS_USER')
    INTO v_db_user
    FROM DUAL;

  IF INSERTING
     OR UPDATING THEN                              --Insert NEW values into auditing table
    INSERT INTO prov_forest_use_audit
           ( -- audit_table_sequence,
		    action
          , forest_file_id
          , forest_tenure_guid
          , file_status_st
          , file_status_date
          , file_type_code
          , forest_region
          , bcts_org_unit
          , sb_funded_ind
          , district_admin_zone
          , mgmt_unit_type
          , mgmt_unit_id
          , pfu_revision_count
          , pfu_entry_userid
          , pfu_entry_timestamp
          , pfu_update_userid
          , pfu_update_timestamp
          , revision_count
          , entry_userid
          , entry_timestamp
          , update_userid
          , update_timestamp)
    VALUES (-- PROV_FOREST_USE_AUDIT_SEQ.NEXTVAL
	        v_action
          , :NEW.forest_file_id
          , :NEW.forest_tenure_guid
          , :NEW.file_status_st
          , :NEW.file_status_date
          , :NEW.file_type_code
          , :NEW.forest_region
          , :NEW.bcts_org_unit
          , :NEW.sb_funded_ind
          , :NEW.district_admin_zone
          , :NEW.mgmt_unit_type
          , :NEW.mgmt_unit_id
          , :NEW.revision_count
          , :NEW.entry_userid
          , :NEW.entry_timestamp
          , :NEW.update_userid
          , :NEW.update_timestamp
          , 1
          , v_db_user
          , SYSDATE
          , v_db_user
          , SYSDATE);
  ELSIF DELETING THEN
    INSERT INTO prov_forest_use_audit
           (-- audit_table_sequence ,
		    action
          , forest_file_id
          , forest_tenure_guid
          , file_status_st
          , file_status_date
          , file_type_code
          , forest_region
          , bcts_org_unit
          , sb_funded_ind
          , district_admin_zone
          , mgmt_unit_type
          , mgmt_unit_id
          , pfu_revision_count
          , pfu_entry_userid
          , pfu_entry_timestamp
          , pfu_update_userid
          , pfu_update_timestamp
          , revision_count
          , entry_userid
          , entry_timestamp
          , update_userid
          , update_timestamp)
    VALUES (-- PROV_FOREST_USE_AUDIT_SEQ.NEXTVAL,
	        v_action
          , :OLD.forest_file_id
          , :OLD.forest_tenure_guid
          , :OLD.file_status_st
          , :OLD.file_status_date
          , :OLD.file_type_code
          , :OLD.forest_region
          , :OLD.bcts_org_unit
          , :OLD.sb_funded_ind
          , :OLD.district_admin_zone
          , :OLD.mgmt_unit_type
          , :OLD.mgmt_unit_id
          , :OLD.revision_count
          , :OLD.entry_userid
          , :OLD.entry_timestamp
          , :OLD.update_userid
          , :OLD.update_timestamp
          , 1
          , v_db_user
          , SYSDATE
          , v_db_user
          , SYSDATE);
  END IF;
EXCEPTION
  WHEN OTHERS THEN
    -- Consider logging the error and then re-raise
    RAISE;
END;


/
ALTER TRIGGER "THE"."PFU_B_I_U_D" ENABLE;

  CREATE OR REPLACE EDITIONABLE TRIGGER "THE"."FI_PFU_AIUD_TR" 
  AFTER INSERT OR UPDATE OR DELETE OF FILE_TYPE_CODE,
                                      FILE_STATUS_ST
  ON PROV_FOREST_USE
  FOR EACH ROW
DECLARE
  c_op_scope_interest CONSTANT    VARCHAR2(20) := 'Interest_All_Attrs';
  p_amend_type     VARCHAR2(4) := null;
  p_amend_skey     NUMBER(10) := null;
  p_hva_skey       NUMBER(10) := null;
  p_map_feature_id NUMBER(10) := null;

BEGIN

   if updating then
      IF :new.file_type_code != :old.file_type_code OR :new.file_status_st != :old.file_status_st THEN
         Fta_Ilrr_Acquirer_Plugin_Pkg.MAINLINE('Update',
                                           :new.forest_file_id,
                                           p_hva_skey,
                                           c_op_scope_interest,
                                           p_amend_type,
                                           p_amend_skey,
                                           p_map_feature_id);
      END IF;
   end if;

   if deleting then
      Fta_Ilrr_Acquirer_Plugin_Pkg.MAINLINE('Delete',
                                        :old.forest_file_id,
                                        p_hva_skey,
                                        c_op_scope_interest,
                                        p_amend_type,
                                        p_amend_skey,
                                        p_map_feature_id);
   end if;

   if inserting then
      Fta_Ilrr_Acquirer_Plugin_Pkg.MAINLINE('Insert',
                                        :new.forest_file_id,
                                        p_hva_skey,
                                        c_op_scope_interest,
                                        p_amend_type,
                                        p_amend_skey,
                                        p_map_feature_id);
   end if;
END;



/
ALTER TRIGGER "THE"."FI_PFU_AIUD_TR" ENABLE;

  CREATE OR REPLACE EDITIONABLE TRIGGER "THE"."TB1$_GNRL_INVC_TXN" 
  BEFORE INSERT OR UPDATE ON THE.GNRL_INVC_TXN
  FOR EACH ROW
BEGIN
  :NEW.MINISTRY_REF_NUM := NVL(RTRIM(:NEW.MINISTRY_REF_NUM), ' ');
END;




/
ALTER TRIGGER "THE"."TB1$_GNRL_INVC_TXN" ENABLE;

  CREATE OR REPLACE EDITIONABLE TRIGGER "THE"."TB1$_GENERAL_INVOICE" 
    BEFORE INSERT OR UPDATE ON THE.GENERAL_INVOICE
    FOR EACH ROW
BEGIN
  :NEW.INVOICE_NUMBER :=       NVL ( RTRIM ( :NEW.INVOICE_NUMBER ), ' ' ) ;
  :NEW.CANCELLATION_IND :=     NVL ( RTRIM ( :NEW.CANCELLATION_IND ), ' ' ) ;
--  :NEW.FOREST_FILE_ID :=       NVL ( RTRIM ( :NEW.FOREST_FILE_ID ), ' ' ) ;
  :NEW.MINISTRY_REF_NUM :=     NVL ( RTRIM ( :NEW.MINISTRY_REF_NUM ), ' ' ) ;
  :NEW.UPDATE_USERID :=        NVL ( RTRIM ( :NEW.UPDATE_USERID ), ' ' ) ;
  :NEW.ENTRY_USERID :=         NVL ( RTRIM ( :NEW.ENTRY_USERID ), ' ' ) ;
  END ;




/
ALTER TRIGGER "THE"."TB1$_GENERAL_INVOICE" ENABLE;

  CREATE OR REPLACE EDITIONABLE TRIGGER "THE"."FI_HVA_AIUD_TR" 
  AFTER INSERT OR UPDATE OR DELETE OF
      issue_date,
      expiry_date,
      extend_date,
      harvest_auth_status_code,
      retirement_date
ON HARVESTING_AUTHORITY
  FOR EACH ROW
DECLARE
  c_op_scope_interest       CONSTANT VARCHAR2(8) := 'Interest';
  p_amend_type                       NUMBER(4) := NULL;
  p_amend_skey                       NUMBER(10) := NULL;
  p_map_feature_id                   NUMBER(10) := NULL;

BEGIN
  IF INSERTING AND :new.cutting_permit_id IS NOT NULL THEN
    fta_ilrr_acquirer_plugin_pkg.mainline('Insert'
                                        , :new.forest_file_id
                                        , :new.hva_skey
                                        , c_op_scope_interest
                                        , p_amend_type
                                        , p_amend_skey
                                        , p_map_feature_id);
  END IF;

  IF UPDATING THEN
     IF :new.cutting_permit_id IS NOT NULL THEN
       fta_ilrr_acquirer_plugin_pkg.mainline('Update'
                                        , :new.forest_file_id
                                        , :new.hva_skey
                                        , c_op_scope_interest
                                        , p_amend_type
                                        , p_amend_skey
                                        , p_map_feature_id);
     --
     ELSIF :new.retirement_date IS NOT NULL THEN
       --Retire the individual Geometry in the ILRR
       Fta_Ilrr_Acquirer_Plugin_Pkg.MAINLINE('Delete',
                                        :new.forest_file_id,
                                        :new.hva_skey,
                                        'Geometry',
                                        'HARV',
                                        p_amend_skey,
                                        p_map_feature_id);
     END IF;
  END IF;

  IF DELETING AND :old.cutting_permit_id IS NOT NULL THEN
    fta_ilrr_acquirer_plugin_pkg.mainline('Delete'
                                        , :old.forest_file_id
                                        , :old.hva_skey
                                        , c_op_scope_interest
                                        , p_amend_type
                                        , p_amend_skey
                                        , p_map_feature_id);
  END IF;
END;



/
ALTER TRIGGER "THE"."FI_HVA_AIUD_TR" ENABLE;

  CREATE OR REPLACE EDITIONABLE TRIGGER "THE"."HVA_B_I_U_D" 
  BEFORE DELETE OR INSERT OR UPDATE
  ON HARVESTING_AUTHORITY
  REFERENCING NEW AS NEW OLD AS OLD
  FOR EACH ROW
DECLARE
/******************************************************************************
   PURPOSE:    Audits changes to column values.

   REVISIONS:
   Ver        Date        Author           Description
   ---------  ----------  ---------------  ------------------------------------
   1.0        2/6/2006    Pangaea          1. Created this trigger.
              5/2/2006    S Lewis          1. Modified to match schema update.
              14/02/2018  CGI              Added HARVESTING_AUTHORITY_GUID
			  22/OCt/2018 CGI              Modified to comment AUDIT_TABLE_SEQUENCE.
			  16/Jul/2020 CGI			   Added FTA4.22 new columns.
******************************************************************************/
  v_action                           harvesting_authority_audit.action%TYPE;
  v_db_user                          VARCHAR2(120);
BEGIN
  IF INSERTING THEN
    v_action := 'INSERT';
  ELSIF UPDATING THEN
    v_action := 'UPDATE';
  ELSIF DELETING THEN
    v_action := 'DELETE';
  END IF;


  --Get machinename and db userid.
  SELECT SYS_CONTEXT('USERENV', 'TERMINAL') || '\' || SYS_CONTEXT('USERENV', 'OS_USER')
    INTO v_db_user
    FROM DUAL;

  IF INSERTING THEN                                --Insert NEW values into auditing table
    INSERT INTO harvesting_authority_audit
           (-- audit_table_sequence,
		    action
          , hva_skey
          , forest_file_id
          , cutting_permit_id
          , harvesting_authority_id
          , harvesting_authority_guid
          , forest_district
          , district_admn_zone
          , geographic_district
          , mgmt_unit_id
          , mgmt_unit_type_code
          , harvest_auth_status_code
          , catastrophic_ind
          , crown_granted_ind
          , cruise_based_ind
          , tenure_term
          , deciduous_ind
          , bcaa_folio_number
          , extend_count
          , harvest_auth_extend_reas_code
          , cascade_split_code
          , expiry_date
          , extend_date
          , issue_date
          , status_date
          , LOCATION
          , higher_level_plan_reference
          , harvest_type_code
          , quota_type_code
          , crown_lands_region_code
          , retirement_date
          , salvage_type_code
          , harvest_area
		  , is_waste_assessment_required  -- Thursday July 16, 2020 related to FTA4.22
		  , is_cp_extensn_appl_fee_waived -- Thursday July 16, 2020 related to FTA4.22
		  , is_cp_extension_appl_fee_paid -- Thursday July 16, 2020 related to FTA4.22
		  , is_within_fibre_recovery_zone -- Thursday July 16, 2020 related to FTA4.22
          , revision_count
          , entry_userid
          , entry_timestamp
          , update_userid
          , update_timestamp)
    VALUES (-- HARVESTING_AUTHORITY_AUDIT_SEQ.NEXTVAL,
	        v_action
          , :NEW.hva_skey
          , :NEW.forest_file_id
          , :NEW.cutting_permit_id
          , :NEW.harvesting_authority_id
          , :NEW.harvesting_authority_guid
          , :NEW.forest_district
          , :NEW.district_admn_zone
          , :NEW.geographic_district
          , :NEW.mgmt_unit_id
          , :NEW.mgmt_unit_type_code
          , :NEW.harvest_auth_status_code
          , :NEW.catastrophic_ind
          , :NEW.crown_granted_ind
          , :NEW.cruise_based_ind
          , :NEW.tenure_term
          , :NEW.deciduous_ind
          , :NEW.bcaa_folio_number
          , :NEW.extend_count
          , :NEW.harvest_auth_extend_reas_code
          , :NEW.cascade_split_code
          , :NEW.expiry_date
          , :NEW.extend_date
          , :NEW.issue_date
          , :NEW.status_date
          , :NEW.LOCATION
          , :NEW.higher_level_plan_reference
          , :NEW.harvest_type_code
          , :NEW.quota_type_code
          , :NEW.crown_lands_region_code
          , :NEW.retirement_date
          , :NEW.salvage_type_code
          , :NEW.harvest_area
		  , :NEW.is_waste_assessment_required  -- Thursday July 16, 2020 related to FTA4.22
		  , :NEW.is_cp_extensn_appl_fee_waived -- Thursday July 16, 2020 related to FTA4.22
		  , :NEW.is_cp_extension_appl_fee_paid -- Thursday July 16, 2020 related to FTA4.22
		  , :NEW.is_within_fibre_recovery_zone -- Thursday July 16, 2020 related to FTA4.22
          , :NEW.revision_count
          , :NEW.entry_userid
          , :NEW.entry_timestamp
          , :NEW.update_userid
          , :NEW.update_timestamp);
  ELSIF DELETING
        OR UPDATING THEN
    INSERT INTO harvesting_authority_audit
           (-- audit_table_sequence ,
		    action
          , hva_skey
          , forest_file_id
          , cutting_permit_id
          , harvesting_authority_id
          , harvesting_authority_guid
          , forest_district
          , district_admn_zone
          , geographic_district
          , mgmt_unit_id
          , mgmt_unit_type_code
          , harvest_auth_status_code
          , catastrophic_ind
          , crown_granted_ind
          , cruise_based_ind
          , tenure_term
          , deciduous_ind
          , bcaa_folio_number
          , extend_count
          , harvest_auth_extend_reas_code
          , cascade_split_code
          , expiry_date
          , extend_date
          , issue_date
          , status_date
          , LOCATION
          , higher_level_plan_reference
          , harvest_type_code
          , quota_type_code
          , crown_lands_region_code
          , retirement_date
          , salvage_type_code
          , harvest_area
		  , is_waste_assessment_required  -- Thursday July 16, 2020 related to FTA4.22
		  , is_cp_extensn_appl_fee_waived -- Thursday July 16, 2020 related to FTA4.22
		  , is_cp_extension_appl_fee_paid -- Thursday July 16, 2020 related to FTA4.22
		  , is_within_fibre_recovery_zone -- Thursday July 16, 2020 related to FTA4.22
          , revision_count
          , entry_userid
          , entry_timestamp
          , update_userid
          , update_timestamp)
    VALUES (-- HARVESTING_AUTHORITY_AUDIT_SEQ.NEXTVAL,
	        v_action
          , :OLD.hva_skey
          , :OLD.forest_file_id
          , :OLD.cutting_permit_id
          , :OLD.harvesting_authority_id
          , :OLD.harvesting_authority_guid
          , :OLD.forest_district
          , :OLD.district_admn_zone
          , :OLD.geographic_district
          , :OLD.mgmt_unit_id
          , :OLD.mgmt_unit_type_code
          , :OLD.harvest_auth_status_code
          , :OLD.catastrophic_ind
          , :OLD.crown_granted_ind
          , :OLD.cruise_based_ind
          , :OLD.tenure_term
          , :OLD.deciduous_ind
          , :OLD.bcaa_folio_number
          , :OLD.extend_count
          , :OLD.harvest_auth_extend_reas_code
          , :OLD.cascade_split_code
          , :OLD.expiry_date
          , :OLD.extend_date
          , :OLD.issue_date
          , :OLD.status_date
          , :OLD.LOCATION
          , :OLD.higher_level_plan_reference
          , :OLD.harvest_type_code
          , :OLD.quota_type_code
          , :OLD.crown_lands_region_code
          , :OLD.retirement_date
          , :OLD.salvage_type_code
          , :OLD.harvest_area
		  , :OLD.is_waste_assessment_required  -- Thursday July 16, 2020 related to FTA4.22
		  , :OLD.is_cp_extensn_appl_fee_waived -- Thursday July 16, 2020 related to FTA4.22
		  , :OLD.is_cp_extension_appl_fee_paid -- Thursday July 16, 2020 related to FTA4.22
		  , :OLD.is_within_fibre_recovery_zone -- Thursday July 16, 2020 related to FTA4.22
          , :OLD.revision_count
          , :OLD.entry_userid
          , :OLD.entry_timestamp
          , :OLD.update_userid
          , :OLD.update_timestamp);
  END IF;
EXCEPTION
  WHEN OTHERS THEN
    -- Consider logging the error and then re-raise
    RAISE;
END;


/
ALTER TRIGGER "THE"."HVA_B_I_U_D" ENABLE;

  CREATE OR REPLACE EDITIONABLE TRIGGER "THE"."FTA_SYNC_TM_BRM" 
/******************************************************************************
   Trigger:  fta_sync_tm_brm
   Purpose: This trigger will update BLANKET_ROAD_MARK to keep in sync with
            TIMBER_MARK
   Revision History
   Person               Date       Comments
   -----------------   ---------  --------------------------------
   C. Geisler          2014-10-20 Created

******************************************************************************/
AFTER INSERT OR UPDATE of
				mark_issue_date,
				mark_expiry_date,
				mark_status_st
  ON timber_mark
  FOR EACH ROW
DECLARE
BEGIN
  IF INSERTING OR UPDATING THEN
    UPDATE  blanket_road_mark
    SET		blanket_road_mark_status = :new.mark_status_st
          , issue_date = :new.mark_issue_date
          , expiry_date = :new.mark_expiry_date
          , update_userid = :new.update_userId
          , update_timestamp = :new.update_timestamp
          , revision_count = revision_count + 1
    WHERE	timber_mark = :new.timber_mark
    ;
  END IF;
END;



/
ALTER TRIGGER "THE"."FTA_SYNC_TM_BRM" ENABLE;

  CREATE OR REPLACE EDITIONABLE TRIGGER "THE"."HAA_B_I_U_D" 
  BEFORE DELETE OR INSERT OR UPDATE
  ON HAULING_AUTHORITY
  REFERENCING NEW AS NEW OLD AS OLD
  FOR EACH ROW
DECLARE
/******************************************************************************
   PURPOSE:    Audits changes to column values.

   REVISIONS:
   Ver        Date        Author           Description
   ---------  ----------  ---------------  ------------------------------------
   1.0        2/6/2006    Pangaea          1. Created this trigger.
              5/2/2006    S Lewis          1. Modified to match schema update.
              26/Apr/2018 CGI _ Victoria   included hauling_authority_guid
			  22/OCt/2018 CGI              Modified to comment AUDIT_TABLE_SEQUENCE.
******************************************************************************/
  v_action                           hauling_authority_audit.action%TYPE;
  v_db_user                          VARCHAR2(120);
BEGIN
  IF INSERTING THEN
    v_action := 'INSERT';
  ELSIF UPDATING THEN
    v_action := 'UPDATE';
  ELSIF DELETING THEN
    v_action := 'DELETE';
  END IF;

  --Get machinename and db userid.
  SELECT SYS_CONTEXT('USERENV', 'TERMINAL') || '\' || SYS_CONTEXT('USERENV', 'OS_USER')
    INTO v_db_user
    FROM DUAL;

  IF INSERTING THEN                                --Insert NEW values into auditing table
    INSERT INTO hauling_authority_audit
           (-- audit_table_sequence,
		    action
          , forest_file_id
          , timber_mark
          , hauling_authority_guid
          , marking_instrument_code
          , marking_method_code
          , revision_count
          , entry_userid
          , entry_timestamp
          , update_userid
          , update_timestamp)
    VALUES (-- HAULING_AUTHORITY_AUDIT_SEQ.NEXTVAL,
	        v_action
          , :NEW.forest_file_id
          , :NEW.timber_mark
          , :NEW.hauling_authority_guid
          , :NEW.marking_instrument_code
          , :NEW.marking_method_code
          , :NEW.revision_count
          , :NEW.entry_userid
          , :NEW.entry_timestamp
          , :NEW.update_userid
          , :NEW.update_timestamp);
  ELSIF DELETING
        OR UPDATING THEN
    INSERT INTO hauling_authority_audit
           (-- audit_table_sequence,
		    action
          , forest_file_id
          , timber_mark
          , hauling_authority_guid
          , marking_instrument_code
          , marking_method_code
          , revision_count
          , entry_userid
          , entry_timestamp
          , update_userid
          , update_timestamp)
    VALUES (-- HAULING_AUTHORITY_AUDIT_SEQ.NEXTVAL,
	        v_action
          , :OLD.forest_file_id
          , :OLD.timber_mark
          , :OLD.hauling_authority_guid
          , :OLD.marking_instrument_code
          , :OLD.marking_method_code
          , :OLD.revision_count
          , :OLD.entry_userid
          , :OLD.entry_timestamp
          , :OLD.update_userid
          , :OLD.update_timestamp);
  END IF;
EXCEPTION
  WHEN OTHERS THEN
    -- Consider logging the error and then re-raise
    RAISE;
END;


/
ALTER TRIGGER "THE"."HAA_B_I_U_D" ENABLE;

  CREATE OR REPLACE EDITIONABLE TRIGGER "THE"."FTA_SYNC_HAA_TM" 
/******************************************************************************
   Trigger:  fta_sync_haa_tm
   Purpose: This trigger will update/delete TIMBER_MARK to keep in sync with
            HAULING_AUTHORITY/HARVESTING_AUTHORITY
      Will be dropped when TIMBER_MARK is no longer required
      NOTE: This trigger will not insert marks for private marks as those
      will be taken care of with a trigger on PRIVATE_MARK_CERTIFICATE
   Revision History
   Person               Date       Comments
   -----------------    ---------  --------------------------------
   M.Dellaviola     2005-01-02 Created
******************************************************************************/
AFTER UPDATE OR DELETE
  ON hauling_authority
  FOR EACH ROW
DECLARE

  CURSOR cur_tm
  IS
    SELECT forest_file_id,
           cutting_permit_id
    FROM timber_mark
    WHERE timber_mark = :OLD.timber_mark;

BEGIN
  IF UPDATING THEN
    -- UPDATE TIMBER_MARK
    UPDATE timber_mark
       SET markng_instrmnt_cd = :NEW.marking_instrument_code
         , marking_method_cd = :NEW.marking_method_code
         , update_userid = :NEW.update_userid
         , update_timestamp = :NEW.update_timestamp
         , revision_count = :NEW.revision_count
     WHERE timber_mark = :OLD.timber_mark;
  ELSIF DELETING THEN
    -- Assumes that delete process has already deleted child records
    -- The only time this should fails is due to other RI from non-FTA systems (eg. CIMS,HBS)

    FOR rec_tm IN cur_tm LOOP
      DELETE FROM prmt_authzd_blk
      WHERE forest_file_id = rec_tm.forest_file_id
      AND cutting_permit_id = rec_tm.cutting_permit_id;
    END LOOP;

    DELETE FROM timber_mark
     WHERE timber_mark = :OLD.timber_mark;
   END IF;
END fta_sync_haa_tm;




/
ALTER TRIGGER "THE"."FTA_SYNC_HAA_TM" ENABLE;

  CREATE OR REPLACE EDITIONABLE TRIGGER "THE"."FTA_SYNC_BRM_HAA_TM" 
/******************************************************************************
   Trigger:  fta_sync_brm_haa_tm
   Purpose: Populates timber_mark with a blanket road mark.
            Will be dropped when TIMBER_MARK is no longer required

   Revision History
   Person               Date       Comments
   -----------------    ---------  --------------------------------
   R.Nanton            2007-04-19  Created
   R.Pardo Figueroa    2008-03-26  Resolve Oracle warnings
   abujold             2011-01-24  MEAR 488 - generate proper issue date for
                                   Blanket road marks when assigned to segments.
   ABujold						 2021-05-06  Add true cruise based ind and quota type for
   																 new fields on blanket road mark table.
******************************************************************************/
AFTER INSERT
  ON THE.HAULING_AUTHORITY
  FOR EACH ROW
DECLARE
  v_count                            NUMBER(10);
  v_cascade_split_code               harvesting_authority.cascade_split_code%TYPE := 'E';

  v_cutting_permit_id                timber_mark.cutting_permit_id%type;
  v_mark_expiry_date                 timber_mark.mark_expiry_date%type;
  v_mark_issue_date                  timber_mark.mark_issue_date%type;

  CURSOR cur_brm
  IS
    SELECT forest_file_id
         , forest_district
         , timber_mark
         , issue_date
         , expiry_date
         , cruise_based_ind
         , quota_type_code
         , entry_userid
         , entry_timestamp
      FROM blanket_road_mark
     WHERE timber_mark = :NEW.timber_mark;

  CURSOR cur_pfu(
    p_forest_file_id                          prov_forest_use.forest_file_id%TYPE)
  IS
    SELECT ou.rollup_region_code
      FROM prov_forest_use pfu
         , org_unit ou
     WHERE pfu.forest_region = ou.org_unit_no
       AND pfu.forest_file_id = p_forest_file_id;

BEGIN
  IF INSERTING THEN
    --Only wish to insert into timber mark if mark is a blanket road mark. I.e. If
    --timber mark is in the blanket road mark table.
    FOR i IN cur_brm LOOP
      --Check to see if the record exists in timber_mark
      SELECT COUNT(ROWID)
        INTO v_count
        FROM timber_mark
       WHERE timber_mark = :NEW.timber_mark;

      IF v_count = 0 THEN
        FOR x IN cur_pfu(i.forest_file_id) LOOP
          IF x.rollup_region_code IN ('RSC','RWC') THEN
            v_cascade_split_code := 'W';
          END IF;
        END LOOP;


        --v_mark_issue_date := i.entry_timestamp;
        v_mark_issue_date := i.issue_date;



            --Insert into timber mark
        v_cutting_permit_id := '~'||SUBSTR(:NEW.timber_mark,-2);
        --v_mark_expiry_date  := TO_DATE('9999-12-31','YYYY-MM-DD');
        v_mark_expiry_date := i.expiry_date;

        INSERT INTO timber_mark(
            timber_mark    ,--n.
            forest_file_id    ,--n.
            cutting_permit_id    ,--n.
            forest_district    ,--n.
            geographic_distrct    ,--n.
            cascade_split_code    ,--y.
            quota_type_code    ,--y.
            deciduous_ind    ,--n.
            catastrophic_ind    ,--n.
            crown_granted_ind    ,--n.
            cruise_based_ind    ,--n.
            certificate    ,--y.
            hdbs_timber_mark    ,--y.
            vm_timber_mark    ,--y.
            tenure_term    ,--n.
            bcaa_folio_number    ,--y.
            activated_userid    ,--y.
            amended_userid    ,--y.
            district_admn_zone    ,--y.
            granted_acqrd_date    ,--y.
            lands_region    ,--y.
            crown_granted_acq_desc    ,--y.
            mark_status_st    ,--y.
            mark_status_date    ,--y.
            mark_amend_date    ,--y.
            mark_appl_date    ,--y.
            mark_cancel_date    ,--y.
            mark_extend_date    ,--y.
            mark_extend_rsn_cd    ,--y.
            mark_extend_count    ,--n.
            mark_issue_date    ,--y.
            mark_expiry_date    ,--y.
            markng_instrmnt_cd    ,--y.
            marking_method_cd    ,--y.
            entry_userid    ,--n.
            entry_timestamp    ,--n.
            update_userid    ,--n.
            update_timestamp    ,--n.
            revision_count    ,--n.
            small_patch_salvage_ind	,--n.
            salvage_type_code	--y.
        )
        VALUES(
            :NEW.timber_mark,--TIMBER_MARK	,--N.
            i.forest_file_id,--FOREST_FILE_ID	,--N.
            v_cutting_permit_id,--CUTTING_PERMIT_ID	,--N.
            i.forest_district,--FOREST_DISTRICT	,--N.
            i.forest_district,--GEOGRAPHIC_DISTRCT	,--N.
            v_cascade_split_code,--CASCADE_SPLIT_CODE	,--Y.
            i.quota_type_code,--QUOTA_TYPE_CODE	,--Y.
            'N',--DECIDUOUS_IND	,--N.
            'N',--CATASTROPHIC_IND	,--N.
            'N',--CROWN_GRANTED_IND	,--N.
            i.cruise_based_ind,--CRUISE_BASED_IND	,--N.
            NULL,--CERTIFICATE	,--Y.
            NULL,--HDBS_TIMBER_MARK	,--Y.
            NULL,--VM_TIMBER_MARK	,--Y.
            0,--TENURE_TERM	,--N.
            NULL,--BCAA_FOLIO_NUMBER	,--Y.
            NULL,--ACTIVATED_USERID	,--Y.
            NULL,--AMENDED_USERID	,--Y.
            NULL,--DISTRICT_ADMN_ZONE	,--Y.
            NULL,--GRANTED_ACQRD_DATE	,--Y.
            NULL,--LANDS_REGION	,--Y.
            NULL,--CROWN_GRANTED_ACQ_DESC	,--Y.
            'HI',--MARK_STATUS_ST	,--Y.
            SYSDATE,--MARK_STATUS_DATE	,--Y.
            NULL,--MARK_AMEND_DATE	,--Y.
            NULL,--MARK_APPL_DATE	,--Y.
            NULL,--MARK_CANCEL_DATE	,--Y.
            NULL,--MARK_EXTEND_DATE	,--Y.
            NULL,--MARK_EXTEND_RSN_CD	,--Y.
            0,--MARK_EXTEND_COUNT	,--N.
            v_mark_issue_date,--MARK_ISSUE_DATE	,--Y.
            v_mark_expiry_date,--MARK_EXPIRY_DATE	,--Y.
            NULL,--MARKNG_INSTRMNT_CD	,--Y.
            NULL,--MARKING_METHOD_CD	,--Y.
            i.entry_userid,--ENTRY_USERID	,--N.
            i.entry_timestamp,--ENTRY_TIMESTAMP	,--N.
            i.entry_userid,--UPDATE_USERID	,--N.
            i.entry_timestamp,--UPDATE_TIMESTAMP	,--N.
            1,--REVISION_COUNT	,--N.
            'N',--SMALL_PATCH_SALVAGE_IND	,--N.
            NULL--SALVAGE_TYPE_CODE	--Y.
        );

      END IF;

      EXIT;
    END LOOP;
  END IF;
END fta_sync_brm_haa_tm;


/
ALTER TRIGGER "THE"."FTA_SYNC_BRM_HAA_TM" ENABLE;

  CREATE OR REPLACE EDITIONABLE TRIGGER "THE"."FTA_SYNC_HVAHAA_TM" 
/******************************************************************************
   Trigger:  fta_sync_haa_tm
   Purpose: This trigger will insert TIMBER_MARK to keep in sync with
            HAULING_AUTHORITY/HARVESTING_AUTHORITY
         Will be dropped when TIMBER_MARK is no longer required
         NOTE: This trigger will not insert marks for private marks as those
         will be taken care of with a trigger on PRIVATE_MARK_CERTIFICATE
   Revision History
   Person               Date       Comments
   -----------------    ---------  --------------------------------
   M.Dellaviola         2005-01-02 Created
   S.Taylor             2006-11-08 Added FSJ support.
******************************************************************************/
AFTER INSERT
   ON harvesting_hauling_xref
   FOR EACH ROW
DECLARE
   v_file_type                 prov_forest_use.file_type_code%TYPE;
   v_mark_type                 prov_forest_use.file_type_code%TYPE;
   v_timber_mark               hauling_authority.timber_mark%TYPE;
   v_file                      harvesting_authority.forest_file_id%TYPE;
   v_mark_file                 harvesting_authority.forest_file_id%TYPE;
   v_cp                        harvesting_authority.cutting_permit_id%TYPE;
   v_instrument                hauling_authority.marking_instrument_code%TYPE;
   v_marking                   hauling_authority.marking_method_code%TYPE;
   v_mark_count                NUMBER (10);
   v_harvest_type_code         harvesting_authority.harvest_type_code%TYPE;
   v_harvesting_authority_id   harvesting_authority.harvesting_authority_id%TYPE;

   CURSOR curhva
   IS
      SELECT a.*
           , pfu.file_type_code
        FROM harvesting_authority a
           , prov_forest_use pfu
       WHERE a.hva_skey = :NEW.hva_skey
         AND a.forest_file_id = pfu.forest_file_id
         AND pfu.file_type_code NOT IN ('B08', 'B09', 'B14');
BEGIN
   -- get file/cp using hva_skey
   FOR rechva IN curhva
   LOOP
      SELECT pfu.forest_file_id
           , pfu.file_type_code
           , haa.marking_instrument_code
           , haa.marking_method_code
        INTO v_mark_file
           , v_mark_type
           , v_instrument
           , v_marking
        FROM hauling_authority haa
           , prov_forest_use pfu
       WHERE haa.timber_mark = :NEW.timber_mark
         AND pfu.forest_file_id = haa.forest_file_id;

      -- only derive CP from 'normal' multimarks. For data conversion historical multimarks
      -- may have anomalies (ie. A01 cp transferred to a multimark which would result in a different mark format
      -- from expected multimark format).
      IF     fta_valid_minor_tsl_file_type (rechva.file_type_code) = 'Y'
         AND SUBSTR (rechva.forest_file_id, 2) = SUBSTR (:NEW.timber_mark, 1, 5)
      THEN
          -- need to derive because CP will be null for multimarks as they really dont have CP but
         -- TIMBER_MARK will need the CP to keep other systems functioning until they switch over to the new tables
         v_cp := SUBSTR (:NEW.timber_mark, 6);
      ELSE
         v_cp := NVL (rechva.cutting_permit_id, ' ');
      END IF;

      IF v_mark_type IN ('B08', 'B09', 'B14')
      THEN
          -- want file of PRIVATE MARK for TIMBER_MARK synch
         -- do NOT want hva.forest_file_id as it may be the one managing the  PRIVATE MARK and for TIMBER_MARK purposes
         -- we need the parent file of the PRIVATE mark
         v_file := v_mark_file;
      END IF;

      IF v_mark_type = 'A06'
      THEN
          -- want file of TL for TIMBER_MARK synch
         -- do NOT want hva.forest_file_id as it may be the one managing the TL and for TIMBER_MARK purposes
         -- we need the parent file of the TL mark
         v_file := v_mark_file;
         v_cp := substr(:new.timber_mark,5,2);
      ELSE
           v_file := recHVA.forest_file_id;
      END IF;

      -- if this is a FSJ harvesting authority, use the last 3 chars of the timber_mark
      -- for the cutting_permit. FSJ HA's do not have CP's, but timber_mark requires
      -- a unique forest_file_id/cutting_permit_id combination.
      SELECT harvest_type_code
        INTO v_harvest_type_code
        FROM harvesting_authority
       WHERE hva_skey = :NEW.hva_skey;

      IF v_harvest_type_code = 'F'
      THEN
         IF v_file IS NULL
         THEN
            SELECT forest_file_id
                 , harvesting_authority_id
              INTO v_file
                 , v_harvesting_authority_id
              FROM harvesting_authority
             WHERE hva_skey = :NEW.hva_skey;
         END IF;

         -- set the cp to the last 3 chars of the hva id
         v_cp :=
            SUBSTR (v_harvesting_authority_id
                  , LENGTH (v_harvesting_authority_id) - 2
                  , LENGTH (v_harvesting_authority_id)
                   );
      END IF;

      -- create TIMBER_MARK
      --RN only if does not exist already
      SELECT COUNT (ROWID)
        INTO v_mark_count
        FROM timber_mark
       WHERE timber_mark = :NEW.timber_mark;

      IF v_mark_count = 0
      THEN
         INSERT INTO timber_mark
           (timber_mark
          , forest_file_id
          , cutting_permit_id
          , forest_district
          , geographic_distrct
          , cascade_split_code
          , quota_type_code
          , deciduous_ind
          , catastrophic_ind
          , crown_granted_ind
          , cruise_based_ind
          , tenure_term
          , district_admn_zone
          , lands_region
          , mark_status_st
          , mark_status_date
          , mark_extend_date
          , mark_extend_rsn_cd
          , mark_extend_count
          , mark_issue_date
          , mark_expiry_date
          , markng_instrmnt_cd
          , marking_method_cd
          , entry_userid
          , entry_timestamp
          , update_userid
          , update_timestamp
          , revision_count
          , small_patch_salvage_ind
          , salvage_type_code
           )
         VALUES (:NEW.timber_mark
           , v_file
           , v_cp
           , rechva.forest_district
           , rechva.geographic_district
           , rechva.cascade_split_code
           , rechva.quota_type_code
           , rechva.deciduous_ind
           , rechva.catastrophic_ind
           , rechva.crown_granted_ind
           , rechva.cruise_based_ind
           , rechva.tenure_term
           , rechva.district_admn_zone
           , rechva.crown_lands_region_code
           , rechva.harvest_auth_status_code
           , rechva.status_date
           , rechva.extend_date
           , rechva.harvest_auth_extend_reas_code
           , NVL (rechva.extend_count, 0)
           , rechva.issue_date
           , rechva.expiry_date
           , v_instrument
           , v_marking
           , rechva.entry_userid
           , rechva.entry_timestamp
           , rechva.update_userid
           , rechva.update_timestamp
           , rechva.revision_count
           , DECODE (rechva.salvage_type_code, 'SSS', 'Y', 'N')
           , rechva.salvage_type_code
            );
      END IF;
   END LOOP;
END fta_sync_hvahaa_tm;



/
ALTER TRIGGER "THE"."FTA_SYNC_HVAHAA_TM" ENABLE;

  CREATE OR REPLACE EDITIONABLE TRIGGER "THE"."FTA_SYNC_HVA_TM" 
/******************************************************************************
   Trigger:  fta_sync_hva_tm
   Purpose: This trigger will update TIMBER_MARK to keep in sync with
            HARVESTING_AUTHORITY
      Will be dropped when TIMBER_MARK is no longer required
      NOTE: This trigger will not insert marks for private marks as those
      will be taken care of with a trigger on PRIVATE_MARK_CERTIFICATE
   Revision History
   Person               Date       Comments
   -----------------    ---------  --------------------------------
   A.Stephenson        2007-04-19 Created
   R.Nanton            2007-06-04 Added FSJ processing .

******************************************************************************/
AFTER UPDATE
  ON harvesting_authority
  FOR EACH ROW
DECLARE
  v_earliest_expiry_hva_skey         harvesting_authority.hva_skey%TYPE;
  v_forest_district                  timber_mark.forest_district%TYPE;
  v_cascade_split_code               timber_mark.cascade_split_code%TYPE;
  v_geographic_distrct               timber_mark.geographic_distrct%TYPE;
  v_mark_status_st                   timber_mark.mark_status_st%TYPE;
  v_tenure_term                      timber_mark.tenure_term%TYPE;
  v_mark_status_date                 timber_mark.mark_status_date%TYPE;
  v_mark_issue_date                  timber_mark.mark_issue_date%TYPE;
  v_mark_expiry_date                 timber_mark.mark_expiry_date%TYPE;
  v_district_admn_zone               timber_mark.district_admn_zone%TYPE;
  v_mark_extend_date                 timber_mark.mark_extend_date%TYPE;
  v_mark_extend_count                timber_mark.mark_extend_count%TYPE;
  v_mark_extend_rsn_cd               timber_mark.mark_extend_rsn_cd%TYPE;
  v_quota_type_code                  timber_mark.quota_type_code%TYPE;
  v_lands_region                     timber_mark.lands_region%TYPE;
  v_salvage_type_code                timber_mark.salvage_type_code%TYPE;
  v_catastrophic_ind                 timber_mark.catastrophic_ind%TYPE;
  v_crown_granted_ind                timber_mark.crown_granted_ind%TYPE;
  v_cruise_based_ind                 timber_mark.cruise_based_ind%TYPE;
  v_deciduous_ind                    timber_mark.deciduous_ind%TYPE;
  v_bcaa_folio_number                timber_mark.bcaa_folio_number%TYPE;
  v_revision_count                   timber_mark.revision_count%TYPE;
BEGIN
  IF UPDATING THEN
    --If HVA is for FSJ, then determine HVA having earliest expiry date.
    IF :NEW.harvest_type_code = 'F' THEN
      fta_get_fsj_hva_trigger_info(:NEW.hva_skey
                                 , v_earliest_expiry_hva_skey
                                 , v_forest_district
                                 , v_cascade_split_code
                                 , v_geographic_distrct
                                 , v_mark_status_st
                                 , v_tenure_term
                                 , v_mark_status_date
                                 , v_mark_issue_date
                                 , v_mark_expiry_date
                                 , v_district_admn_zone
                                 , v_mark_extend_date
                                 , v_mark_extend_count
                                 , v_mark_extend_rsn_cd
                                 , v_quota_type_code
                                 , v_lands_region
                                 , v_salvage_type_code
                                 , v_catastrophic_ind
                                 , v_crown_granted_ind
                                 , v_cruise_based_ind
                                 , v_deciduous_ind
                                 , v_bcaa_folio_number
                                 , v_revision_count);

     --If returned hva is not same as current hva then update the mark
     --details with the returned hva information.
      IF :NEW.hva_skey <> v_earliest_expiry_hva_skey AND v_earliest_expiry_hva_skey IS NOT NULL THEN
        UPDATE timber_mark
           SET forest_district = v_forest_district
             , cascade_split_code = v_cascade_split_code
             , geographic_distrct = v_geographic_distrct
             , mark_status_st = v_mark_status_st
             , tenure_term = v_tenure_term
             , mark_status_date = v_mark_status_date
             , mark_issue_date = v_mark_issue_date
             , mark_expiry_date = v_mark_expiry_date
             , district_admn_zone = v_district_admn_zone
             , mark_extend_date = v_mark_extend_date
             , mark_extend_count = NVL(v_mark_extend_count, 0)
             , mark_extend_rsn_cd = v_mark_extend_rsn_cd
             , quota_type_code = v_quota_type_code
             , lands_region = v_lands_region
             , salvage_type_code = v_salvage_type_code
             , catastrophic_ind = v_catastrophic_ind
             , crown_granted_ind = v_crown_granted_ind
             , cruise_based_ind = v_cruise_based_ind
             , deciduous_ind = v_deciduous_ind
             , bcaa_folio_number = :NEW.bcaa_folio_number
             , update_userid = :NEW.update_userid
             , update_timestamp = :NEW.update_timestamp
             , revision_count = revision_count + 1
         WHERE timber_mark IN(SELECT DISTINCT timber_mark
                                FROM harvesting_hauling_xref hhx
                               WHERE hhx.hva_skey = :NEW.hva_skey);
      ELSE
     --If returned hva is same as current hva then just update the mark
     --details with the current hva information.
        UPDATE timber_mark tm
           SET forest_district = :NEW.forest_district
             , cascade_split_code = :NEW.cascade_split_code
             , geographic_distrct = :NEW.geographic_district
             , mark_status_st = :NEW.harvest_auth_status_code
             , tenure_term = :NEW.tenure_term
             , mark_status_date = :NEW.status_date
             , mark_issue_date = :NEW.issue_date
             , mark_expiry_date = :NEW.expiry_date
             , district_admn_zone = :NEW.district_admn_zone
             , mark_extend_date = :NEW.extend_date
             , mark_extend_count = NVL(:NEW.extend_count, 0)
             , mark_extend_rsn_cd = :NEW.harvest_auth_extend_reas_code
             , quota_type_code = :NEW.quota_type_code
             , lands_region = :NEW.crown_lands_region_code
             , salvage_type_code = :NEW.salvage_type_code
             , catastrophic_ind = :NEW.catastrophic_ind
             , crown_granted_ind = :NEW.crown_granted_ind
             , cruise_based_ind = :NEW.cruise_based_ind
             , deciduous_ind = :NEW.deciduous_ind
             , bcaa_folio_number = :NEW.bcaa_folio_number
             , update_userid = :NEW.update_userid
             , update_timestamp = :NEW.update_timestamp
             , revision_count = revision_count + 1
         WHERE tm.timber_mark IN(SELECT hhx.timber_mark
                                   FROM harvesting_hauling_xref hhx
                                  WHERE hhx.hva_skey = :NEW.hva_skey);
      END IF;
    ELSE
    --Update is for non-FSJ harvesting authority.
      UPDATE timber_mark tm
         SET forest_district = :NEW.forest_district
           , cascade_split_code = :NEW.cascade_split_code
           , geographic_distrct = :NEW.geographic_district
           , mark_status_st = :NEW.harvest_auth_status_code
           , tenure_term = :NEW.tenure_term
           , mark_status_date = :NEW.status_date
           , mark_issue_date = :NEW.issue_date
           , mark_expiry_date = :NEW.expiry_date
           , district_admn_zone = :NEW.district_admn_zone
           , mark_extend_date = :NEW.extend_date
           , mark_extend_count = NVL(:NEW.extend_count, 0)
           , mark_extend_rsn_cd = :NEW.harvest_auth_extend_reas_code
           , quota_type_code = :NEW.quota_type_code
           , lands_region = :NEW.crown_lands_region_code
           , salvage_type_code = :NEW.salvage_type_code
           , catastrophic_ind = :NEW.catastrophic_ind
           , crown_granted_ind = :NEW.crown_granted_ind
           , cruise_based_ind = :NEW.cruise_based_ind
           , deciduous_ind = :NEW.deciduous_ind
           , bcaa_folio_number = :NEW.bcaa_folio_number
           , update_userid = :NEW.update_userid
           , update_timestamp = :NEW.update_timestamp
           , revision_count = revision_count + 1
       WHERE tm.timber_mark IN(SELECT hhx.timber_mark
                                 FROM harvesting_hauling_xref hhx
                                WHERE hhx.hva_skey = :NEW.hva_skey);
    END IF;
  END IF;
END fta_sync_hva_tm;



/
ALTER TRIGGER "THE"."FTA_SYNC_HVA_TM" ENABLE;

  CREATE OR REPLACE EDITIONABLE TRIGGER "THE"."TRG_ADS_HARVEST1" 
AFTER UPDATE  OR  DELETE OR INSERT
ON ads_harvest
REFERENCING NEW AS NEW OLD AS OLD
FOR EACH ROW
DECLARE

  l_table_name      ECAS_AUDIT_DETAIL.TABLE_NAME%TYPE;
  l_column_name     ECAS_AUDIT_DETAIL.COLUMN_NAME%TYPE;
  l_old_value       ECAS_AUDIT_DETAIL.OLD_VALUE%TYPE;
  l_new_value       ECAS_AUDIT_DETAIL.NEW_VALUE%TYPE;
  l_ecas_id         ECAS_AUDIT_DETAIL.ECAS_ID%TYPE;
  l_audit_event_id  ECAS_AUDIT_DETAIL.AUDIT_EVENT_ID%TYPE;
  l_userid          ECAS_AUDIT_DETAIL.ENTRY_USERID%TYPE;
  l_business_id     ECAS_AUDIT_DETAIL.BUSINESS_IDENTIFIER%TYPE;

  l_error_message VARCHAR2(1500);

BEGIN

  l_table_name := 'ADS_HARVEST';
  l_audit_event_id := NULL;

  IF UPDATING OR INSERTING THEN
    l_userid := :NEW.update_userid;
    l_ecas_id := :NEW.ecas_id;
	l_business_id := 'Harvest Method: '||:NEW.APPRAISAL_HARVEST_METHOD_CODE||' Harvest System: '||:NEW.HARVEST_SYSTEM_CODE;

    Pkg_Ecas_Audit.CHECK_EVENT_ID(l_audit_event_id
                                , l_ecas_id
                                , 'UPD'
                                , l_userid
                                , SYSDATE
                                , l_error_message);

    IF l_audit_event_id IS NOT NULL THEN

      IF NVL(:NEW.APPRAISAL_HARVEST_METHOD_CODE,0) != NVL(:OLD.APPRAISAL_HARVEST_METHOD_CODE,0) THEN
        l_column_name := 'APPRAISAL_HARVEST_METHOD_CODE';
        l_old_value := :OLD.APPRAISAL_HARVEST_METHOD_CODE;
        l_new_value := :NEW.APPRAISAL_HARVEST_METHOD_CODE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.HARVEST_SYSTEM_CODE,0) != NVL(:OLD.HARVEST_SYSTEM_CODE,0) THEN
        l_column_name := 'HARVEST_SYSTEM_CODE';
        l_old_value := :OLD.HARVEST_SYSTEM_CODE;
        l_new_value := :NEW.HARVEST_SYSTEM_CODE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.HARVEST_VOLUME,0) != NVL(:OLD.HARVEST_VOLUME,0) THEN
        l_column_name := 'HARVEST_VOLUME';
        l_old_value := :OLD.HARVEST_VOLUME;
        l_new_value := :NEW.HARVEST_VOLUME;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.BGC_ZONE_CODE, 0) != NVL(:OLD.BGC_ZONE_CODE, 0) THEN
        l_column_name := 'BGC_ZONE_CODE';
        l_old_value := :OLD.BGC_ZONE_CODE;
        l_new_value := :NEW.BGC_ZONE_CODE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.NET_VOLUME_PER_TREE, 0) != NVL(:OLD.NET_VOLUME_PER_TREE, 0) THEN
        l_column_name := 'NET_VOLUME_PER_TREE';
        l_old_value := :OLD.NET_VOLUME_PER_TREE;
        l_new_value := :NEW.NET_VOLUME_PER_TREE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.VOLUME_PER_HECTARE, 0) != NVL(:OLD.VOLUME_PER_HECTARE, 0) THEN
        l_column_name := 'VOLUME_PER_HECTARE';
        l_old_value := :OLD.VOLUME_PER_HECTARE;
        l_new_value := :NEW.VOLUME_PER_HECTARE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.STAND_DEFECT_PCT, 0) != NVL(:OLD.STAND_DEFECT_PCT, 0) THEN
        l_column_name := 'STAND_DEFECT_PCT';
        l_old_value := :OLD.STAND_DEFECT_PCT;
        l_new_value := :NEW.STAND_DEFECT_PCT;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.PARTIAL_CUT_PCT, 0) != NVL(:OLD.PARTIAL_CUT_PCT, 0) THEN
        l_column_name := 'PARTIAL_CUT_PCT';
        l_old_value := :OLD.PARTIAL_CUT_PCT;
        l_new_value := :NEW.PARTIAL_CUT_PCT;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.AVERAGE_SLOPE_PCT, 0) != NVL(:OLD.AVERAGE_SLOPE_PCT, 0) THEN
        l_column_name := 'AVERAGE_SLOPE_PCT';
        l_old_value := :OLD.AVERAGE_SLOPE_PCT;
        l_new_value := :NEW.AVERAGE_SLOPE_PCT;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.BLOWDOWN_PCT, 0) != NVL(:OLD.BLOWDOWN_PCT, 0) THEN
        l_column_name := 'BLOWDOWN_PCT';
        l_old_value := :OLD.BLOWDOWN_PCT;
        l_new_value := :NEW.BLOWDOWN_PCT;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.HEAVY_FIRE_DAMAGE_PCT, 0) != NVL(:OLD.HEAVY_FIRE_DAMAGE_PCT, 0) THEN
        l_column_name := 'HEAVY_FIRE_DAMAGE_PCT';
        l_old_value := :OLD.HEAVY_FIRE_DAMAGE_PCT;
        l_new_value := :NEW.HEAVY_FIRE_DAMAGE_PCT;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.DEAD_USELESS_SNAGS_PCT, 0) != NVL(:OLD.DEAD_USELESS_SNAGS_PCT, 0) THEN
        l_column_name := 'DEAD_USELESS_SNAGS_PCT';
        l_old_value := :OLD.DEAD_USELESS_SNAGS_PCT;
        l_new_value := :NEW.DEAD_USELESS_SNAGS_PCT;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.SKYLINE_YARDING_DISTANCE, 0) != NVL(:OLD.SKYLINE_YARDING_DISTANCE, 0) THEN
        l_column_name := 'SKYLINE_YARDING_DISTANCE';
        l_old_value := :OLD.SKYLINE_YARDING_DISTANCE;
        l_new_value := :NEW.SKYLINE_YARDING_DISTANCE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.HELICOPTER_YARDING_DISTANCE, 0) != NVL(:OLD.HELICOPTER_YARDING_DISTANCE, 0) THEN
        l_column_name := 'HELICOPTER_YARDING_DISTANCE';
        l_old_value := :OLD.HELICOPTER_YARDING_DISTANCE;
        l_new_value := :NEW.HELICOPTER_YARDING_DISTANCE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.UPHILL_YARDING_IND, 0) != NVL(:OLD.UPHILL_YARDING_IND, 0) THEN
        l_column_name := 'UPHILL_YARDING_IND';
        l_old_value := :OLD.UPHILL_YARDING_IND;
        l_new_value := :NEW.UPHILL_YARDING_IND;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

    END IF; --l_audit_event_id is not null

  ELSIF DELETING THEN
    l_ecas_id := :OLD.ecas_id;
	l_business_id := 'Harvest Method: '||:OLD.APPRAISAL_HARVEST_METHOD_CODE||' Harvest System: '||:OLD.HARVEST_SYSTEM_CODE;
    Pkg_Ecas_Audit.GET_USERID(l_userid
                            , 'DEL'
                            , l_ecas_id
                            , SYSDATE
                            , l_error_message);

    Pkg_Ecas_Audit.CHECK_EVENT_ID(l_audit_event_id
                                , l_ecas_id
                                , 'DEL'
                                , l_userid
                                , SYSDATE
                                , l_error_message);

    IF l_audit_event_id IS NOT NULL THEN

      l_column_name := 'APPRAISAL_HARVEST_METHOD_CODE';
      l_old_value := :OLD.APPRAISAL_HARVEST_METHOD_CODE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'HARVEST_SYSTEM_CODE';
      l_old_value := :OLD.HARVEST_SYSTEM_CODE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'HARVEST_VOLUME';
      l_old_value := :OLD.HARVEST_VOLUME;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'BGC_ZONE_CODE';
      l_old_value := :OLD.BGC_ZONE_CODE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'NET_VOLUME_PER_TREE';
      l_old_value := :OLD.NET_VOLUME_PER_TREE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'VOLUME_PER_HECTARE';
      l_old_value := :OLD.VOLUME_PER_HECTARE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'STAND_DEFECT_PCT';
      l_old_value := :OLD.STAND_DEFECT_PCT;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'PARTIAL_CUT_PCT';
      l_old_value := :OLD.PARTIAL_CUT_PCT;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'AVERAGE_SLOPE_PCT';
      l_old_value := :OLD.AVERAGE_SLOPE_PCT;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'BLOWDOWN_PCT';
      l_old_value := :OLD.BLOWDOWN_PCT;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'HEAVY_FIRE_DAMAGE_PCT';
      l_old_value := :OLD.HEAVY_FIRE_DAMAGE_PCT;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'DEAD_USELESS_SNAGS_PCT';
      l_old_value := :OLD.DEAD_USELESS_SNAGS_PCT;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'SKYLINE_YARDING_DISTANCE';
      l_old_value := :OLD.SKYLINE_YARDING_DISTANCE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'HELICOPTER_YARDING_DISTANCE';
      l_old_value := :OLD.HELICOPTER_YARDING_DISTANCE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'UPHILL_YARDING_IND';
      l_old_value := :OLD.UPHILL_YARDING_IND;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

    END IF; --l_audit_event_id is not null

  END IF; --if updating or deleting

  EXCEPTION
    WHEN TOO_MANY_ROWS THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.record.toomany:TRG_ADS_HARVEST1,'||TRIM(l_table_name)||','||SQLCODE||';';
    WHEN DUP_VAL_ON_INDEX THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.record.duplicate:TRG_ADS_HARVEST1,'||TRIM(l_table_name)||','||SQLCODE||';';
    WHEN NO_DATA_FOUND THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.record.invalid:TRG_ADS_HARVEST1,'||TRIM(l_table_name)||';';
    WHEN OTHERS THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.unexpected:TRG_ADS_HARVEST1,'||SQLCODE||','||SQLERRM||';';
    RAISE;
END TRG_ADS_HARVEST1;



/
ALTER TRIGGER "THE"."TRG_ADS_HARVEST1" ENABLE;

  CREATE OR REPLACE EDITIONABLE TRIGGER "THE"."ADS_HARVEST_SPECIES1" 
AFTER DELETE OR UPDATE OR INSERT
ON THE.ADS_HARVEST_SPECIES
REFERENCING NEW AS NEW OLD AS OLD
FOR EACH ROW
DECLARE

  l_table_name      ECAS_AUDIT_DETAIL.TABLE_NAME%TYPE;
  l_column_name     ECAS_AUDIT_DETAIL.COLUMN_NAME%TYPE;
  l_old_value       ECAS_AUDIT_DETAIL.OLD_VALUE%TYPE;
  l_new_value       ECAS_AUDIT_DETAIL.NEW_VALUE%TYPE;
  l_ecas_id         ECAS_AUDIT_DETAIL.ECAS_ID%TYPE;
  l_audit_event_id  ECAS_AUDIT_DETAIL.AUDIT_EVENT_ID%TYPE;
  l_userid          ECAS_AUDIT_DETAIL.ENTRY_USERID%TYPE;
  l_business_id     ECAS_AUDIT_DETAIL.BUSINESS_IDENTIFIER%TYPE;

  l_error_message VARCHAR2(1500);

BEGIN

  l_table_name := 'ADS_HARVEST_SPECIES';
  l_audit_event_id := NULL;

  IF UPDATING OR INSERTING THEN
    l_userid := :NEW.update_userid;
    l_ecas_id := :NEW.ecas_id;
    l_business_id := 'Harv Mthd: '||:NEW.APPRAISAL_HARVEST_METHOD_CODE||' Harv Sys: '||:NEW.HARVEST_SYSTEM_CODE||' Species: '||:NEW.HDBS_TREE_SPECIES;

    Pkg_Ecas_Audit.CHECK_EVENT_ID(l_audit_event_id
                                , l_ecas_id
                                , 'UPD'
                                , l_userid
                                , SYSDATE
                                , l_error_message);

    IF l_audit_event_id IS NOT NULL THEN

      IF NVL(:NEW.APPRAISAL_HARVEST_METHOD_CODE, 0) != NVL(:OLD.APPRAISAL_HARVEST_METHOD_CODE, 0) THEN
        l_column_name := 'APPRAISAL_HARVEST_METHOD_CODE';
        l_old_value := :OLD.APPRAISAL_HARVEST_METHOD_CODE;
        l_new_value := :NEW.APPRAISAL_HARVEST_METHOD_CODE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      IF NVL(:NEW.HARVEST_SYSTEM_CODE, 0) != NVL(:OLD.HARVEST_SYSTEM_CODE, 0) THEN
        l_column_name := 'HARVEST_SYSTEM_CODE';
        l_old_value := :OLD.HARVEST_SYSTEM_CODE;
        l_new_value := :NEW.HARVEST_SYSTEM_CODE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      IF NVL(:NEW.HDBS_TREE_SPECIES, 0) != NVL(:OLD.HDBS_TREE_SPECIES, 0) THEN
        l_column_name := 'HDBS_TREE_SPECIES';
        l_old_value := :OLD.HDBS_TREE_SPECIES;
        l_new_value := :NEW.HDBS_TREE_SPECIES;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      IF NVL(:NEW.SPECIES_VOLUME, 0) != NVL(:OLD.SPECIES_VOLUME, 0) THEN
        l_column_name := 'SPECIES_VOLUME';
        l_old_value := :OLD.SPECIES_VOLUME;
        l_new_value := :NEW.SPECIES_VOLUME;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

    END IF;

  ELSIF DELETING THEN
    l_ecas_id := :OLD.ecas_id;
    l_business_id := 'Harv Mthd: '||:OLD.APPRAISAL_HARVEST_METHOD_CODE||' Harv Sys: '||:OLD.HARVEST_SYSTEM_CODE||' Species: '||:OLD.HDBS_TREE_SPECIES;
    Pkg_Ecas_Audit.GET_USERID(l_userid
                            , 'DEL'
                            , l_ecas_id
                            , SYSDATE
                            , l_error_message);

    Pkg_Ecas_Audit.CHECK_EVENT_ID(l_audit_event_id
                                , l_ecas_id
                                , 'DEL'
                                , l_userid
                                , SYSDATE
                                , l_error_message);

    IF l_audit_event_id IS NOT NULL THEN

      l_column_name := 'APPRAISAL_HARVEST_METHOD_CODE';
      l_old_value := :OLD.APPRAISAL_HARVEST_METHOD_CODE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'HARVEST_SYSTEM_CODE';
      l_old_value := :OLD.HARVEST_SYSTEM_CODE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'HDBS_TREE_SPECIES';
      l_old_value := :OLD.HDBS_TREE_SPECIES;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'SPECIES_VOLUME';
      l_old_value := :OLD.SPECIES_VOLUME;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

    END IF;

  END IF; --if updating or deleting


  EXCEPTION
    WHEN TOO_MANY_ROWS THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.record.toomany:TRG_ADS_HARVEST1,'||TRIM(l_table_name)||','||SQLCODE||';';
    WHEN DUP_VAL_ON_INDEX THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.record.duplicate:TRG_ADS_HARVEST1,'||TRIM(l_table_name)||','||SQLCODE||';';
    WHEN NO_DATA_FOUND THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.record.invalid:TRG_ADS_HARVEST1,'||TRIM(l_table_name)||';';
    WHEN OTHERS THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.unexpected:TRG_ADS_HARVEST1,'||SQLCODE||','||SQLERRM||';';
    RAISE;

END ;






/
ALTER TRIGGER "THE"."ADS_HARVEST_SPECIES1" ENABLE;

  CREATE OR REPLACE EDITIONABLE TRIGGER "THE"."TRG_ADS_SPECIES_VOLUME1" 
 AFTER UPDATE OR
	   DELETE OR INSERT
   ON ADS_SPECIES_VOLUME

FOR EACH ROW

DECLARE

  l_table_name      ECAS_AUDIT_DETAIL.TABLE_NAME%TYPE;
  l_column_name     ECAS_AUDIT_DETAIL.COLUMN_NAME%TYPE;
  l_old_value       ECAS_AUDIT_DETAIL.OLD_VALUE%TYPE;
  l_new_value       ECAS_AUDIT_DETAIL.NEW_VALUE%TYPE;
  l_ecas_id         ECAS_AUDIT_DETAIL.ECAS_ID%TYPE;
  l_audit_event_id  ECAS_AUDIT_DETAIL.AUDIT_EVENT_ID%TYPE;
  l_userid          ECAS_AUDIT_DETAIL.ENTRY_USERID%TYPE;
  l_business_id     ECAS_AUDIT_DETAIL.BUSINESS_IDENTIFIER%TYPE;

  l_error_message VARCHAR2(1500);

BEGIN

  l_table_name := 'ADS_SPECIES_VOLUME';
  l_audit_event_id := NULL;

  IF UPDATING OR INSERTING THEN
    l_userid := :NEW.update_userid;
    l_ecas_id := :NEW.ecas_id;
    l_business_id := 'Species: '||:NEW.HDBS_TREE_SPECIES;

    Pkg_Ecas_Audit.CHECK_EVENT_ID(l_audit_event_id
                                , l_ecas_id
                                , 'UPD'
                                , l_userid
                                , SYSDATE
                                , l_error_message);

    IF l_audit_event_id IS NOT NULL THEN

      IF NVL(:NEW.HDBS_TREE_SPECIES,0) != NVL(:OLD.HDBS_TREE_SPECIES,0) THEN
        l_column_name := 'HDBS_TREE_SPECIES';
        l_old_value := :OLD.HDBS_TREE_SPECIES;
        l_new_value := :NEW.HDBS_TREE_SPECIES;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.SPECIES_VOLUME,0) != NVL(:OLD.SPECIES_VOLUME,0) THEN
        l_column_name := 'SPECIES_VOLUME';
        l_old_value := :OLD.SPECIES_VOLUME;
        l_new_value := :NEW.SPECIES_VOLUME;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.GRADE_SOURCE, 0) != NVL(:OLD.GRADE_SOURCE, 0) THEN
        l_column_name := 'GRADE_SOURCE';
        l_old_value := :OLD.GRADE_SOURCE;
        l_new_value := :NEW.GRADE_SOURCE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.DECAY_PCT, 0) != NVL(:OLD.DECAY_PCT, 0) THEN
        l_column_name := 'DECAY_PCT';
        l_old_value := :OLD.DECAY_PCT;
        l_new_value := :NEW.DECAY_PCT;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.STUD_PCT, 0) != NVL(:OLD.STUD_PCT, 0) THEN
        l_column_name := 'STUD_PCT';
        l_old_value := :OLD.STUD_PCT;
        l_new_value := :NEW.STUD_PCT;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.BURN_PCT, 0) != NVL(:OLD.BURN_PCT, 0) THEN
        l_column_name := 'BURN_PCT';
        l_old_value := :OLD.BURN_PCT;
        l_new_value := :NEW.BURN_PCT;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.GRADE_PCT, 0) != NVL(:OLD.GRADE_PCT, 0) THEN
        l_column_name := 'GRADE_PCT';
        l_old_value := :OLD.GRADE_PCT;
        l_new_value := :NEW.GRADE_PCT;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.LUMBER_RECOVERY_FACTOR, 0) != NVL(:OLD.LUMBER_RECOVERY_FACTOR, 0) THEN
        l_column_name := 'LUMBER_RECOVERY_FACTOR';
        l_old_value := :OLD.LUMBER_RECOVERY_FACTOR;
        l_new_value := :NEW.LUMBER_RECOVERY_FACTOR;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);

      END IF;

    END IF; --l_audit_event_id is not null

  ELSIF DELETING THEN
    l_ecas_id := :OLD.ecas_id;
    l_business_id := 'Species: '||:OLD.HDBS_TREE_SPECIES;
    Pkg_Ecas_Audit.GET_USERID(l_userid
                            , 'DEL'
                            , l_ecas_id
                            , SYSDATE
                            , l_error_message);

    Pkg_Ecas_Audit.CHECK_EVENT_ID(l_audit_event_id
                                , l_ecas_id
                                , 'DEL'
                                , l_userid
                                , SYSDATE
                                , l_error_message);

    IF l_audit_event_id IS NOT NULL THEN

      l_column_name := 'HDBS_TREE_SPECIES';
      l_old_value := :OLD.HDBS_TREE_SPECIES;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'SPECIES_VOLUME';
      l_old_value := :OLD.SPECIES_VOLUME;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'GRADE_SOURCE';
      l_old_value := :OLD.GRADE_SOURCE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'DECAY_PCT';
      l_old_value := :OLD.DECAY_PCT;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'STUD_PCT';
      l_old_value := :OLD.STUD_PCT;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'BURN_PCT';
      l_old_value := :OLD.BURN_PCT;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'GRADE_PCT';
      l_old_value := :OLD.GRADE_PCT;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'LUMBER_RECOVERY_FACTOR';
      l_old_value := :OLD.LUMBER_RECOVERY_FACTOR;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

    END IF; --l_audit_event_id is not null

  END IF; --if updating or deleting

  EXCEPTION
    WHEN TOO_MANY_ROWS THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.record.toomany:TRG_ADS_SPECIES_VOLUME1,'||TRIM(l_table_name)||','||SQLCODE||';';
    WHEN DUP_VAL_ON_INDEX THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.record.duplicate:TRG_ADS_SPECIES_VOLUME1,'||TRIM(l_table_name)||','||SQLCODE||';';
    WHEN NO_DATA_FOUND THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.record.invalid:TRG_ADS_SPECIES_VOLUME1,'||TRIM(l_table_name)||';';
    WHEN OTHERS THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.unexpected:TRG_ADS_SPECIES_VOLUME1,'||SQLCODE||','||SQLERRM||';';
    RAISE;
END TRG_ADS_SPECIES_VOLUME1;




/
ALTER TRIGGER "THE"."TRG_ADS_SPECIES_VOLUME1" ENABLE;

  CREATE OR REPLACE EDITIONABLE TRIGGER "THE"."TRG_ADS_SPECIES_GRADE1" 
 AFTER UPDATE OR
	   DELETE OR INSERT
   ON ADS_SPECIES_GRADE

FOR EACH ROW

DECLARE

  l_table_name      ECAS_AUDIT_DETAIL.TABLE_NAME%TYPE;
  l_column_name     ECAS_AUDIT_DETAIL.COLUMN_NAME%TYPE;
  l_old_value       ECAS_AUDIT_DETAIL.OLD_VALUE%TYPE;
  l_new_value       ECAS_AUDIT_DETAIL.NEW_VALUE%TYPE;
  l_ecas_id         ECAS_AUDIT_DETAIL.ECAS_ID%TYPE;
  l_audit_event_id  ECAS_AUDIT_DETAIL.AUDIT_EVENT_ID%TYPE;
  l_userid          ECAS_AUDIT_DETAIL.ENTRY_USERID%TYPE;
  l_business_id     ECAS_AUDIT_DETAIL.BUSINESS_IDENTIFIER%TYPE;

  l_error_message VARCHAR2(1500);

BEGIN

  l_table_name := 'ADS_SPECIES_GRADE';
  l_audit_event_id := NULL;

  IF UPDATING OR INSERTING THEN
    l_userid := :NEW.update_userid;
    l_ecas_id := :NEW.ecas_id;
    l_business_id := 'Species: '||:NEW.HDBS_TREE_SPECIES||' Grade '||:NEW.SCALE_GRADE_CODE;

    Pkg_Ecas_Audit.CHECK_EVENT_ID(l_audit_event_id
                                , l_ecas_id
                                , 'UPD'
                                , l_userid
                                , SYSDATE
                                , l_error_message);

    IF l_audit_event_id IS NOT NULL THEN

      IF NVL(:NEW.HDBS_TREE_SPECIES,0) != NVL(:OLD.HDBS_TREE_SPECIES,0) THEN
        l_column_name := 'HDBS_TREE_SPECIES';
        l_old_value := :OLD.HDBS_TREE_SPECIES;
        l_new_value := :NEW.HDBS_TREE_SPECIES;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.SCALE_GRADE_CODE,0) != NVL(:OLD.SCALE_GRADE_CODE,0) THEN
        l_column_name := 'SCALE_GRADE_CODE';
        l_old_value := :OLD.SCALE_GRADE_CODE;
        l_new_value := :NEW.SCALE_GRADE_CODE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.GRADE_PCT,0) != NVL(:OLD.GRADE_PCT,0) THEN
        l_column_name := 'GRADE_PCT';
        l_old_value := :OLD.GRADE_PCT;
        l_new_value := :NEW.GRADE_PCT;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);

      END IF;

    END IF; --l_audit_event_id is not null

  ELSIF DELETING THEN
    l_ecas_id := :OLD.ecas_id;
    l_business_id := 'Species: '||:OLD.HDBS_TREE_SPECIES||' Grade '||:OLD.SCALE_GRADE_CODE;
    Pkg_Ecas_Audit.GET_USERID(l_userid
                            , 'DEL'
                            , l_ecas_id
                            , SYSDATE
                            , l_error_message);

    Pkg_Ecas_Audit.CHECK_EVENT_ID(l_audit_event_id
                                , l_ecas_id
                                , 'DEL'
                                , l_userid
                                , SYSDATE
                                , l_error_message);

    IF l_audit_event_id IS NOT NULL THEN

      l_column_name := 'HDBS_TREE_SPECIES';
      l_old_value := :OLD.HDBS_TREE_SPECIES;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'SCALE_GRADE_CODE';
      l_old_value := :OLD.SCALE_GRADE_CODE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'GRADE_PCT';
      l_old_value := :OLD.GRADE_PCT;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

    END IF; --l_audit_event_id is not null

  END IF; --if updating or deleting

  EXCEPTION
    WHEN TOO_MANY_ROWS THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.record.toomany:TRG_ADS_SPECIES_GRADE1,'||TRIM(l_table_name)||','||SQLCODE||';';
    WHEN DUP_VAL_ON_INDEX THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.record.duplicate:TRG_ADS_SPECIES_GRADE1,'||TRIM(l_table_name)||','||SQLCODE||';';
    WHEN NO_DATA_FOUND THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.record.invalid:TRG_ADS_SPECIES_GRADE1,'||TRIM(l_table_name)||';';
    WHEN OTHERS THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.unexpected:TRG_ADS_SPECIES_GRADE1,'||SQLCODE||','||SQLERRM||';';
    RAISE;
END TRG_ADS_SPECIES_GRADE1;




/
ALTER TRIGGER "THE"."TRG_ADS_SPECIES_GRADE1" ENABLE;

  CREATE OR REPLACE EDITIONABLE TRIGGER "THE"."TRG_ADS_BCTS_LEVY_COST1" 
 AFTER UPDATE OR
	   DELETE OR INSERT
   ON ADS_BCTS_LEVY_COST

FOR EACH ROW

DECLARE

  l_table_name      ECAS_AUDIT_DETAIL.TABLE_NAME%TYPE;
  l_column_name     ECAS_AUDIT_DETAIL.COLUMN_NAME%TYPE;
  l_old_value       ECAS_AUDIT_DETAIL.OLD_VALUE%TYPE;
  l_new_value       ECAS_AUDIT_DETAIL.NEW_VALUE%TYPE;
  l_ecas_id         ECAS_AUDIT_DETAIL.ECAS_ID%TYPE;
  l_audit_event_id  ECAS_AUDIT_DETAIL.AUDIT_EVENT_ID%TYPE;
  l_userid          ECAS_AUDIT_DETAIL.ENTRY_USERID%TYPE;
  l_business_id     ECAS_AUDIT_DETAIL.BUSINESS_IDENTIFIER%TYPE;

  l_error_message VARCHAR2(1500);

BEGIN

  l_table_name := 'ADS_BCTS_LEVY_COST';
  l_audit_event_id := NULL;

  IF UPDATING OR INSERTING THEN
    l_ecas_id := :NEW.ecas_id;
    l_userid := :NEW.update_userid;
	l_business_id := 'Log Group: '||:NEW.INTERIOR_LOG_GROUP_CODE;

    Pkg_Ecas_Audit.CHECK_EVENT_ID(l_audit_event_id
                                , l_ecas_id
                                , 'UPD'
                                , l_userid
                                , SYSDATE
                                , l_error_message);

    IF l_audit_event_id IS NOT NULL THEN

      IF NVL(:NEW.INTERIOR_LOG_GROUP_CODE,0) != NVL(:OLD.INTERIOR_LOG_GROUP_CODE,0) THEN
        l_column_name := 'INTERIOR_LOG_GROUP_CODE';
        l_old_value := :OLD.INTERIOR_LOG_GROUP_CODE;
        l_new_value := :NEW.INTERIOR_LOG_GROUP_CODE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.DEVELOPMENT_LEVY, 0) != NVL(:OLD.DEVELOPMENT_LEVY, 0) THEN
        l_column_name := 'DEVELOPMENT_LEVY';
        l_old_value := :OLD.DEVELOPMENT_LEVY;
        l_new_value := :NEW.DEVELOPMENT_LEVY;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.SILVICULTURE_LEVY, 0) != NVL(:OLD.SILVICULTURE_LEVY, 0) THEN
        l_column_name := 'SILVICULTURE_LEVY';
        l_old_value := :OLD.SILVICULTURE_LEVY;
        l_new_value := :NEW.SILVICULTURE_LEVY;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.BONUS_BID_AMOUNT, 0) != NVL(:OLD.BONUS_BID_AMOUNT, 0) THEN
        l_column_name := 'BONUS_BID_AMOUNT';
        l_old_value := :OLD.BONUS_BID_AMOUNT;
        l_new_value := :NEW.BONUS_BID_AMOUNT;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

    END IF; --l_audit_event_id is not null

  ELSIF DELETING THEN
    l_ecas_id := :OLD.ecas_id;
	l_business_id := 'Log Group: '||:OLD.INTERIOR_LOG_GROUP_CODE;
    Pkg_Ecas_Audit.GET_USERID(l_userid
                            , 'DEL'
                            , l_ecas_id
                            , SYSDATE
                            , l_error_message);

    Pkg_Ecas_Audit.CHECK_EVENT_ID(l_audit_event_id
                                , l_ecas_id
                                , 'DEL'
                                , l_userid
                                , SYSDATE
                                , l_error_message);

    IF l_audit_event_id IS NOT NULL THEN

      l_column_name := 'INTERIOR_LOG_GROUP_CODE';
      l_old_value := :OLD.INTERIOR_LOG_GROUP_CODE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'DEVELOPMENT_LEVY';
      l_old_value := :OLD.DEVELOPMENT_LEVY;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'SILVICULTURE_LEVY';
      l_old_value := :OLD.SILVICULTURE_LEVY;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'BONUS_BID_AMOUNT';
      l_old_value := :OLD.BONUS_BID_AMOUNT;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

    END IF; --l_audit_event_id is not null

  END IF; --if updating or deleting

  EXCEPTION
    WHEN TOO_MANY_ROWS THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.record.toomany:TRG_ADS_BCTS_LEVY_COST1,'||TRIM(l_table_name)||','||SQLCODE||';';
    WHEN DUP_VAL_ON_INDEX THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.record.duplicate:TRG_ADS_BCTS_LEVY_COST1,'||TRIM(l_table_name)||','||SQLCODE||';';
    WHEN NO_DATA_FOUND THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.record.invalid:TRG_ADS_BCTS_LEVY_COST1,'||TRIM(l_table_name)||';';
    WHEN OTHERS THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.unexpected:TRG_ADS_BCTS_LEVY_COST1,'||SQLCODE||','||SQLERRM||';';
    RAISE;
END TRG_ADS_BCTS_LEVY_COST1;




/
ALTER TRIGGER "THE"."TRG_ADS_BCTS_LEVY_COST1" ENABLE;

  CREATE OR REPLACE EDITIONABLE TRIGGER "THE"."TRG_ADS_DETAILED_ENG_COST1" 
 AFTER UPDATE OR
	   DELETE OR INSERT
   ON ADS_DETAILED_ENGINEERING_COST

FOR EACH ROW

DECLARE

  l_table_name      ECAS_AUDIT_DETAIL.TABLE_NAME%TYPE;
  l_column_name     ECAS_AUDIT_DETAIL.COLUMN_NAME%TYPE;
  l_old_value       ECAS_AUDIT_DETAIL.OLD_VALUE%TYPE;
  l_new_value       ECAS_AUDIT_DETAIL.NEW_VALUE%TYPE;
  l_ecas_id         ECAS_AUDIT_DETAIL.ECAS_ID%TYPE;
  l_audit_event_id  ECAS_AUDIT_DETAIL.AUDIT_EVENT_ID%TYPE;
  l_userid          ECAS_AUDIT_DETAIL.ENTRY_USERID%TYPE;
  l_business_id     ECAS_AUDIT_DETAIL.BUSINESS_IDENTIFIER%TYPE;

  l_error_message VARCHAR2(1500);

BEGIN

  l_table_name := 'ADS_DETAILED_ENGINEERING_COST';
  l_audit_event_id := NULL;

  IF UPDATING OR INSERTING THEN
    l_userid := :NEW.update_userid;
    l_ecas_id := :NEW.ecas_id;
	l_business_id := 'Est Id:'||:NEW.COST_ESTIMATE_ID||' Devel Type: '||:NEW.ROAD_DEVELOPMENT_TYPE_CODE||' Year: '||:NEW.APPRAISAL_YEAR;

    Pkg_Ecas_Audit.CHECK_EVENT_ID(l_audit_event_id
                                , l_ecas_id
                                , 'UPD'
                                , l_userid
                                , SYSDATE
                                , l_error_message);

    IF l_audit_event_id IS NOT NULL THEN

        IF NVL(:NEW.COST_ESTIMATE_ID,0) != NVL(:OLD.COST_ESTIMATE_ID,0) THEN
          l_column_name := 'COST_ESTIMATE_ID';
          l_old_value := :OLD.COST_ESTIMATE_ID;
          l_new_value := :NEW.COST_ESTIMATE_ID;
          Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                        , l_audit_event_id
                                        , l_table_name
                                        , l_column_name
                                        , l_old_value
                                        , l_new_value
                                        , l_userid
									    , l_business_id
                                        , l_error_message);

        END IF;

        IF NVL(:NEW.ROAD_DEVELOPMENT_TYPE_CODE,0) != NVL(:OLD.ROAD_DEVELOPMENT_TYPE_CODE,0) THEN
          l_column_name := 'ROAD_DEVELOPMENT_TYPE_CODE';
          l_old_value := :OLD.ROAD_DEVELOPMENT_TYPE_CODE;
          l_new_value := :NEW.ROAD_DEVELOPMENT_TYPE_CODE;
          Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                        , l_audit_event_id
                                        , l_table_name
                                        , l_column_name
                                        , l_old_value
                                        , l_new_value
                                        , l_userid
									    , l_business_id
                                        , l_error_message);

        END IF;

        IF NVL(:NEW.APPRAISAL_YEAR,0) != NVL(:OLD.APPRAISAL_YEAR,0) THEN
          l_column_name := 'APPRAISAL_YEAR';
          l_old_value := :OLD.APPRAISAL_YEAR;
          l_new_value := :NEW.APPRAISAL_YEAR;
          Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                        , l_audit_event_id
                                        , l_table_name
                                        , l_column_name
                                        , l_old_value
                                        , l_new_value
                                        , l_userid
									    , l_business_id
                                        , l_error_message);

        END IF;

        IF NVL(:NEW.ROAD_LENGTH, 0) != NVL(:OLD.ROAD_LENGTH, 0) THEN
          l_column_name := 'ROAD_LENGTH';
          l_old_value := :OLD.ROAD_LENGTH;
          l_new_value := :NEW.ROAD_LENGTH;
          Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                        , l_audit_event_id
                                        , l_table_name
                                        , l_column_name
                                        , l_old_value
                                        , l_new_value
                                        , l_userid
									    , l_business_id
                                        , l_error_message);

        END IF;

        IF NVL(:NEW.AMORTIZED_PCT, 0) != NVL(:OLD.AMORTIZED_PCT, 0) THEN
          l_column_name := 'AMORTIZED_PCT';
          l_old_value := :OLD.AMORTIZED_PCT;
          l_new_value := :NEW.AMORTIZED_PCT;
          Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                        , l_audit_event_id
                                        , l_table_name
                                        , l_column_name
                                        , l_old_value
                                        , l_new_value
                                        , l_userid
									    , l_business_id
                                        , l_error_message);

        END IF;

        IF NVL(:NEW.CROWN_PCT,0) != NVL(:OLD.CROWN_PCT,0) THEN
          l_column_name := 'CROWN_PCT';
          l_old_value := :OLD.CROWN_PCT;
          l_new_value := :NEW.CROWN_PCT;
          Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                        , l_audit_event_id
                                        , l_table_name
                                        , l_column_name
                                        , l_old_value
                                        , l_new_value
                                        , l_userid
									    , l_business_id
                                        , l_error_message);

        END IF;

        IF NVL(:NEW.TOTAL_COST, 0) != NVL(:OLD.TOTAL_COST, 0) THEN
          l_column_name := 'TOTAL_COST';
          l_old_value := :OLD.TOTAL_COST;
          l_new_value := :NEW.TOTAL_COST;
          Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                        , l_audit_event_id
                                        , l_table_name
                                        , l_column_name
                                        , l_old_value
                                        , l_new_value
                                        , l_userid
									    , l_business_id
                                        , l_error_message);

        END IF;

        IF NVL(:NEW.COMMENTS, 0) != NVL(:OLD.COMMENTS, 0) THEN
          l_column_name := 'COMMENTS';
          l_old_value := :OLD.COMMENTS;
          l_new_value := :NEW.COMMENTS;
          Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                        , l_audit_event_id
                                        , l_table_name
                                        , l_column_name
                                        , l_old_value
                                        , l_new_value
                                        , l_userid
									    , l_business_id
                                        , l_error_message);

        END IF;

    END IF; --l_audit_event_id is not null

  ELSIF DELETING THEN
    l_ecas_id := :OLD.ecas_id;
	l_business_id := 'Est Id:'||:OLD.COST_ESTIMATE_ID||' Devel Type: '||:OLD.ROAD_DEVELOPMENT_TYPE_CODE||' Year: '||:OLD.APPRAISAL_YEAR;
    Pkg_Ecas_Audit.GET_USERID(l_userid
                            , 'DEL'
                            , l_ecas_id
                            , SYSDATE
                            , l_error_message);

    Pkg_Ecas_Audit.CHECK_EVENT_ID(l_audit_event_id
                                , l_ecas_id
                                , 'DEL'
                                , l_userid
                                , SYSDATE
                                , l_error_message);

    IF l_audit_event_id IS NOT NULL THEN

      l_column_name := 'COST_ESTIMATE_ID';
      l_old_value := :OLD.COST_ESTIMATE_ID;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'ROAD_DEVELOPMENT_TYPE_CODE';
      l_old_value := :OLD.ROAD_DEVELOPMENT_TYPE_CODE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'APPRAISAL_YEAR';
      l_old_value := :OLD.APPRAISAL_YEAR;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'ROAD_LENGTH';
      l_old_value := :OLD.ROAD_LENGTH;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'AMORTIZED_PCT';
      l_old_value := :OLD.AMORTIZED_PCT;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'CROWN_PCT';
      l_old_value := :OLD.CROWN_PCT;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'TOTAL_COST';
      l_old_value := :OLD.TOTAL_COST;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'COMMENTS';
      l_old_value := :OLD.COMMENTS;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

    END IF; --l_audit_event_id is not null

  END IF; --if updating or deleting

  EXCEPTION
    WHEN TOO_MANY_ROWS THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.record.toomany:TRG_ADS_DETAILED_ENG_COST1,'||TRIM(l_table_name)||','||SQLCODE||';';
    WHEN DUP_VAL_ON_INDEX THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.record.duplicate:TRG_ADS_DETAILED_ENG_COST1,'||TRIM(l_table_name)||','||SQLCODE||';';
    WHEN NO_DATA_FOUND THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.record.invalid:TRG_ADS_DETAILED_ENG_COST1,'||TRIM(l_table_name)||';';
    WHEN OTHERS THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.unexpected:TRG_ADS_DETAILED_ENG_COST1,'||SQLCODE||','||SQLERRM||';';
    RAISE;
END TRG_ADS_DETAILED_ENG_COST1;




/
ALTER TRIGGER "THE"."TRG_ADS_DETAILED_ENG_COST1" ENABLE;

  CREATE OR REPLACE EDITIONABLE TRIGGER "THE"."TRG_ADS_ROAD1" 
 AFTER UPDATE OR
	   DELETE OR INSERT
   ON ADS_ROAD

FOR EACH ROW

DECLARE

  l_table_name      ECAS_AUDIT_DETAIL.TABLE_NAME%TYPE;
  l_column_name     ECAS_AUDIT_DETAIL.COLUMN_NAME%TYPE;
  l_old_value       ECAS_AUDIT_DETAIL.OLD_VALUE%TYPE;
  l_new_value       ECAS_AUDIT_DETAIL.NEW_VALUE%TYPE;
  l_ecas_id         ECAS_AUDIT_DETAIL.ECAS_ID%TYPE;
  l_audit_event_id  ECAS_AUDIT_DETAIL.AUDIT_EVENT_ID%TYPE;
  l_userid          ECAS_AUDIT_DETAIL.ENTRY_USERID%TYPE;
  l_business_id     ECAS_AUDIT_DETAIL.BUSINESS_IDENTIFIER%TYPE;

  l_error_message VARCHAR2(1500);

BEGIN

  l_table_name := 'ADS_ROAD';
  l_audit_event_id := NULL;

  IF UPDATING OR INSERTING THEN
    l_userid := :NEW.update_userid;
    l_ecas_id := :NEW.ecas_id;
    l_business_id := 'Road: '||SUBSTR(:NEW.ROAD_NAME,1,30);

    Pkg_Ecas_Audit.CHECK_EVENT_ID(l_audit_event_id
                                , l_ecas_id
                                , 'UPD'
                                , l_userid
                                , SYSDATE
                                , l_error_message);

    IF l_audit_event_id IS NOT NULL THEN

      IF NVL(:NEW.ROAD_NAME,0) != NVL(:OLD.ROAD_NAME,0) THEN
        l_column_name := 'ROAD_NAME';
        l_old_value := :OLD.ROAD_NAME;
        l_new_value := :NEW.ROAD_NAME;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.BGC_ZONE_CODE, 0) != NVL(:OLD.BGC_ZONE_CODE, 0) THEN
        l_column_name := 'BGC_ZONE_CODE';
        l_old_value := :OLD.BGC_ZONE_CODE;
        l_new_value := :NEW.BGC_ZONE_CODE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.ROAD_GROUP_CODE, 0) != NVL(:OLD.ROAD_GROUP_CODE, 0) THEN
        l_column_name := 'ROAD_GROUP_CODE';
        l_old_value := :OLD.ROAD_GROUP_CODE;
        l_new_value := :NEW.ROAD_GROUP_CODE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);

      END IF;

    END IF; --l_audit_event_id is not null

  ELSIF DELETING THEN
    l_ecas_id := :OLD.ecas_id;
    l_business_id := 'Road: '||SUBSTR(:OLD.ROAD_NAME,1,30);
    Pkg_Ecas_Audit.GET_USERID(l_userid
                            , 'DEL'
                            , l_ecas_id
                            , SYSDATE
                            , l_error_message);

    Pkg_Ecas_Audit.CHECK_EVENT_ID(l_audit_event_id
                                , l_ecas_id
                                , 'DEL'
                                , l_userid
                                , SYSDATE
                                , l_error_message);

    IF l_audit_event_id IS NOT NULL THEN

      l_column_name := 'ROAD_NAME';
      l_old_value := :OLD.ROAD_NAME;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'BGC_ZONE_CODE';
      l_old_value := :OLD.BGC_ZONE_CODE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'ROAD_GROUP_CODE';
      l_old_value := :OLD.ROAD_GROUP_CODE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

    END IF; --l_audit_event_id is not null

  END IF; --if updating or deleting

  EXCEPTION
    WHEN TOO_MANY_ROWS THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.record.toomany:TRG_ADS_ROAD1,'||TRIM(l_table_name)||','||SQLCODE||';';
    WHEN DUP_VAL_ON_INDEX THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.record.duplicate:TRG_ADS_ROAD1,'||TRIM(l_table_name)||','||SQLCODE||';';
    WHEN NO_DATA_FOUND THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.record.invalid:TRG_ADS_ROAD1,'||TRIM(l_table_name)||';';
    WHEN OTHERS THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.unexpected:TRG_ADS_ROAD1,'||SQLCODE||','||SQLERRM||';';
    RAISE;
END TRG_ADS_ROAD1;




/
ALTER TRIGGER "THE"."TRG_ADS_ROAD1" ENABLE;

  CREATE OR REPLACE EDITIONABLE TRIGGER "THE"."TRG_ADS_SPECIFIED_OPERATION1" 
 AFTER UPDATE OR
	   DELETE OR INSERT
   ON ADS_SPECIFIED_OPERATION

FOR EACH ROW

DECLARE

  l_table_name      ECAS_AUDIT_DETAIL.TABLE_NAME%TYPE;
  l_column_name     ECAS_AUDIT_DETAIL.COLUMN_NAME%TYPE;
  l_old_value       ECAS_AUDIT_DETAIL.OLD_VALUE%TYPE;
  l_new_value       ECAS_AUDIT_DETAIL.NEW_VALUE%TYPE;
  l_ecas_id         ECAS_AUDIT_DETAIL.ECAS_ID%TYPE;
  l_audit_event_id  ECAS_AUDIT_DETAIL.AUDIT_EVENT_ID%TYPE;
  l_userid          ECAS_AUDIT_DETAIL.ENTRY_USERID%TYPE;
  l_business_id     ECAS_AUDIT_DETAIL.BUSINESS_IDENTIFIER%TYPE;

  l_error_message VARCHAR2(1500);

BEGIN

  l_table_name := 'ADS_SPECIFIED_OPERATION';
  l_audit_event_id := NULL;

  IF UPDATING OR INSERTING THEN
    l_userid := :NEW.update_userid;
    l_ecas_id := :NEW.ecas_id;
	l_business_id := 'Spec Op Phase: '||:NEW.SPECIFIED_OPERATIONS_PHS_CODE||' Spec Op: '||:NEW.SPECIFIED_OPERATIONS_CODE;

    Pkg_Ecas_Audit.CHECK_EVENT_ID(l_audit_event_id
                                , l_ecas_id
                                , 'UPD'
                                , l_userid
                                , SYSDATE
                                , l_error_message);

    IF l_audit_event_id IS NOT NULL THEN

      IF NVL(:NEW.SPECIFIED_OPERATIONS_PHS_CODE,0) != NVL(:OLD.SPECIFIED_OPERATIONS_PHS_CODE,0) THEN
        l_column_name := 'SPECIFIED_OPERATIONS_PHS_CODE';
        l_old_value := :OLD.SPECIFIED_OPERATIONS_PHS_CODE;
        l_new_value := :NEW.SPECIFIED_OPERATIONS_PHS_CODE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.SPECIFIED_OPERATIONS_CODE,0) != NVL(:OLD.SPECIFIED_OPERATIONS_CODE,0) THEN
        l_column_name := 'SPECIFIED_OPERATIONS_CODE';
        l_old_value := :OLD.SPECIFIED_OPERATIONS_CODE;
        l_new_value := :NEW.SPECIFIED_OPERATIONS_CODE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.SPECIFIED_OPERATING_RATE,0) != NVL(:OLD.SPECIFIED_OPERATING_RATE,0) THEN
        l_column_name := 'SPECIFIED_OPERATING_RATE';
        l_old_value := :OLD.SPECIFIED_OPERATING_RATE;
        l_new_value := :NEW.SPECIFIED_OPERATING_RATE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.BLENDED_PCT, 0) != NVL(:OLD.BLENDED_PCT, 0) THEN
        l_column_name := 'BLENDED_PCT';
        l_old_value := :OLD.BLENDED_PCT;
        l_new_value := :NEW.BLENDED_PCT;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.COMMENTS, 0) != NVL(:OLD.COMMENTS, 0) THEN
        l_column_name := 'COMMENTS';
        l_old_value := :OLD.COMMENTS;
        l_new_value := :NEW.COMMENTS;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.APPLICABLE_VOLUME, 0) != NVL(:OLD.APPLICABLE_VOLUME, 0) THEN
        l_column_name := 'APPLICABLE_VOLUME';
        l_old_value := :OLD.APPLICABLE_VOLUME;
        l_new_value := :NEW.APPLICABLE_VOLUME;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.APPLICABLE_FRACTION, 0) != NVL(:OLD.APPLICABLE_FRACTION, 0) THEN
        l_column_name := 'APPLICABLE_FRACTION';
        l_old_value := :OLD.APPLICABLE_FRACTION;
        l_new_value := :NEW.APPLICABLE_FRACTION;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);

      END IF;

    END IF; --l_audit_event_id is not null

  ELSIF DELETING THEN
    l_ecas_id := :OLD.ecas_id;
	l_business_id := 'Spec Op Phase: '||:OLD.SPECIFIED_OPERATIONS_PHS_CODE||' Spec Op: '||:OLD.SPECIFIED_OPERATIONS_CODE;
    Pkg_Ecas_Audit.GET_USERID(l_userid
                            , 'DEL'
                            , l_ecas_id
                            , SYSDATE
                            , l_error_message);

    Pkg_Ecas_Audit.CHECK_EVENT_ID(l_audit_event_id
                                , l_ecas_id
                                , 'DEL'
                                , l_userid
                                , SYSDATE
                                , l_error_message);

    IF l_audit_event_id IS NOT NULL THEN

      l_column_name := 'SPECIFIED_OPERATIONS_PHS_CODE';
      l_old_value := :OLD.SPECIFIED_OPERATIONS_PHS_CODE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'SPECIFIED_OPERATIONS_CODE';
      l_old_value := :OLD.SPECIFIED_OPERATIONS_CODE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'SPECIFIED_OPERATING_RATE';
      l_old_value := :OLD.SPECIFIED_OPERATING_RATE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'BLENDED_PCT';
      l_old_value := :OLD.BLENDED_PCT;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'COMMENTS';
      l_old_value := :OLD.COMMENTS;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'APPLICABLE_VOLUME';
      l_old_value := :OLD.APPLICABLE_VOLUME;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'APPLICABLE_FRACTION';
      l_old_value := :OLD.APPLICABLE_FRACTION;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

    END IF; --l_audit_event_id is not null

  END IF; --if updating or deleting

  EXCEPTION
    WHEN TOO_MANY_ROWS THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.record.toomany:TRG_ADS_SPECIFIED_OPERATION1,'||TRIM(l_table_name)||','||SQLCODE||';';
    WHEN DUP_VAL_ON_INDEX THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.record.duplicate:TRG_ADS_SPECIFIED_OPERATION1,'||TRIM(l_table_name)||','||SQLCODE||';';
    WHEN NO_DATA_FOUND THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.record.invalid:TRG_ADS_SPECIFIED_OPERATION1,'||TRIM(l_table_name)||';';
    WHEN OTHERS THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.unexpected:TRG_ADS_SPECIFIED_OPERATION1,'||SQLCODE||','||SQLERRM||';';
    RAISE;
END TRG_ADS_SPECIFIED_OPERATION1;


/
ALTER TRIGGER "THE"."TRG_ADS_SPECIFIED_OPERATION1" ENABLE;

  CREATE OR REPLACE EDITIONABLE TRIGGER "THE"."TRG_ADS_AMENDMENT1" 
 AFTER UPDATE OR
	   DELETE OR INSERT
   ON ADS_AMENDMENT

FOR EACH ROW

DECLARE

  l_table_name      ECAS_AUDIT_DETAIL.TABLE_NAME%TYPE;
  l_column_name     ECAS_AUDIT_DETAIL.COLUMN_NAME%TYPE;
  l_old_value       ECAS_AUDIT_DETAIL.OLD_VALUE%TYPE;
  l_new_value       ECAS_AUDIT_DETAIL.NEW_VALUE%TYPE;
  l_ecas_id         ECAS_AUDIT_DETAIL.ECAS_ID%TYPE;
  l_audit_event_id  ECAS_AUDIT_DETAIL.AUDIT_EVENT_ID%TYPE;
  l_userid          ECAS_AUDIT_DETAIL.ENTRY_USERID%TYPE;
  l_business_id     ECAS_AUDIT_DETAIL.BUSINESS_IDENTIFIER%TYPE;

  l_error_message VARCHAR2(1500);

BEGIN

  l_table_name := 'ADS_AMENDMENT';
  l_audit_event_id := NULL;

  IF UPDATING OR INSERTING THEN
    l_ecas_id := :NEW.ecas_id;
    l_userid := :NEW.update_userid;
    l_business_id := 'Cut Block: '||:NEW.CUT_BLOCK_ID||' Approve Date: '||TO_CHAR(:NEW.APPROVAL_DATE,'YYYY-MM-DD');

    Pkg_Ecas_Audit.CHECK_EVENT_ID(l_audit_event_id
                                , l_ecas_id
                                , 'UPD'
                                , l_userid
                                , SYSDATE
                                , l_error_message);

    IF l_audit_event_id IS NOT NULL THEN

      IF NVL(:NEW.FOREST_FILE_ID,0) != NVL(:OLD.FOREST_FILE_ID,0) THEN
        l_column_name := 'FOREST_FILE_ID';
        l_old_value := :OLD.FOREST_FILE_ID;
        l_new_value := :NEW.FOREST_FILE_ID;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.CUTTING_PERMIT_ID,0) != NVL(:OLD.CUTTING_PERMIT_ID,0) THEN
        l_column_name := 'CUTTING_PERMIT_ID';
        l_old_value := :OLD.CUTTING_PERMIT_ID;
        l_new_value := :NEW.CUTTING_PERMIT_ID;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.CUT_BLOCK_ID,0) != NVL(:OLD.CUT_BLOCK_ID,0) THEN
        l_column_name := 'CUT_BLOCK_ID';
        l_old_value := :OLD.CUT_BLOCK_ID;
        l_new_value := :NEW.CUT_BLOCK_ID;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.APPROVAL_DATE,TO_DATE('1800-01-01','YYYY-MM-DD')) != NVL(:OLD.APPROVAL_DATE,TO_DATE('1800-01-01','YYYY-MM-DD')) THEN
        l_column_name := 'APPROVAL_DATE';
        l_old_value := :OLD.APPROVAL_DATE;
        l_new_value := :NEW.APPROVAL_DATE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.APPRAISAL_AMENDMENT_TYPE_CODE,0) != NVL(:OLD.APPRAISAL_AMENDMENT_TYPE_CODE,0) THEN
        l_column_name := 'APPRAISAL_AMENDMENT_TYPE_CODE';
        l_old_value := :OLD.APPRAISAL_AMENDMENT_TYPE_CODE;
        l_new_value := :NEW.APPRAISAL_AMENDMENT_TYPE_CODE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.AMENDMENT_AREA,0) != NVL(:OLD.AMENDMENT_AREA,0) THEN
        l_column_name := 'AMENDMENT_AREA';
        l_old_value := :OLD.AMENDMENT_AREA;
        l_new_value := :NEW.AMENDMENT_AREA;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

    END IF; --l_audit_event_id is not null

  ELSIF DELETING THEN
    l_ecas_id := :OLD.ecas_id;
    l_business_id := 'Cut Block: '||:OLD.CUT_BLOCK_ID||' Approve Date: '||TO_CHAR(:OLD.APPROVAL_DATE,'YYYY-MM-DD');
    Pkg_Ecas_Audit.GET_USERID(l_userid
                            , 'DEL'
                            , l_ecas_id
                            , SYSDATE
                            , l_error_message);

    Pkg_Ecas_Audit.CHECK_EVENT_ID(l_audit_event_id
                                , l_ecas_id
                                , 'DEL'
                                , l_userid
                                , SYSDATE
                                , l_error_message);

    IF l_audit_event_id IS NOT NULL THEN

      l_column_name := 'FOREST_FILE_ID';
      l_old_value := :OLD.FOREST_FILE_ID;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'CUTTING_PERMIT_ID';
      l_old_value := :OLD.CUTTING_PERMIT_ID;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'CUT_BLOCK_ID';
      l_old_value := :OLD.CUT_BLOCK_ID;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'APPROVAL_DATE';
      l_old_value := :OLD.APPROVAL_DATE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'APPRAISAL_AMENDMENT_TYPE_CODE';
      l_old_value := :OLD.APPRAISAL_AMENDMENT_TYPE_CODE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'AMENDMENT_AREA';
      l_old_value := :OLD.AMENDMENT_AREA;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

    END IF; --l_audit_event_id is not null

  END IF; --if updating or deleting

  EXCEPTION
    WHEN TOO_MANY_ROWS THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.record.toomany:TRG_ADS_AMENDMENT1,'||TRIM(l_table_name)||','||SQLCODE||';';
    WHEN DUP_VAL_ON_INDEX THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.record.duplicate:TRG_ADS_AMENDMENT1,'||TRIM(l_table_name)||','||SQLCODE||';';
    WHEN NO_DATA_FOUND THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.record.invalid:TRG_ADS_AMENDMENT1,'||TRIM(l_table_name)||';';
    WHEN OTHERS THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.unexpected:TRG_ADS_AMENDMENT1,'||SQLCODE||','||SQLERRM||';';
    RAISE;
END TRG_ADS_AMENDMENT1;




/
ALTER TRIGGER "THE"."TRG_ADS_AMENDMENT1" ENABLE;

  CREATE OR REPLACE EDITIONABLE TRIGGER "THE"."TRG_ADS_TABULAR_ROAD_SECTION1" 
 AFTER UPDATE OR
	   DELETE OR INSERT
   ON ADS_TABULAR_ROAD_SECTION

FOR EACH ROW

DECLARE

  l_table_name      ECAS_AUDIT_DETAIL.TABLE_NAME%TYPE;
  l_column_name     ECAS_AUDIT_DETAIL.COLUMN_NAME%TYPE;
  l_old_value       ECAS_AUDIT_DETAIL.OLD_VALUE%TYPE;
  l_new_value       ECAS_AUDIT_DETAIL.NEW_VALUE%TYPE;
  l_ecas_id         ECAS_AUDIT_DETAIL.ECAS_ID%TYPE;
  l_audit_event_id  ECAS_AUDIT_DETAIL.AUDIT_EVENT_ID%TYPE;
  l_userid          ECAS_AUDIT_DETAIL.ENTRY_USERID%TYPE;
  l_business_id     ECAS_AUDIT_DETAIL.BUSINESS_IDENTIFIER%TYPE;

  l_error_message VARCHAR2(1500);
  l_temp_business_id VARCHAR2(500);

BEGIN

  l_table_name := 'ADS_TABULAR_ROAD_SECTION';
  l_audit_event_id := NULL;

  IF UPDATING OR INSERTING THEN
    l_userid := :NEW.update_userid;
    l_ecas_id := :NEW.ecas_id;
	l_temp_business_id := 'Rd:'||SUBSTR(:NEW.ROAD_NAME,1,15)||' Strt:'||:NEW.STATION_START_POINT||' End:'||:NEW.STATION_END_POINT;
	l_temp_business_id := l_temp_business_id||' Id:'||:NEW.SECTION_ID;
	l_business_id := SUBSTR(l_temp_business_id,1,50);

    Pkg_Ecas_Audit.CHECK_EVENT_ID(l_audit_event_id
                                , l_ecas_id
                                , 'UPD'
                                , l_userid
                                , SYSDATE
                                , l_error_message);

    IF l_audit_event_id IS NOT NULL THEN

      IF NVL(:NEW.ROAD_NAME,0) != NVL(:OLD.ROAD_NAME,0) THEN
        l_column_name := 'ROAD_NAME';
        l_old_value := :OLD.ROAD_NAME;
        l_new_value := :NEW.ROAD_NAME;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.SECTION_ID,0) != NVL(:OLD.SECTION_ID,0) THEN
        l_column_name := 'SECTION_ID';
        l_old_value := :OLD.SECTION_ID;
        l_new_value := :NEW.SECTION_ID;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.APPRAISAL_YEAR,0) != NVL(:OLD.APPRAISAL_YEAR,0) THEN
        l_column_name := 'APPRAISAL_YEAR';
        l_old_value := :OLD.APPRAISAL_YEAR;
        l_new_value := :NEW.APPRAISAL_YEAR;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.STATION_START_POINT, 0) != NVL(:OLD.STATION_START_POINT, 0) THEN
        l_column_name := 'STATION_START_POINT';
        l_old_value := :OLD.STATION_START_POINT;
        l_new_value := :NEW.STATION_START_POINT;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.STATION_END_POINT, 0) != NVL(:OLD.STATION_END_POINT, 0) THEN
        l_column_name := 'STATION_END_POINT';
        l_old_value := :OLD.STATION_END_POINT;
        l_new_value := :NEW.STATION_END_POINT;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.UPHILL_SIDE_SLOPE_PCT, 0) != NVL(:OLD.UPHILL_SIDE_SLOPE_PCT, 0) THEN
        l_column_name := 'UPHILL_SIDE_SLOPE_PCT';
        l_old_value := :OLD.UPHILL_SIDE_SLOPE_PCT;
        l_new_value := :NEW.UPHILL_SIDE_SLOPE_PCT;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.TRUCKING_DISTANCE, 0) != NVL(:OLD.TRUCKING_DISTANCE, 0) THEN
        l_column_name := 'TRUCKING_DISTANCE';
        l_old_value := :OLD.TRUCKING_DISTANCE;
        l_new_value := :NEW.TRUCKING_DISTANCE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.SUBGRADE_ROCK_PCT, 0) != NVL(:OLD.SUBGRADE_ROCK_PCT, 0) THEN
        l_column_name := 'SUBGRADE_ROCK_PCT';
        l_old_value := :OLD.SUBGRADE_ROCK_PCT;
        l_new_value := :NEW.SUBGRADE_ROCK_PCT;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.CROWN_PCT, 0) != NVL(:OLD.CROWN_PCT, 0) THEN
        l_column_name := 'CROWN_PCT';
        l_old_value := :OLD.CROWN_PCT;
        l_new_value := :NEW.CROWN_PCT;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.AMORTIZED_PCT, 0) != NVL(:OLD.AMORTIZED_PCT, 0) THEN
        l_column_name := 'AMORTIZED_PCT';
        l_old_value := :OLD.AMORTIZED_PCT;
        l_new_value := :NEW.AMORTIZED_PCT;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.PARTIAL_PCT, 0) != NVL(:OLD.PARTIAL_PCT, 0) THEN
        l_column_name := 'PARTIAL_PCT';
        l_old_value := :OLD.PARTIAL_PCT;
        l_new_value := :NEW.PARTIAL_PCT;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.BORROW_PIT_LOCATION, 0) != NVL(:OLD.BORROW_PIT_LOCATION, 0) THEN
        l_column_name := 'BORROW_PIT_LOCATION';
        l_old_value := :OLD.BORROW_PIT_LOCATION;
        l_new_value := :NEW.BORROW_PIT_LOCATION;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.SECTION_BUILT_IND, 0) != NVL(:OLD.SECTION_BUILT_IND, 0) THEN
        l_column_name := 'SECTION_BUILT_IND';
        l_old_value := :OLD.SECTION_BUILT_IND;
        l_new_value := :NEW.SECTION_BUILT_IND;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.SECTION_LENGTH, 0) != NVL(:OLD.SECTION_LENGTH, 0) THEN
        l_column_name := 'SECTION_LENGTH';
        l_old_value := :OLD.SECTION_LENGTH;
        l_new_value := :NEW.SECTION_LENGTH;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.SIDE_SLOPE_PCT, 0) != NVL(:OLD.SIDE_SLOPE_PCT, 0) THEN
        l_column_name := 'SIDE_SLOPE_PCT';
        l_old_value := :OLD.SIDE_SLOPE_PCT;
        l_new_value := :NEW.SIDE_SLOPE_PCT;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.STABILIZING_MATERIAL_LENGTH, 0) != NVL(:OLD.STABILIZING_MATERIAL_LENGTH, 0) THEN
        l_column_name := 'STABILIZING_MATERIAL_LENGTH';
        l_old_value := :OLD.STABILIZING_MATERIAL_LENGTH;
        l_new_value := :NEW.STABILIZING_MATERIAL_LENGTH;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.SURFACING_RMC_CODE, 0) != NVL(:OLD.SURFACING_RMC_CODE, 0) THEN
        l_column_name := 'SURFACING_RMC_CODE';
        l_old_value := :OLD.SURFACING_RMC_CODE;
        l_new_value := :NEW.SURFACING_RMC_CODE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.SURFACING_MATERIAL_TYPE_CODE, 0) != NVL(:OLD.SURFACING_MATERIAL_TYPE_CODE, 0) THEN
        l_column_name := 'SURFACING_MATERIAL_TYPE_CODE';
        l_old_value := :OLD.SURFACING_MATERIAL_TYPE_CODE;
        l_new_value := :NEW.SURFACING_MATERIAL_TYPE_CODE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.CONSTRUCTION_CATEGORY_CODE, 0) != NVL(:OLD.CONSTRUCTION_CATEGORY_CODE, 0) THEN
        l_column_name := 'CONSTRUCTION_CATEGORY_CODE';
        l_old_value := :OLD.CONSTRUCTION_CATEGORY_CODE;
        l_new_value := :NEW.CONSTRUCTION_CATEGORY_CODE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.SUBGRADE_RMC_CODE, 0) != NVL(:OLD.SUBGRADE_RMC_CODE, 0) THEN
        l_column_name := 'SUBGRADE_RMC_CODE';
        l_old_value := :OLD.SUBGRADE_RMC_CODE;
        l_new_value := :NEW.SUBGRADE_RMC_CODE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.SOIL_MOISTURE_CODE, 0) != NVL(:OLD.SOIL_MOISTURE_CODE, 0) THEN
        l_column_name := 'SOIL_MOISTURE_CODE';
        l_old_value := :OLD.SOIL_MOISTURE_CODE;
        l_new_value := :NEW.SOIL_MOISTURE_CODE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.APPRAISAL_ROAD_TYPE_CODE, 0) != NVL(:OLD.APPRAISAL_ROAD_TYPE_CODE, 0) THEN
        l_column_name := 'APPRAISAL_ROAD_TYPE_CODE';
        l_old_value := :OLD.APPRAISAL_ROAD_TYPE_CODE;
        l_new_value := :NEW.APPRAISAL_ROAD_TYPE_CODE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.BANK_HEIGHT_CATEGORY_CODE, 0) != NVL(:OLD.BANK_HEIGHT_CATEGORY_CODE, 0) THEN
        l_column_name := 'BANK_HEIGHT_CATEGORY_CODE';
        l_old_value := :OLD.BANK_HEIGHT_CATEGORY_CODE;
        l_new_value := :NEW.BANK_HEIGHT_CATEGORY_CODE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.BEC_ZONE_CODE, 0) != NVL(:OLD.BEC_ZONE_CODE, 0) THEN
        l_column_name := 'BEC_ZONE_CODE';
        l_old_value := :OLD.BEC_ZONE_CODE;
        l_new_value := :NEW.BEC_ZONE_CODE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
                                      , l_business_id
                                      , l_error_message);

      END IF;

    END IF; --l_audit_event_id is not null

  ELSIF DELETING THEN
    l_ecas_id := :OLD.ecas_id;
	l_temp_business_id := 'Rd:'||SUBSTR(:OLD.ROAD_NAME,1,15)||' Strt:'||:OLD.STATION_START_POINT||' End:'||:OLD.STATION_END_POINT;
	l_temp_business_id := l_temp_business_id||' Id:'||:OLD.SECTION_ID;
	l_business_id := SUBSTR(l_temp_business_id,1,50);
    Pkg_Ecas_Audit.GET_USERID(l_userid
                            , 'DEL'
                            , l_ecas_id
                            , SYSDATE
                            , l_error_message);

    Pkg_Ecas_Audit.CHECK_EVENT_ID(l_audit_event_id
                                , l_ecas_id
                                , 'DEL'
                                , l_userid
                                , SYSDATE
                                , l_error_message);

    IF l_audit_event_id IS NOT NULL THEN

      l_column_name := 'ROAD_NAME';
      l_old_value := :OLD.ROAD_NAME;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'SECTION_ID';
      l_old_value := :OLD.SECTION_ID;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'APPRAISAL_YEAR';
      l_old_value := :OLD.APPRAISAL_YEAR;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'STATION_START_POINT';
      l_old_value := :OLD.STATION_START_POINT;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'STATION_END_POINT';
      l_old_value := :OLD.STATION_END_POINT;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'UPHILL_SIDE_SLOPE_PCT';
      l_old_value := :OLD.UPHILL_SIDE_SLOPE_PCT;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'TRUCKING_DISTANCE';
      l_old_value := :OLD.TRUCKING_DISTANCE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'SUBGRADE_ROCK_PCT';
      l_old_value := :OLD.SUBGRADE_ROCK_PCT;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'CROWN_PCT';
      l_old_value := :OLD.CROWN_PCT;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'AMORTIZED_PCT';
      l_old_value := :OLD.AMORTIZED_PCT;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'PARTIAL_PCT';
      l_old_value := :OLD.PARTIAL_PCT;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'BORROW_PIT_LOCATION';
      l_old_value := :OLD.BORROW_PIT_LOCATION;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'SECTION_BUILT_IND';
      l_old_value := :OLD.SECTION_BUILT_IND;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'SECTION_LENGTH';
      l_old_value := :OLD.SECTION_LENGTH;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'SIDE_SLOPE_PCT';
      l_old_value := :OLD.SIDE_SLOPE_PCT;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'STABILIZING_MATERIAL_LENGTH';
      l_old_value := :OLD.STABILIZING_MATERIAL_LENGTH;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'SURFACING_RMC_CODE';
      l_old_value := :OLD.SURFACING_RMC_CODE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'SURFACING_MATERIAL_TYPE_CODE';
      l_old_value := :OLD.SURFACING_MATERIAL_TYPE_CODE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'CONSTRUCTION_CATEGORY_CODE';
      l_old_value := :OLD.CONSTRUCTION_CATEGORY_CODE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'SUBGRADE_RMC_CODE';
      l_old_value := :OLD.SUBGRADE_RMC_CODE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'SOIL_MOISTURE_CODE';
      l_old_value := :OLD.SOIL_MOISTURE_CODE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'APPRAISAL_ROAD_TYPE_CODE';
      l_old_value := :OLD.APPRAISAL_ROAD_TYPE_CODE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'BANK_HEIGHT_CATEGORY_CODE';
      l_old_value := :OLD.BANK_HEIGHT_CATEGORY_CODE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'BEC_ZONE_CODE';
      l_old_value := :OLD.BEC_ZONE_CODE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
                                      , l_business_id
                                      , l_error_message);
      END IF;

    END IF; --l_audit_event_id is not null

  END IF; --if updating or deleting

  EXCEPTION
    WHEN TOO_MANY_ROWS THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.record.toomany:TRG_ADS_TABULAR_ROAD_SECTION1,'||TRIM(l_table_name)||','||SQLCODE||';';
    WHEN DUP_VAL_ON_INDEX THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.record.duplicate:TRG_ADS_TABULAR_ROAD_SECTION1,'||TRIM(l_table_name)||','||SQLCODE||';';
    WHEN NO_DATA_FOUND THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.record.invalid:TRG_ADS_TABULAR_ROAD_SECTION1,'||TRIM(l_table_name)||';';
    WHEN OTHERS THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.unexpected:TRG_ADS_TABULAR_ROAD_SECTION1,'||SQLCODE||','||SQLERRM||';';
    RAISE;
END TRG_ADS_TABULAR_ROAD_SECTION1;




/
ALTER TRIGGER "THE"."TRG_ADS_TABULAR_ROAD_SECTION1" ENABLE;

  CREATE OR REPLACE EDITIONABLE TRIGGER "THE"."TRG_ADS_BRIDGE1" 
 AFTER UPDATE OR
	   DELETE OR INSERT
   ON ADS_BRIDGE

FOR EACH ROW

DECLARE

  l_table_name      ECAS_AUDIT_DETAIL.TABLE_NAME%TYPE;
  l_column_name     ECAS_AUDIT_DETAIL.COLUMN_NAME%TYPE;
  l_old_value       ECAS_AUDIT_DETAIL.OLD_VALUE%TYPE;
  l_new_value       ECAS_AUDIT_DETAIL.NEW_VALUE%TYPE;
  l_ecas_id         ECAS_AUDIT_DETAIL.ECAS_ID%TYPE;
  l_audit_event_id  ECAS_AUDIT_DETAIL.AUDIT_EVENT_ID%TYPE;
  l_userid          ECAS_AUDIT_DETAIL.ENTRY_USERID%TYPE;
  l_business_id     ECAS_AUDIT_DETAIL.BUSINESS_IDENTIFIER%TYPE;

  l_error_message VARCHAR2(1500);
  l_temp_business_id VARCHAR2(500);

BEGIN

  l_table_name := 'ADS_BRIDGE';
  l_audit_event_id := NULL;

  IF UPDATING OR INSERTING THEN
    l_userid := :NEW.update_userid;
    l_ecas_id := :NEW.ecas_id;
	l_temp_business_id := 'Road:'||SUBSTR(:NEW.ROAD_NAME,1,20)||' Start:'||:NEW.STATION_START_POINT||' End:'||:NEW.STATION_END_POINT;
	l_business_id := SUBSTR(l_temp_business_id,1,50);

    Pkg_Ecas_Audit.CHECK_EVENT_ID(l_audit_event_id
                                , l_ecas_id
                                , 'UPD'
                                , l_userid
                                , SYSDATE
                                , l_error_message);

    IF l_audit_event_id IS NOT NULL THEN

      IF NVL(:NEW.ROAD_NAME,0) != NVL(:OLD.ROAD_NAME,0) THEN
        l_column_name := 'ROAD_NAME';
        l_old_value := :OLD.ROAD_NAME;
        l_new_value := :NEW.ROAD_NAME;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.BRIDGE_ID,0) != NVL(:OLD.BRIDGE_ID,0) THEN
        l_column_name := 'BRIDGE_ID';
        l_old_value := :OLD.BRIDGE_ID;
        l_new_value := :NEW.BRIDGE_ID;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.APPRAISAL_YEAR,0) != NVL(:OLD.APPRAISAL_YEAR,0) THEN
        l_column_name := 'APPRAISAL_YEAR';
        l_old_value := :OLD.APPRAISAL_YEAR;
        l_new_value := :NEW.APPRAISAL_YEAR;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.STATION_START_POINT,0) != NVL(:OLD.STATION_START_POINT,0) THEN
        l_column_name := 'STATION_START_POINT';
        l_old_value := :OLD.STATION_START_POINT;
        l_new_value := :NEW.STATION_START_POINT;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.STATION_END_POINT,0) != NVL(:OLD.STATION_END_POINT,0) THEN
        l_column_name := 'STATION_END_POINT';
        l_old_value := :OLD.STATION_END_POINT;
        l_new_value := :NEW.STATION_END_POINT;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.BRIDGE_TYPE_CODE,0) != NVL(:OLD.BRIDGE_TYPE_CODE,0) THEN
        l_column_name := 'BRIDGE_TYPE_CODE';
        l_old_value := :OLD.BRIDGE_TYPE_CODE;
        l_new_value := :NEW.BRIDGE_TYPE_CODE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.ROAD_DEVELOPMENT_TYPE_CODE,0) != NVL(:OLD.ROAD_DEVELOPMENT_TYPE_CODE,0) THEN
        l_column_name := 'ROAD_DEVELOPMENT_TYPE_CODE';
        l_old_value := :OLD.ROAD_DEVELOPMENT_TYPE_CODE;
        l_new_value := :NEW.ROAD_DEVELOPMENT_TYPE_CODE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.CRIB_HEIGHT, 0) != NVL(:OLD.CRIB_HEIGHT, 0) THEN
        l_column_name := 'CRIB_HEIGHT';
        l_old_value := :OLD.CRIB_HEIGHT;
        l_new_value := :NEW.CRIB_HEIGHT;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.SPAN_LENGTH, 0) != NVL(:OLD.SPAN_LENGTH, 0) THEN
        l_column_name := 'SPAN_LENGTH';
        l_old_value := :OLD.SPAN_LENGTH;
        l_new_value := :NEW.SPAN_LENGTH;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.CROWN_PCT, 0) != NVL(:OLD.CROWN_PCT, 0) THEN
        l_column_name := 'CROWN_PCT';
        l_old_value := :OLD.CROWN_PCT;
        l_new_value := :NEW.CROWN_PCT;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.AMORTIZED_PCT, 0) != NVL(:OLD.AMORTIZED_PCT, 0) THEN
        l_column_name := 'AMORTIZED_PCT';
        l_old_value := :OLD.AMORTIZED_PCT;
        l_new_value := :NEW.AMORTIZED_PCT;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.BRIDGE_BUILT_IND, 0) != NVL(:OLD.BRIDGE_BUILT_IND, 0) THEN
        l_column_name := 'BRIDGE_BUILT_IND';
        l_old_value := :OLD.BRIDGE_BUILT_IND;
        l_new_value := :NEW.BRIDGE_BUILT_IND;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

    END IF;

  END IF; --l_audit_event_id is not null

  ELSIF DELETING THEN
    l_ecas_id := :OLD.ecas_id;
	l_temp_business_id := 'Road:'||SUBSTR(:OLD.ROAD_NAME,1,20)||' Start:'||:OLD.STATION_START_POINT||' End:'||:OLD.STATION_END_POINT;
	l_business_id := SUBSTR(l_temp_business_id,1,50);
    Pkg_Ecas_Audit.GET_USERID(l_userid
                            , 'DEL'
                            , l_ecas_id
                            , SYSDATE
                            , l_error_message);

    Pkg_Ecas_Audit.CHECK_EVENT_ID(l_audit_event_id
                                , l_ecas_id
                                , 'DEL'
                                , l_userid
                                , SYSDATE
                                , l_error_message);

    IF l_audit_event_id IS NOT NULL THEN

      l_column_name := 'ROAD_NAME';
      l_old_value := :OLD.ROAD_NAME;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'BRIDGE_ID';
      l_old_value := :OLD.BRIDGE_ID;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'APPRAISAL_YEAR';
      l_old_value := :OLD.APPRAISAL_YEAR;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'STATION_START_POINT';
      l_old_value := :OLD.STATION_START_POINT;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'STATION_END_POINT';
      l_old_value := :OLD.STATION_END_POINT;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'BRIDGE_TYPE_CODE';
      l_old_value := :OLD.BRIDGE_TYPE_CODE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'ROAD_DEVELOPMENT_TYPE_CODE';
      l_old_value := :OLD.ROAD_DEVELOPMENT_TYPE_CODE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'CRIB_HEIGHT';
      l_old_value := :OLD.CRIB_HEIGHT;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'SPAN_LENGTH';
      l_old_value := :OLD.SPAN_LENGTH;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'CROWN_PCT';
      l_old_value := :OLD.CROWN_PCT;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'AMORTIZED_PCT';
      l_old_value := :OLD.AMORTIZED_PCT;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'BRIDGE_BUILT_IND';
      l_old_value := :OLD.BRIDGE_BUILT_IND;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

    END IF; --l_audit_event_id is not null

  END IF; --if updating or deleting

  EXCEPTION
    WHEN TOO_MANY_ROWS THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.record.toomany:TRG_ADS_BRIDGE1,'||TRIM(l_table_name)||','||SQLCODE||';';
    WHEN DUP_VAL_ON_INDEX THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.record.duplicate:TRG_ADS_BRIDGE1,'||TRIM(l_table_name)||','||SQLCODE||';';
    WHEN NO_DATA_FOUND THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.record.invalid:TRG_ADS_BRIDGE1,'||TRIM(l_table_name)||';';
    WHEN OTHERS THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.unexpected:TRG_ADS_BRIDGE1,'||SQLCODE||','||SQLERRM||';';
    RAISE;
END TRG_ADS_BRIDGE1;




/
ALTER TRIGGER "THE"."TRG_ADS_BRIDGE1" ENABLE;

  CREATE OR REPLACE EDITIONABLE TRIGGER "THE"."TRG_ADS_OTHER_DEV_COST1" 
 AFTER UPDATE OR
	   DELETE OR INSERT
   ON ADS_OTHER_DEVELOPMENT_COST

FOR EACH ROW

DECLARE

  l_table_name      ECAS_AUDIT_DETAIL.TABLE_NAME%TYPE;
  l_column_name     ECAS_AUDIT_DETAIL.COLUMN_NAME%TYPE;
  l_old_value       ECAS_AUDIT_DETAIL.OLD_VALUE%TYPE;
  l_new_value       ECAS_AUDIT_DETAIL.NEW_VALUE%TYPE;
  l_ecas_id         ECAS_AUDIT_DETAIL.ECAS_ID%TYPE;
  l_audit_event_id  ECAS_AUDIT_DETAIL.AUDIT_EVENT_ID%TYPE;
  l_userid          ECAS_AUDIT_DETAIL.ENTRY_USERID%TYPE;
  l_business_id     ECAS_AUDIT_DETAIL.BUSINESS_IDENTIFIER%TYPE;

  l_error_message VARCHAR2(1500);

BEGIN

  l_table_name := 'ADS_OTHER_DEVELOPMENT_COST';
  l_audit_event_id := NULL;

  IF UPDATING OR INSERTING THEN
    l_userid := :NEW.update_userid;
    l_ecas_id := :NEW.ecas_id;
    l_business_id := 'Devel Type: '||:NEW.OTHER_DEVELOPMENT_TYPE_CODE||' Description: '||SUBSTR(:NEW.OTHER_DEVELOPMENT_DESCRIPTION,1,20);

    Pkg_Ecas_Audit.CHECK_EVENT_ID(l_audit_event_id
                                , l_ecas_id
                                , 'UPD'
                                , l_userid
                                , SYSDATE
                                , l_error_message);

    IF l_audit_event_id IS NOT NULL THEN

      IF NVL(:NEW.OTHER_DEVELOPMENT_TYPE_CODE,0) != NVL(:OLD.OTHER_DEVELOPMENT_TYPE_CODE,0) THEN
        l_column_name := 'OTHER_DEVELOPMENT_TYPE_CODE';
        l_old_value := :OLD.OTHER_DEVELOPMENT_TYPE_CODE;
        l_new_value := :NEW.OTHER_DEVELOPMENT_TYPE_CODE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.OTHER_DEVELOPMENT_ID,0) != NVL(:OLD.OTHER_DEVELOPMENT_ID,0) THEN
        l_column_name := 'OTHER_DEVELOPMENT_ID';
        l_old_value := :OLD.OTHER_DEVELOPMENT_ID;
        l_new_value := :NEW.OTHER_DEVELOPMENT_ID;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.OTHER_DEVELOPMENT_DESCRIPTION,0) != NVL(:OLD.OTHER_DEVELOPMENT_DESCRIPTION,0) THEN
        l_column_name := 'OTHER_DEVELOPMENT_DESCRIPTION';
        l_old_value := :OLD.OTHER_DEVELOPMENT_DESCRIPTION;
        l_new_value := :NEW.OTHER_DEVELOPMENT_DESCRIPTION;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.COST_BASE_YEAR,0) != NVL(:OLD.COST_BASE_YEAR,0) THEN
        l_column_name := 'COST_BASE_YEAR';
        l_old_value := :OLD.COST_BASE_YEAR;
        l_new_value := :NEW.COST_BASE_YEAR;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.ORIGINAL_COST,0) != NVL(:OLD.ORIGINAL_COST,0) THEN
        l_column_name := 'ORIGINAL_COST';
        l_old_value := :OLD.ORIGINAL_COST;
        l_new_value := :NEW.ORIGINAL_COST;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.TRENDED_COST,0) != NVL(:OLD.TRENDED_COST,0) THEN
        l_column_name := 'TRENDED_COST';
        l_old_value := :OLD.TRENDED_COST;
        l_new_value := :NEW.TRENDED_COST;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.APPLICABLE_VOLUME, 0) != NVL(:OLD.APPLICABLE_VOLUME, 0) THEN
        l_column_name := 'APPLICABLE_VOLUME';
        l_old_value := :OLD.APPLICABLE_VOLUME;
        l_new_value := :NEW.APPLICABLE_VOLUME;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.AMENDED_IND, 0) != NVL(:OLD.AMENDED_IND, 0) THEN
        l_column_name := 'AMENDED_IND';
        l_old_value := :OLD.AMENDED_IND;
        l_new_value := :NEW.AMENDED_IND;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);

      END IF;

    END IF; --l_audit_event_id is not null

  ELSIF DELETING THEN
    l_ecas_id := :OLD.ecas_id;
    l_business_id := 'Devel Type: '||:OLD.OTHER_DEVELOPMENT_TYPE_CODE||' Description: '||SUBSTR(:OLD.OTHER_DEVELOPMENT_DESCRIPTION,1,20);
    Pkg_Ecas_Audit.GET_USERID(l_userid
                            , 'DEL'
                            , l_ecas_id
                            , SYSDATE
                            , l_error_message);

    Pkg_Ecas_Audit.CHECK_EVENT_ID(l_audit_event_id
                                , l_ecas_id
                                , 'DEL'
                                , l_userid
                                , SYSDATE
                                , l_error_message);

    IF l_audit_event_id IS NOT NULL THEN

      l_column_name := 'OTHER_DEVELOPMENT_TYPE_CODE';
      l_old_value := :OLD.OTHER_DEVELOPMENT_TYPE_CODE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'OTHER_DEVELOPMENT_ID';
      l_old_value := :OLD.OTHER_DEVELOPMENT_ID;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'OTHER_DEVELOPMENT_DESCRIPTION';
      l_old_value := :OLD.OTHER_DEVELOPMENT_DESCRIPTION;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'COST_BASE_YEAR';
      l_old_value := :OLD.COST_BASE_YEAR;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'ORIGINAL_COST';
      l_old_value := :OLD.ORIGINAL_COST;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'TRENDED_COST';
      l_old_value := :OLD.TRENDED_COST;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'APPLICABLE_VOLUME';
      l_old_value := :OLD.APPLICABLE_VOLUME;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'AMENDED_IND';
      l_old_value := :OLD.AMENDED_IND;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

    END IF; --l_audit_event_id is not null

  END IF; --if updating or deleting

  EXCEPTION
    WHEN TOO_MANY_ROWS THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.record.toomany:TRG_ADS_OTHER_DEV_COST1,'||TRIM(l_table_name)||','||SQLCODE||';';
    WHEN DUP_VAL_ON_INDEX THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.record.duplicate:TRG_ADS_OTHER_DEV_COST1,'||TRIM(l_table_name)||','||SQLCODE||';';
    WHEN NO_DATA_FOUND THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.record.invalid:TRG_ADS_OTHER_DEV_COST1,'||TRIM(l_table_name)||';';
    WHEN OTHERS THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.unexpected:TRG_ADS_OTHER_DEV_COST1,'||SQLCODE||','||SQLERRM||';';
    RAISE;
END TRG_ADS_OTHER_DEV_COST1;




/
ALTER TRIGGER "THE"."TRG_ADS_OTHER_DEV_COST1" ENABLE;

  CREATE OR REPLACE EDITIONABLE TRIGGER "THE"."TRG_ADS_ENG_SECTION_COST1" 
 AFTER UPDATE OR
	   DELETE OR INSERT
   ON ADS_ENGINEERING_SECTION_COST

FOR EACH ROW

DECLARE

  l_table_name      ECAS_AUDIT_DETAIL.TABLE_NAME%TYPE;
  l_column_name     ECAS_AUDIT_DETAIL.COLUMN_NAME%TYPE;
  l_old_value       ECAS_AUDIT_DETAIL.OLD_VALUE%TYPE;
  l_new_value       ECAS_AUDIT_DETAIL.NEW_VALUE%TYPE;
  l_ecas_id         ECAS_AUDIT_DETAIL.ECAS_ID%TYPE;
  l_audit_event_id  ECAS_AUDIT_DETAIL.AUDIT_EVENT_ID%TYPE;
  l_userid          ECAS_AUDIT_DETAIL.ENTRY_USERID%TYPE;
  l_business_id     ECAS_AUDIT_DETAIL.BUSINESS_IDENTIFIER%TYPE;

  l_error_message VARCHAR2(1500);
  l_temp_business_id VARCHAR2(500);

BEGIN

  l_table_name := 'ADS_ENGINEERING_SECTION_COST';
  l_audit_event_id := NULL;

  IF UPDATING OR INSERTING THEN
    l_userid := :NEW.update_userid;
    l_ecas_id := :NEW.ecas_id;
	l_temp_business_id := 'Road:'||SUBSTR(:NEW.ROAD_NAME,1,20)||' Start:'||:NEW.STATION_START_POINT||' End:'||:NEW.STATION_END_POINT;
	l_business_id := SUBSTR(l_temp_business_id,1,50);

    Pkg_Ecas_Audit.CHECK_EVENT_ID(l_audit_event_id
                                , l_ecas_id
                                , 'UPD'
                                , l_userid
                                , SYSDATE
                                , l_error_message);

    IF l_audit_event_id IS NOT NULL THEN

      IF NVL(:NEW.ROAD_DEVELOPMENT_TYPE_CODE,0) != NVL(:OLD.ROAD_DEVELOPMENT_TYPE_CODE,0) THEN
        l_column_name := 'ROAD_DEVELOPMENT_TYPE_CODE';
        l_old_value := :OLD.ROAD_DEVELOPMENT_TYPE_CODE;
        l_new_value := :NEW.ROAD_DEVELOPMENT_TYPE_CODE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.APPRAISAL_YEAR,0) != NVL(:OLD.APPRAISAL_YEAR,0) THEN
        l_column_name := 'APPRAISAL_YEAR';
        l_old_value := :OLD.APPRAISAL_YEAR;
        l_new_value := :NEW.APPRAISAL_YEAR;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.COST_ESTIMATE_ID,0) != NVL(:OLD.COST_ESTIMATE_ID,0) THEN
        l_column_name := 'COST_ESTIMATE_ID';
        l_old_value := :OLD.COST_ESTIMATE_ID;
        l_new_value := :NEW.COST_ESTIMATE_ID;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.ROAD_ENGINEERING_TYPE_CODE,0) != NVL(:OLD.ROAD_ENGINEERING_TYPE_CODE,0) THEN
        l_column_name := 'ROAD_ENGINEERING_TYPE_CODE';
        l_old_value := :OLD.ROAD_ENGINEERING_TYPE_CODE;
        l_new_value := :NEW.ROAD_ENGINEERING_TYPE_CODE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.ENGINEERING_SECTION_ID,0) != NVL(:OLD.ENGINEERING_SECTION_ID,0) THEN
        l_column_name := 'ENGINEERING_SECTION_ID';
        l_old_value := :OLD.ENGINEERING_SECTION_ID;
        l_new_value := :NEW.ENGINEERING_SECTION_ID;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.RD_ENGINEERING_PROJ_TYPE_CODE,0) != NVL(:OLD.RD_ENGINEERING_PROJ_TYPE_CODE,0) THEN
        l_column_name := 'RD_ENGINEERING_PROJ_TYPE_CODE';
        l_old_value := :OLD.RD_ENGINEERING_PROJ_TYPE_CODE;
        l_new_value := :NEW.RD_ENGINEERING_PROJ_TYPE_CODE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.ROAD_NAME,0) != NVL(:OLD.ROAD_NAME,0) THEN
        l_column_name := 'ROAD_NAME';
        l_old_value := :OLD.ROAD_NAME;
        l_new_value := :NEW.ROAD_NAME;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.STATION_START_POINT, 0) != NVL(:OLD.STATION_START_POINT, 0) THEN
        l_column_name := 'STATION_START_POINT';
        l_old_value := :OLD.STATION_START_POINT;
        l_new_value := :NEW.STATION_START_POINT;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.STATION_END_POINT, 0) != NVL(:OLD.STATION_END_POINT, 0) THEN
        l_column_name := 'STATION_END_POINT';
        l_old_value := :OLD.STATION_END_POINT;
        l_new_value := :NEW.STATION_END_POINT;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.CONSTRUCTION_CATEGORY_CODE, 0) != NVL(:OLD.CONSTRUCTION_CATEGORY_CODE, 0) THEN
        l_column_name := 'CONSTRUCTION_CATEGORY_CODE';
        l_old_value := :OLD.CONSTRUCTION_CATEGORY_CODE;
        l_new_value := :NEW.CONSTRUCTION_CATEGORY_CODE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.REMAINING_ROAD_WIDTH, 0) != NVL(:OLD.REMAINING_ROAD_WIDTH, 0) THEN
        l_column_name := 'REMAINING_ROAD_WIDTH';
        l_old_value := :OLD.REMAINING_ROAD_WIDTH;
        l_new_value := :NEW.REMAINING_ROAD_WIDTH;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.SUBGRADE_SPOIL_LOCATION, 0) != NVL(:OLD.SUBGRADE_SPOIL_LOCATION, 0) THEN
        l_column_name := 'SUBGRADE_SPOIL_LOCATION';
        l_old_value := :OLD.SUBGRADE_SPOIL_LOCATION;
        l_new_value := :NEW.SUBGRADE_SPOIL_LOCATION;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.STABILIZING_MATERIAL_TYPE_CODE, 0) != NVL(:OLD.STABILIZING_MATERIAL_TYPE_CODE, 0) THEN
        l_column_name := 'STABILIZING_MATERIAL_TYPE_CODE';
        l_old_value := :OLD.STABILIZING_MATERIAL_TYPE_CODE;
        l_new_value := :NEW.STABILIZING_MATERIAL_TYPE_CODE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.SUBGRADE_HAUL_DISTANCE, 0) != NVL(:OLD.SUBGRADE_HAUL_DISTANCE, 0) THEN
        l_column_name := 'SUBGRADE_HAUL_DISTANCE';
        l_old_value := :OLD.SUBGRADE_HAUL_DISTANCE;
        l_new_value := :NEW.SUBGRADE_HAUL_DISTANCE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.UPHILL_SIDE_SLOPE_PCT, 0) != NVL(:OLD.UPHILL_SIDE_SLOPE_PCT, 0) THEN
        l_column_name := 'UPHILL_SIDE_SLOPE_PCT';
        l_old_value := :OLD.UPHILL_SIDE_SLOPE_PCT;
        l_new_value := :NEW.UPHILL_SIDE_SLOPE_PCT;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.TOTAL_END_HAUL_VOLUME, 0) != NVL(:OLD.TOTAL_END_HAUL_VOLUME, 0) THEN
        l_column_name := 'TOTAL_END_HAUL_VOLUME';
        l_old_value := :OLD.TOTAL_END_HAUL_VOLUME;
        l_new_value := :NEW.TOTAL_END_HAUL_VOLUME;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.SUBGRADE_COST_PER_KM, 0) != NVL(:OLD.SUBGRADE_COST_PER_KM, 0) THEN
        l_column_name := 'SUBGRADE_COST_PER_KM';
        l_old_value := :OLD.SUBGRADE_COST_PER_KM;
        l_new_value := :NEW.SUBGRADE_COST_PER_KM;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.SURFACING_REQUIRED_IND, 0) != NVL(:OLD.SURFACING_REQUIRED_IND, 0) THEN
        l_column_name := 'SURFACING_REQUIRED_IND';
        l_old_value := :OLD.SURFACING_REQUIRED_IND;
        l_new_value := :NEW.SURFACING_REQUIRED_IND;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.SURFACING_DEPTH, 0) != NVL(:OLD.SURFACING_DEPTH, 0) THEN
        l_column_name := 'SURFACING_DEPTH';
        l_old_value := :OLD.SURFACING_DEPTH;
        l_new_value := :NEW.SURFACING_DEPTH;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.SURFACING_PIT_LOCATION, 0) != NVL(:OLD.SURFACING_PIT_LOCATION, 0) THEN
        l_column_name := 'SURFACING_PIT_LOCATION';
        l_old_value := :OLD.SURFACING_PIT_LOCATION;
        l_new_value := :NEW.SURFACING_PIT_LOCATION;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.SURFACING_HAUL_DISTANCE, 0) != NVL(:OLD.SURFACING_HAUL_DISTANCE, 0) THEN
        l_column_name := 'SURFACING_HAUL_DISTANCE';
        l_old_value := :OLD.SURFACING_HAUL_DISTANCE;
        l_new_value := :NEW.SURFACING_HAUL_DISTANCE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.SURFACING_MATERIAL_TYPE_CODE, 0) != NVL(:OLD.SURFACING_MATERIAL_TYPE_CODE, 0) THEN
        l_column_name := 'SURFACING_MATERIAL_TYPE_CODE';
        l_old_value := :OLD.SURFACING_MATERIAL_TYPE_CODE;
        l_new_value := :NEW.SURFACING_MATERIAL_TYPE_CODE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.SURFACING_VOLUME, 0) != NVL(:OLD.SURFACING_VOLUME, 0) THEN
        l_column_name := 'SURFACING_VOLUME';
        l_old_value := :OLD.SURFACING_VOLUME;
        l_new_value := :NEW.SURFACING_VOLUME;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.SURFACING_COST_PER_KM, 0) != NVL(:OLD.SURFACING_COST_PER_KM, 0) THEN
        l_column_name := 'SURFACING_COST_PER_KM';
        l_old_value := :OLD.SURFACING_COST_PER_KM;
        l_new_value := :NEW.SURFACING_COST_PER_KM;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.TOTAL_SECTION_COST, 0) != NVL(:OLD.TOTAL_SECTION_COST, 0) THEN
        l_column_name := 'TOTAL_SECTION_COST';
        l_old_value := :OLD.TOTAL_SECTION_COST;
        l_new_value := :NEW.TOTAL_SECTION_COST;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.SECTION_BUILT_IND, 0) != NVL(:OLD.SECTION_BUILT_IND, 0) THEN
        l_column_name := 'SECTION_BUILT_IND';
        l_old_value := :OLD.SECTION_BUILT_IND;
        l_new_value := :NEW.SECTION_BUILT_IND;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.AMORTIZED_PCT, 0) != NVL(:OLD.AMORTIZED_PCT, 0) THEN
        l_column_name := 'AMORTIZED_PCT';
        l_old_value := :OLD.AMORTIZED_PCT;
        l_new_value := :NEW.AMORTIZED_PCT;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.CROWN_PCT, 0) != NVL(:OLD.CROWN_PCT, 0) THEN
        l_column_name := 'CROWN_PCT';
        l_old_value := :OLD.CROWN_PCT;
        l_new_value := :NEW.CROWN_PCT;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.PARTIAL_PCT, 0) != NVL(:OLD.PARTIAL_PCT, 0) THEN
        l_column_name := 'PARTIAL_PCT';
        l_old_value := :OLD.PARTIAL_PCT;
        l_new_value := :NEW.PARTIAL_PCT;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

    END IF; --l_audit_event_id is not null

  ELSIF DELETING THEN
    l_ecas_id := :OLD.ecas_id;
	l_temp_business_id := 'Road:'||SUBSTR(:OLD.ROAD_NAME,1,20)||' Start:'||:OLD.STATION_START_POINT||' End:'||:OLD.STATION_END_POINT;
	l_business_id := SUBSTR(l_temp_business_id,1,50);
    Pkg_Ecas_Audit.GET_USERID(l_userid
                            , 'DEL'
                            , l_ecas_id
                            , SYSDATE
                            , l_error_message);

    Pkg_Ecas_Audit.CHECK_EVENT_ID(l_audit_event_id
                                , l_ecas_id
                                , 'DEL'
                                , l_userid
                                , SYSDATE
                                , l_error_message);

    IF l_audit_event_id IS NOT NULL THEN

      l_column_name := 'ROAD_DEVELOPMENT_TYPE_CODE';
      l_old_value := :OLD.ROAD_DEVELOPMENT_TYPE_CODE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'APPRAISAL_YEAR';
      l_old_value := :OLD.APPRAISAL_YEAR;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'COST_ESTIMATE_ID';
      l_old_value := :OLD.COST_ESTIMATE_ID;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'ROAD_ENGINEERING_TYPE_CODE';
      l_old_value := :OLD.ROAD_ENGINEERING_TYPE_CODE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'ENGINEERING_SECTION_ID';
      l_old_value := :OLD.ENGINEERING_SECTION_ID;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'RD_ENGINEERING_PROJ_TYPE_CODE';
      l_old_value := :OLD.RD_ENGINEERING_PROJ_TYPE_CODE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'ROAD_NAME';
      l_old_value := :OLD.ROAD_NAME;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'STATION_START_POINT';
      l_old_value := :OLD.STATION_START_POINT;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'STATION_END_POINT';
      l_old_value := :OLD.STATION_END_POINT;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'CONSTRUCTION_CATEGORY_CODE';
      l_old_value := :OLD.CONSTRUCTION_CATEGORY_CODE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'REMAINING_ROAD_WIDTH';
      l_old_value := :OLD.REMAINING_ROAD_WIDTH;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'SUBGRADE_SPOIL_LOCATION';
      l_old_value := :OLD.SUBGRADE_SPOIL_LOCATION;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'STABILIZING_MATERIAL_TYPE_CODE';
      l_old_value := :OLD.STABILIZING_MATERIAL_TYPE_CODE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'SUBGRADE_HAUL_DISTANCE';
      l_old_value := :OLD.SUBGRADE_HAUL_DISTANCE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'UPHILL_SIDE_SLOPE_PCT';
      l_old_value := :OLD.UPHILL_SIDE_SLOPE_PCT;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'TOTAL_END_HAUL_VOLUME';
      l_old_value := :OLD.TOTAL_END_HAUL_VOLUME;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'SUBGRADE_COST_PER_KM';
      l_old_value := :OLD.SUBGRADE_COST_PER_KM;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'SURFACING_REQUIRED_IND';
      l_old_value := :OLD.SURFACING_REQUIRED_IND;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'SURFACING_DEPTH';
      l_old_value := :OLD.SURFACING_DEPTH;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'SURFACING_PIT_LOCATION';
      l_old_value := :OLD.SURFACING_PIT_LOCATION;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'SURFACING_HAUL_DISTANCE';
      l_old_value := :OLD.SURFACING_HAUL_DISTANCE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'SURFACING_MATERIAL_TYPE_CODE';
      l_old_value := :OLD.SURFACING_MATERIAL_TYPE_CODE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'SURFACING_VOLUME';
      l_old_value := :OLD.SURFACING_VOLUME;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'SURFACING_COST_PER_KM';
      l_old_value := :OLD.SURFACING_COST_PER_KM;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'TOTAL_SECTION_COST';
      l_old_value := :OLD.TOTAL_SECTION_COST;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'SECTION_BUILT_IND';
      l_old_value := :OLD.SECTION_BUILT_IND;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'AMORTIZED_PCT';
      l_old_value := :OLD.AMORTIZED_PCT;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'CROWN_PCT';
      l_old_value := :OLD.CROWN_PCT;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'PARTIAL_PCT';
      l_old_value := :OLD.PARTIAL_PCT;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

    END IF; --l_audit_event_id is not null

  END IF; --if updating or deleting

  EXCEPTION
    WHEN TOO_MANY_ROWS THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.record.toomany:TRG_ADS_ENG_SECTION_COST1,'||TRIM(l_table_name)||','||SQLCODE||';';
    WHEN DUP_VAL_ON_INDEX THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.record.duplicate:TRG_ADS_ENG_SECTION_COST1,'||TRIM(l_table_name)||','||SQLCODE||';';
    WHEN NO_DATA_FOUND THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.record.invalid:TRG_ADS_ENG_SECTION_COST1,'||TRIM(l_table_name)||';';
    WHEN OTHERS THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.unexpected:TRG_ADS_ENG_SECTION_COST1,'||SQLCODE||','||SQLERRM||';';
    RAISE;
END TRG_ADS_ENG_SECTION_COST1;




/
ALTER TRIGGER "THE"."TRG_ADS_ENG_SECTION_COST1" ENABLE;

  CREATE OR REPLACE EDITIONABLE TRIGGER "THE"."TRG_ADS_SUPPORT_DOCUMENT1" 
 AFTER UPDATE OR INSERT OR DELETE
   ON ADS_SUPPORT_DOCUMENT

FOR EACH ROW

DECLARE

  l_table_name      ECAS_AUDIT_DETAIL.TABLE_NAME%TYPE;
  l_column_name     ECAS_AUDIT_DETAIL.COLUMN_NAME%TYPE;
  l_old_value       ECAS_AUDIT_DETAIL.OLD_VALUE%TYPE;
  l_new_value       ECAS_AUDIT_DETAIL.NEW_VALUE%TYPE;
  l_ecas_id         ECAS_AUDIT_DETAIL.ECAS_ID%TYPE;
  l_audit_event_id  ECAS_AUDIT_DETAIL.AUDIT_EVENT_ID%TYPE;
  l_userid          ECAS_AUDIT_DETAIL.ENTRY_USERID%TYPE;
  l_business_id     ECAS_AUDIT_DETAIL.BUSINESS_IDENTIFIER%TYPE;
  l_file_name       ECAS_SUBMITTED_FILE.FILE_NAME%TYPE;

  l_error_message VARCHAR2(1500);

BEGIN

  l_table_name := 'ADS_SUPPORT_DOCUMENT';
  l_audit_event_id := NULL;
  l_business_id := 'Doc Type: '||:NEW.APPRAISAL_DOCUMENT_TYPE_CODE;

  IF INSERTING THEN
    l_ecas_id := :NEW.ecas_id;
    l_userid := :NEW.update_userid;

    IF :NEW.ECAS_SUBMITTED_FILE_ID IS NOT NULL THEN
      SELECT FILE_NAME
      INTO l_file_name
      FROM ECAS_SUBMITTED_FILE
      WHERE ECAS_ID = l_ecas_id
      AND ECAS_SUBMITTED_FILE_ID = :NEW.ECAS_SUBMITTED_FILE_ID;
	ELSE

	 l_file_name := ' ';
	END IF;
	l_business_id := l_business_id||' File: '||SUBSTR(l_file_name,1,28);

    Pkg_Ecas_Audit.CHECK_EVENT_ID(l_audit_event_id
                                , l_ecas_id
                                , 'UPD' --INSERTS are considered updates from a business standpoint
                                , l_userid
                                , SYSDATE
                                , l_error_message);

    IF l_audit_event_id IS NOT NULL THEN
      l_column_name := 'DOCUMENT_ID';
      l_old_value := NULL;
      l_new_value := :NEW.DOCUMENT_ID;
      Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                    , l_audit_event_id
                                    , l_table_name
                                    , l_column_name
                                    , l_old_value
                                    , l_new_value
                                    , l_userid
 								    , l_business_id
                                    , l_error_message);

      l_column_name := 'TRANSMISSION_TYPE_CODE';
      l_old_value := NULL;
      l_new_value := :NEW.TRANSMISSION_TYPE_CODE;
      Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                    , l_audit_event_id
                                    , l_table_name
                                    , l_column_name
                                    , l_old_value
                                    , l_new_value
                                    , l_userid
 								    , l_business_id
                                    , l_error_message);
      l_column_name := 'APPRAISAL_DOCUMENT_TYPE_CODE';
      l_old_value := NULL;
      l_new_value := :NEW.APPRAISAL_DOCUMENT_TYPE_CODE;
      Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                    , l_audit_event_id
                                    , l_table_name
                                    , l_column_name
                                    , l_old_value
                                    , l_new_value
                                    , l_userid
 								    , l_business_id
                                    , l_error_message);


      IF :NEW.ZIP_FILE_IND IS NOT NULL THEN
        l_column_name := 'ZIP_FILE_IND';
        l_old_value := NULL;
        l_new_value := :NEW.ZIP_FILE_IND;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 								      , l_business_id
                                      , l_error_message);
      END IF;


      IF :NEW.ECAS_SUBMITTED_FILE_ID IS NOT NULL THEN
        l_column_name := 'ECAS_SUBMITTED_FILE_ID';
        l_old_value := NULL;
        l_new_value := :NEW.ECAS_SUBMITTED_FILE_ID;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 								      , l_business_id
                                      , l_error_message);
      END IF;
    END IF; --l_audit_event_id is not null

  ELSIF UPDATING THEN
    l_ecas_id := :OLD.ecas_id;
    l_userid := :NEW.update_userid;

    IF :NEW.ECAS_SUBMITTED_FILE_ID IS NOT NULL THEN
	  SELECT FILE_NAME
	  INTO l_file_name
	  FROM ECAS_SUBMITTED_FILE
	  WHERE ECAS_ID = l_ecas_id
	  AND ECAS_SUBMITTED_FILE_ID = :NEW.ECAS_SUBMITTED_FILE_ID;
	ELSE
	 l_file_name := ' ';
	END IF;

	l_business_id := l_business_id||' File: '||SUBSTR(l_file_name,1,28);

    Pkg_Ecas_Audit.CHECK_EVENT_ID(l_audit_event_id
                                , l_ecas_id
                                , 'UPD'
                                , l_userid
                                , SYSDATE
                                , l_error_message);

    IF l_audit_event_id IS NOT NULL THEN

      IF :NEW.DOCUMENT_ID != :OLD.DOCUMENT_ID THEN
        l_column_name := 'DOCUMENT_ID';
        l_old_value := :OLD.DOCUMENT_ID;
        l_new_value := :NEW.DOCUMENT_ID;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 								      , l_business_id
                                      , l_error_message);
      END IF;

      IF :NEW.TRANSMISSION_TYPE_CODE != :OLD.TRANSMISSION_TYPE_CODE THEN
        l_column_name := 'TRANSMISSION_TYPE_CODE';
        l_old_value := :OLD.TRANSMISSION_TYPE_CODE;
        l_new_value := :NEW.TRANSMISSION_TYPE_CODE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 								      , l_business_id
                                      , l_error_message);
      END IF;

      IF :NEW.APPRAISAL_DOCUMENT_TYPE_CODE != :OLD.APPRAISAL_DOCUMENT_TYPE_CODE THEN
        l_column_name := 'APPRAISAL_DOCUMENT_TYPE_CODE';
        l_old_value := :OLD.APPRAISAL_DOCUMENT_TYPE_CODE;
        l_new_value := :NEW.APPRAISAL_DOCUMENT_TYPE_CODE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 								      , l_business_id
                                      , l_error_message);
      END IF;


      IF NVL(:NEW.ZIP_FILE_IND, 0) != NVL(:OLD.ZIP_FILE_IND, 0) THEN
        l_column_name := 'ZIP_FILE_IND';
        l_old_value := :OLD.ZIP_FILE_IND;
        l_new_value := :NEW.ZIP_FILE_IND;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 								      , l_business_id
                                      , l_error_message);
      END IF;


      IF NVL(:NEW.ECAS_SUBMITTED_FILE_ID, 0) != NVL(:OLD.ECAS_SUBMITTED_FILE_ID, 0) THEN
        l_column_name := 'ECAS_SUBMITTED_FILE_ID';
        l_old_value := :OLD.ECAS_SUBMITTED_FILE_ID;
        l_new_value := :NEW.ECAS_SUBMITTED_FILE_ID;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 								      , l_business_id
                                      , l_error_message);
      END IF;
    END IF; --l_audit_event_id is not null

  ELSIF DELETING THEN
    l_ecas_id := :OLD.ecas_id;
    l_business_id := 'Doc Type: '||:OLD.APPRAISAL_DOCUMENT_TYPE_CODE;

    IF :OLD.ECAS_SUBMITTED_FILE_ID IS NOT NULL THEN
	  SELECT FILE_NAME
	  INTO l_file_name
	  FROM ECAS_SUBMITTED_FILE
	  WHERE ECAS_ID = l_ecas_id
	  AND ECAS_SUBMITTED_FILE_ID = :OLD.ECAS_SUBMITTED_FILE_ID;
	ELSE
	 l_file_name := ' ';
	END IF;

	l_business_id := l_business_id||' File: '||SUBSTR(l_file_name,1,28);

    Pkg_Ecas_Audit.GET_USERID(l_userid
                            , 'UPD' --DELETES are considered updates from a business standpoint
                            , l_ecas_id
                            , SYSDATE
                            , l_error_message);

    Pkg_Ecas_Audit.CHECK_EVENT_ID(l_audit_event_id
                                , l_ecas_id
                                , 'UPD' --DELETES are considered updates from a business standpoint
                                , l_userid
                                , SYSDATE
                                , l_error_message);

    IF l_audit_event_id IS NOT NULL THEN

      l_column_name := 'DOCUMENT_ID';
      l_old_value := :OLD.DOCUMENT_ID;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 								      , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'TRANSMISSION_TYPE_CODE';
      l_old_value := :OLD.TRANSMISSION_TYPE_CODE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 								      , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'APPRAISAL_DOCUMENT_TYPE_CODE';
      l_old_value := :OLD.APPRAISAL_DOCUMENT_TYPE_CODE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 								      , l_business_id
                                      , l_error_message);
      END IF;


      l_column_name := 'ZIP_FILE_IND';
      l_old_value := :OLD.ZIP_FILE_IND;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 								      , l_business_id
                                      , l_error_message);
      END IF;


      l_column_name := 'ECAS_SUBMITTED_FILE_ID';
      l_old_value := :OLD.ECAS_SUBMITTED_FILE_ID;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 								      , l_business_id
                                      , l_error_message);
      END IF;
    END IF; --l_audit_event_id is not null
  END IF; --if inserting or updating or deleting

  EXCEPTION
    WHEN TOO_MANY_ROWS THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.record.toomany:TRG_ADS_SUPPORT_DOCUMENT1,'||TRIM(l_table_name)||','||SQLCODE||';';
    WHEN DUP_VAL_ON_INDEX THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.record.duplicate:TRG_ADS_SUPPORT_DOCUMENT1,'||TRIM(l_table_name)||','||SQLCODE||';';
    WHEN NO_DATA_FOUND THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.record.invalid:TRG_ADS_SUPPORT_DOCUMENT1,'||TRIM(l_table_name)||';';
    WHEN OTHERS THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.unexpected:TRG_ADS_SUPPORT_DOCUMENT1,'||SQLCODE||','||SQLERRM||';';
    RAISE;
END TRG_ADS_SUPPORT_DOCUMENT1;


/
ALTER TRIGGER "THE"."TRG_ADS_SUPPORT_DOCUMENT1" ENABLE;

  CREATE OR REPLACE EDITIONABLE TRIGGER "THE"."TRG_ADS_BCTS_MAND_UTILIZATION1" 
AFTER  DELETE  OR UPDATE OR INSERT
ON ADS_BCTS_MANDATORY_UTILIZATION
REFERENCING NEW AS NEW OLD AS OLD
FOR EACH ROW
DECLARE
  l_table_name      ECAS_AUDIT_DETAIL.TABLE_NAME%TYPE;
  l_column_name     ECAS_AUDIT_DETAIL.COLUMN_NAME%TYPE;
  l_old_value       ECAS_AUDIT_DETAIL.OLD_VALUE%TYPE;
  l_new_value       ECAS_AUDIT_DETAIL.NEW_VALUE%TYPE;
  l_ecas_id         ECAS_AUDIT_DETAIL.ECAS_ID%TYPE;
  l_audit_event_id  ECAS_AUDIT_DETAIL.AUDIT_EVENT_ID%TYPE;
  l_userid          ECAS_AUDIT_DETAIL.ENTRY_USERID%TYPE;
  l_business_id     ECAS_AUDIT_DETAIL.BUSINESS_IDENTIFIER%TYPE;
  l_error_message VARCHAR2(1500);

BEGIN

  l_table_name := 'ADS_BCTS_MANDATORY_UTILIZATION';
  l_audit_event_id := NULL;

  IF UPDATING OR INSERTING THEN
    l_userid := :NEW.update_userid;
    l_ecas_id := :NEW.ecas_id;
	l_business_id := 'Species Group: '||:NEW.MANDATORY_UTIL_SPECIES_GP_CODE;
    Pkg_Ecas_Audit.CHECK_EVENT_ID(l_audit_event_id
                                , l_ecas_id
                                , 'UPD'
                                , l_userid
                                , SYSDATE
                                , l_error_message);

    IF l_audit_event_id IS NOT NULL THEN
      IF NVL(:NEW.MANDATORY_UTIL_SPECIES_GP_CODE,0) != NVL(:OLD.MANDATORY_UTIL_SPECIES_GP_CODE,0) THEN
        l_column_name := 'MANDATORY_UTIL_SPECIES_GP_CODE';
        l_old_value := :OLD.MANDATORY_UTIL_SPECIES_GP_CODE;
        l_new_value := :NEW.MANDATORY_UTIL_SPECIES_GP_CODE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      IF NVL(:NEW.MISCELLANEOUS_PRODUCT, 0) != NVL(:OLD.MISCELLANEOUS_PRODUCT, 0) THEN
        l_column_name := 'MISCELLANEOUS_PRODUCT';
        l_old_value := :OLD.MISCELLANEOUS_PRODUCT;
        l_new_value := :NEW.MISCELLANEOUS_PRODUCT;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

    END IF; --l_audit_event_id is not null

  ELSIF DELETING THEN
    l_ecas_id := :OLD.ecas_id;
	l_business_id := 'Species Group: '||:NEW.MANDATORY_UTIL_SPECIES_GP_CODE;
    Pkg_Ecas_Audit.GET_USERID(l_userid
                            , 'DEL'
                            , l_ecas_id
                            , SYSDATE
                            , l_error_message);

    Pkg_Ecas_Audit.CHECK_EVENT_ID(l_audit_event_id
                                , l_ecas_id
                                , 'DEL'
                                , l_userid
                                , SYSDATE
                                , l_error_message);

    IF l_audit_event_id IS NOT NULL THEN
      l_column_name := 'MANDATORY_UTIL_SPECIES_GP_CODE';
      l_old_value := :OLD.MANDATORY_UTIL_SPECIES_GP_CODE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'MISCELLANEOUS_PRODUCT';
      l_old_value := :OLD.MISCELLANEOUS_PRODUCT;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

    END IF; --l_audit_event_id is not null

  END IF; --if updating or deleting

  EXCEPTION
    WHEN TOO_MANY_ROWS THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.record.toomany:TRG_ADS_BCTS_MAND_UTILIZATION1,'||TRIM(l_table_name)||','||SQLCODE||';';
    WHEN DUP_VAL_ON_INDEX THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.record.duplicate:TRG_ADS_BCTS_MAND_UTILIZATION1,'||TRIM(l_table_name)||','||SQLCODE||';';
    WHEN NO_DATA_FOUND THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.record.invalid:TRG_ADS_BCTS_MAND_UTILIZATION1,'||TRIM(l_table_name)||';';
    WHEN OTHERS THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.unexpected:TRG_ADS_BCTS_MAND_UTILIZATION1,'||SQLCODE||','||SQLERRM||';';
    RAISE;

END TRG_ADS_BCTS_MAND_UTILIZATION1;



/
ALTER TRIGGER "THE"."TRG_ADS_BCTS_MAND_UTILIZATION1" ENABLE;

  CREATE OR REPLACE EDITIONABLE TRIGGER "THE"."TRG_ADS_CUTBLOCK_AUTH_DETAIL1" 
 AFTER UPDATE OR
	   DELETE OR INSERT
   ON ADS_CUTTING_AUTHORITY_DETAIL

FOR EACH ROW

DECLARE

  l_table_name      ECAS_AUDIT_DETAIL.TABLE_NAME%TYPE;
  l_column_name     ECAS_AUDIT_DETAIL.COLUMN_NAME%TYPE;
  l_old_value       ECAS_AUDIT_DETAIL.OLD_VALUE%TYPE;
  l_new_value       ECAS_AUDIT_DETAIL.NEW_VALUE%TYPE;
  l_ecas_id         ECAS_AUDIT_DETAIL.ECAS_ID%TYPE;
  l_audit_event_id  ECAS_AUDIT_DETAIL.AUDIT_EVENT_ID%TYPE;
  l_userid          ECAS_AUDIT_DETAIL.ENTRY_USERID%TYPE;
  l_business_id     ECAS_AUDIT_DETAIL.BUSINESS_IDENTIFIER%TYPE;

  l_error_message VARCHAR2(1500);

BEGIN

  l_table_name := 'ADS_CUTTING_AUTHORITY_DETAIL';
  l_audit_event_id := NULL;

  IF UPDATING OR INSERTING THEN
    l_userid := :NEW.update_userid;
    l_ecas_id := :NEW.ecas_id;
	l_business_id := 'Cut Block: '||:NEW.CUT_BLOCK_ID||' Harvest Method: '||:NEW.APPRAISAL_HARVEST_METHOD_CODE;

    Pkg_Ecas_Audit.CHECK_EVENT_ID(l_audit_event_id
                                , l_ecas_id
                                , 'UPD'
                                , l_userid
                                , SYSDATE
                                , l_error_message);

    IF l_audit_event_id IS NOT NULL THEN

      IF NVL(:NEW.FOREST_FILE_ID,0) != NVL(:OLD.FOREST_FILE_ID,0) THEN
        l_column_name := 'FOREST_FILE_ID';
        l_old_value := :OLD.FOREST_FILE_ID;
        l_new_value := :NEW.FOREST_FILE_ID;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.CUTTING_PERMIT_ID,0) != NVL(:OLD.CUTTING_PERMIT_ID,0) THEN
        l_column_name := 'CUTTING_PERMIT_ID';
        l_old_value := :OLD.CUTTING_PERMIT_ID;
        l_new_value := :NEW.CUTTING_PERMIT_ID;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.CUT_BLOCK_ID,0) != NVL(:OLD.CUT_BLOCK_ID,0) THEN
        l_column_name := 'CUT_BLOCK_ID';
        l_old_value := :OLD.CUT_BLOCK_ID;
        l_new_value := :NEW.CUT_BLOCK_ID;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.APPRAISAL_HARVEST_METHOD_CODE,0) != NVL(:OLD.APPRAISAL_HARVEST_METHOD_CODE,0) THEN
        l_column_name := 'APPRAISAL_HARVEST_METHOD_CODE';
        l_old_value := :OLD.APPRAISAL_HARVEST_METHOD_CODE;
        l_new_value := :NEW.APPRAISAL_HARVEST_METHOD_CODE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.BLOCK_VOLUME, 0) != NVL(:OLD.BLOCK_VOLUME, 0) THEN
        l_column_name := 'BLOCK_VOLUME';
        l_old_value := :OLD.BLOCK_VOLUME;
        l_new_value := :NEW.BLOCK_VOLUME;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.WATER_TRANSPORT_CODE, 0) != NVL(:OLD.WATER_TRANSPORT_CODE, 0) THEN
        l_column_name := 'WATER_TRANSPORT_CODE';
        l_old_value := :OLD.WATER_TRANSPORT_CODE;
        l_new_value := :NEW.WATER_TRANSPORT_CODE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.PT_OF_APPRAISAL_ST,0) != NVL(:OLD.PT_OF_APPRAISAL_ST,0) THEN
        l_column_name := 'PT_OF_APPRAISAL_ST';
        l_old_value := :OLD.PT_OF_APPRAISAL_ST;
        l_new_value := :NEW.PT_OF_APPRAISAL_ST;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.PT_OF_ORIGIN1,0) != NVL(:OLD.PT_OF_ORIGIN1,0) THEN
        l_column_name := 'PT_OF_ORIGIN1';
        l_old_value := :OLD.PT_OF_ORIGIN1;
        l_new_value := :NEW.PT_OF_ORIGIN1;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(Pkg_Sil_Date_Conversion.CONVERT_TO_CHAR(:NEW.EFFECTIVE_DATE1, 'YYYY-MM-DD', 'Y'), 0)
           != NVL(Pkg_Sil_Date_Conversion.CONVERT_TO_CHAR(:OLD.EFFECTIVE_DATE1, 'YYYY-MM-DD', 'Y'), 0) THEN
        l_column_name := 'EFFECTIVE_DATE1';
        l_old_value := :OLD.EFFECTIVE_DATE1;
        l_new_value := :NEW.EFFECTIVE_DATE1;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.PT_OF_ORIGIN2, 0) != NVL(:OLD.PT_OF_ORIGIN2, 0) THEN
        l_column_name := 'PT_OF_ORIGIN2';
        l_old_value := :OLD.PT_OF_ORIGIN2;
        l_new_value := :NEW.PT_OF_ORIGIN2;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(Pkg_Sil_Date_Conversion.CONVERT_TO_CHAR(:NEW.EFFECTIVE_DATE2, 'YYYY-MM-DD', 'Y'), 0)
           != NVL(Pkg_Sil_Date_Conversion.CONVERT_TO_CHAR(:OLD.EFFECTIVE_DATE2, 'YYYY-MM-DD', 'Y'), 0) THEN
        l_column_name := 'EFFECTIVE_DATE2';
        l_old_value := :OLD.EFFECTIVE_DATE2;
        l_new_value := :NEW.EFFECTIVE_DATE2;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.TRUCK_HAUL_DISTANCE, 0) != NVL(:OLD.TRUCK_HAUL_DISTANCE, 0) THEN
        l_column_name := 'TRUCK_HAUL_DISTANCE';
        l_old_value := :OLD.TRUCK_HAUL_DISTANCE;
        l_new_value := :NEW.TRUCK_HAUL_DISTANCE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.CREW_TRANSPORT_DISTANCE, 0) != NVL(:OLD.CREW_TRANSPORT_DISTANCE, 0) THEN
        l_column_name := 'CREW_TRANSPORT_DISTANCE';
        l_old_value := :OLD.CREW_TRANSPORT_DISTANCE;
        l_new_value := :NEW.CREW_TRANSPORT_DISTANCE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.CREW_MARSHAL_POINT, 0) != NVL(:OLD.CREW_MARSHAL_POINT, 0) THEN
        l_column_name := 'CREW_MARSHAL_POINT';
        l_old_value := :OLD.CREW_MARSHAL_POINT;
        l_new_value := :NEW.CREW_MARSHAL_POINT;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.ISOLATED_IND, 0) != NVL(:OLD.ISOLATED_IND, 0) THEN
        l_column_name := 'ISOLATED_IND';
        l_old_value := :OLD.ISOLATED_IND;
        l_new_value := :NEW.ISOLATED_IND;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.ROAD_MAINTENANCE_IND, 0) != NVL(:OLD.ROAD_MAINTENANCE_IND, 0) THEN
        l_column_name := 'ROAD_MAINTENANCE_IND';
        l_old_value := :OLD.ROAD_MAINTENANCE_IND;
        l_new_value := :NEW.ROAD_MAINTENANCE_IND;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.APPRAISAL_LOG_DUMP_LOCN_CODE, 0) != NVL(:OLD.APPRAISAL_LOG_DUMP_LOCN_CODE, 0) THEN
        l_column_name := 'APPRAISAL_LOG_DUMP_LOCN_CODE';
        l_old_value := :OLD.APPRAISAL_LOG_DUMP_LOCN_CODE;
        l_new_value := :NEW.APPRAISAL_LOG_DUMP_LOCN_CODE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.OTHER_APPRAISAL_LOG_DUMP_DESC, 0) != NVL(:OLD.OTHER_APPRAISAL_LOG_DUMP_DESC, 0) THEN
        l_column_name := 'OTHER_APPRAISAL_LOG_DUMP_DESC';
        l_old_value := :OLD.OTHER_APPRAISAL_LOG_DUMP_DESC;
        l_new_value := :NEW.OTHER_APPRAISAL_LOG_DUMP_DESC;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.ADS_LOCATION_CODE, 0) != NVL(:OLD.ADS_LOCATION_CODE, 0) THEN
        l_column_name := 'ADS_LOCATION_CODE';
        l_old_value := :OLD.ADS_LOCATION_CODE;
        l_new_value := :NEW.ADS_LOCATION_CODE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.ADS_LOCATION_DISTANCE, 0) != NVL(:OLD.ADS_LOCATION_DISTANCE, 0) THEN
        l_column_name := 'ADS_LOCATION_DISTANCE';
        l_old_value := :OLD.ADS_LOCATION_DISTANCE;
        l_new_value := :NEW.ADS_LOCATION_DISTANCE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

    END IF; --l_audit_event_id is not null

  ELSIF DELETING THEN
    l_ecas_id := :OLD.ecas_id;
	l_business_id := 'Cut Block: '||:OLD.CUT_BLOCK_ID||' Harvest Method: '||:OLD.APPRAISAL_HARVEST_METHOD_CODE;
    Pkg_Ecas_Audit.GET_USERID(l_userid
                            , 'DEL'
                            , l_ecas_id
                            , SYSDATE
                            , l_error_message);

    Pkg_Ecas_Audit.CHECK_EVENT_ID(l_audit_event_id
                                , l_ecas_id
                                , 'DEL'
                                , l_userid
                                , SYSDATE
                                , l_error_message);

    IF l_audit_event_id IS NOT NULL THEN

      l_column_name := 'FOREST_FILE_ID';
      l_old_value := :OLD.FOREST_FILE_ID;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'CUTTING_PERMIT_ID';
      l_old_value := :OLD.CUTTING_PERMIT_ID;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'CUT_BLOCK_ID';
      l_old_value := :OLD.CUT_BLOCK_ID;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'APPRAISAL_HARVEST_METHOD_CODE';
      l_old_value := :OLD.APPRAISAL_HARVEST_METHOD_CODE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'BLOCK_VOLUME';
      l_old_value := :OLD.BLOCK_VOLUME;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'WATER_TRANSPORT_CODE';
      l_old_value := :OLD.WATER_TRANSPORT_CODE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'PT_OF_APPRAISAL_ST';
      l_old_value := :OLD.PT_OF_APPRAISAL_ST;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'PT_OF_ORIGIN1';
      l_old_value := :OLD.PT_OF_ORIGIN1;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'EFFECTIVE_DATE1';
      l_old_value := :OLD.EFFECTIVE_DATE1;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'PT_OF_ORIGIN2';
      l_old_value := :OLD.PT_OF_ORIGIN2;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'EFFECTIVE_DATE2';
      l_old_value := :OLD.EFFECTIVE_DATE2;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'TRUCK_HAUL_DISTANCE';
      l_old_value := :OLD.TRUCK_HAUL_DISTANCE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'CREW_TRANSPORT_DISTANCE';
      l_old_value := :OLD.CREW_TRANSPORT_DISTANCE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'CREW_MARSHAL_POINT';
      l_old_value := :OLD.CREW_MARSHAL_POINT;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'ISOLATED_IND';
      l_old_value := :OLD.ISOLATED_IND;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'ROAD_MAINTENANCE_IND';
      l_old_value := :OLD.ROAD_MAINTENANCE_IND;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'APPRAISAL_LOG_DUMP_LOCN_CODE';
      l_old_value := :OLD.APPRAISAL_LOG_DUMP_LOCN_CODE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'OTHER_APPRAISAL_LOG_DUMP_DESC';
      l_old_value := :OLD.OTHER_APPRAISAL_LOG_DUMP_DESC;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'ADS_LOCATION_CODE';
      l_old_value := :OLD.ADS_LOCATION_CODE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'ADS_LOCATION_DISTANCE';
      l_old_value := :OLD.ADS_LOCATION_DISTANCE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;


    END IF; --l_audit_event_id is not null

  END IF; --if updating or deleting

  EXCEPTION
    WHEN TOO_MANY_ROWS THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.record.toomany:TRG_ADS_CUTBLOCK_AUTH_DETAIL1,'||TRIM(l_table_name)||','||SQLCODE||';';
    WHEN DUP_VAL_ON_INDEX THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.record.duplicate:TRG_ADS_CUTBLOCK_AUTH_DETAIL1,'||TRIM(l_table_name)||','||SQLCODE||';';
    WHEN NO_DATA_FOUND THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.record.invalid:TRG_ADS_CUTBLOCK_AUTH_DETAIL1,'||TRIM(l_table_name)||';';
    WHEN OTHERS THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.unexpected:TRG_ADS_CUTBLOCK_AUTH_DETAIL1,'||SQLCODE||','||SQLERRM||';';
    RAISE;
END TRG_ADS_CUTBLOCK_AUTH_DETAIL1;




/
ALTER TRIGGER "THE"."TRG_ADS_CUTBLOCK_AUTH_DETAIL1" ENABLE;

  CREATE OR REPLACE EDITIONABLE TRIGGER "THE"."TRG_ADS_SUBMITTED_TIMBER_MARK1" 
 AFTER UPDATE OR
	   DELETE OR INSERT
   ON ADS_SUBMITTED_TIMBER_MARK

FOR EACH ROW

DECLARE

  l_table_name      ECAS_AUDIT_DETAIL.TABLE_NAME%TYPE;
  l_column_name     ECAS_AUDIT_DETAIL.COLUMN_NAME%TYPE;
  l_old_value       ECAS_AUDIT_DETAIL.OLD_VALUE%TYPE;
  l_new_value       ECAS_AUDIT_DETAIL.NEW_VALUE%TYPE;
  l_ecas_id         ECAS_AUDIT_DETAIL.ECAS_ID%TYPE;
  l_audit_event_id  ECAS_AUDIT_DETAIL.AUDIT_EVENT_ID%TYPE;
  l_userid          ECAS_AUDIT_DETAIL.ENTRY_USERID%TYPE;
  l_business_id     ECAS_AUDIT_DETAIL.BUSINESS_IDENTIFIER%TYPE;

  l_error_message VARCHAR2(1500);

BEGIN

  l_table_name := 'ADS_SUBMITTED_TIMBER_MARK';
  l_audit_event_id := NULL;

  IF UPDATING OR INSERTING THEN
    l_userid := :NEW.update_userid;
    l_ecas_id := :NEW.ecas_id;
	l_business_id := 'Mark: '||:NEW.timber_mark;

    Pkg_Ecas_Audit.CHECK_EVENT_ID(l_audit_event_id
                                , l_ecas_id
                                , 'UPD'
                                , l_userid
                                , SYSDATE
                                , l_error_message);

    IF l_audit_event_id IS NOT NULL THEN

      IF NVL(:NEW.TIMBER_MARK,0) != NVL(:OLD.TIMBER_MARK,0) THEN
        l_column_name := 'TIMBER_MARK';
        l_old_value := :OLD.TIMBER_MARK;
        l_new_value := :NEW.TIMBER_MARK;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.PRIMARY_MARK_IND,0) != NVL(:OLD.PRIMARY_MARK_IND,0) THEN
        l_column_name := 'PRIMARY_MARK_IND';
        l_old_value := :OLD.PRIMARY_MARK_IND;
        l_new_value := :NEW.PRIMARY_MARK_IND;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.MARK_CRUISE_VOLUME, 0) != NVL(:OLD.MARK_CRUISE_VOLUME, 0) THEN
        l_column_name := 'MARK_CRUISE_VOLUME';
        l_old_value := :OLD.MARK_CRUISE_VOLUME;
        l_new_value := :NEW.MARK_CRUISE_VOLUME;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

    END IF; --l_audit_event_id is not null

  ELSIF DELETING THEN
    l_ecas_id := :OLD.ecas_id;
	l_business_id := 'Mark: '||:OLD.timber_mark;
    Pkg_Ecas_Audit.GET_USERID(l_userid
                            , 'DEL'
                            , l_ecas_id
                            , SYSDATE
                            , l_error_message);

    Pkg_Ecas_Audit.CHECK_EVENT_ID(l_audit_event_id
                                , l_ecas_id
                                , 'DEL'
                                , l_userid
                                , SYSDATE
                                , l_error_message);

    IF l_audit_event_id IS NOT NULL THEN

      l_column_name := 'TIMBER_MARK';
      l_old_value := :OLD.TIMBER_MARK;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'PRIMARY_MARK_IND';
      l_old_value := :OLD.PRIMARY_MARK_IND;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'MARK_CRUISE_VOLUME';
      l_old_value := :OLD.MARK_CRUISE_VOLUME;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

    END IF; --l_audit_event_id is not null

  END IF; --if updating or deleting

  EXCEPTION
    WHEN TOO_MANY_ROWS THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.record.toomany:TRG_ADS_SUBMITTED_TIMBER_MARK1,'||TRIM(l_table_name)||','||SQLCODE||';';
    WHEN DUP_VAL_ON_INDEX THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.record.duplicate:TRG_ADS_SUBMITTED_TIMBER_MARK1,'||TRIM(l_table_name)||','||SQLCODE||';';
    WHEN NO_DATA_FOUND THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.record.invalid:TRG_ADS_SUBMITTED_TIMBER_MARK1,'||TRIM(l_table_name)||';';
    WHEN OTHERS THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.unexpected:TRG_ADS_SUBMITTED_TIMBER_MARK1,'||SQLCODE||','||SQLERRM||';';
    RAISE;
END TRG_ADS_SUBMITTED_TIMBER_MARK1;




/
ALTER TRIGGER "THE"."TRG_ADS_SUBMITTED_TIMBER_MARK1" ENABLE;

  CREATE OR REPLACE EDITIONABLE TRIGGER "THE"."CBLK_B_I_U_D" 
  BEFORE DELETE OR INSERT OR UPDATE
  ON CUT_BLOCK
  REFERENCING NEW AS NEW OLD AS OLD
  FOR EACH ROW
DECLARE
/******************************************************************************
   PURPOSE:    Audits changes to column values.

   REVISIONS:
   Ver        Date        Author           Description
   ---------  ----------  ---------------  ------------------------------------
   1.0        2/6/2006    Pangaea          1. Created this trigger.
              5/2/2006    S Lewis          1. Modified trigger to match schema update.
              14/2/2018   CGI              Included the CUT_BLOCK_GUID.
			  16/July/2020 CGI             Included IS_WASTE_ASSESSMENT_REQUIRED column
******************************************************************************/
  v_action                           cut_block_audit.action%TYPE;
  v_db_user                          VARCHAR2(120);
BEGIN
  IF INSERTING THEN
    v_action := 'INSERT';
  ELSIF UPDATING THEN
    v_action := 'UPDATE';
  ELSIF DELETING THEN
    v_action := 'DELETE';
  END IF;

  --Get machinename and db userid.
  SELECT SYS_CONTEXT('USERENV', 'TERMINAL') || '\' || SYS_CONTEXT('USERENV', 'OS_USER')
    INTO v_db_user
    FROM DUAL;

  IF INSERTING THEN                                --Insert NEW values into auditing table
    INSERT INTO cut_block_audit
           (--audit_table_sequence,
		    action
          , cb_skey
          , hva_skey
          , forest_file_id
          , cutting_permit_id
          , timber_mark
          , cut_block_id
          , cut_block_guid
          , sp_exempt_ind
          , block_status_date
          , cut_block_description
          , cut_regulation_code
          , reforest_declare_type_code
          , block_status_st
          , revision_count
          , entry_userid
          , entry_timestamp
          , update_userid
          , update_timestamp
  		    , is_waste_assessment_required) -- Thursday July 16, 2020 related to FTA4.22
    VALUES (--CUT_BLOCK_AUDIT_SEQ.NEXTVAL,
	        v_action
          , :NEW.cb_skey
          , :NEW.hva_skey
          , :NEW.forest_file_id
          , :NEW.cutting_permit_id
          , :NEW.timber_mark
          , :NEW.cut_block_id
          , :NEW.cut_block_guid
          , :NEW.sp_exempt_ind
          , :NEW.block_status_date
          , :NEW.cut_block_description
          , :NEW.cut_regulation_code
          , :NEW.reforest_declare_type_code
          , :NEW.block_status_st
          , :NEW.revision_count
          , :NEW.entry_userid
          , :NEW.entry_timestamp
          , :NEW.update_userid
          , :NEW.update_timestamp
  		    , :NEW.is_waste_assessment_required); -- Thursday July 16, 2020 related to FTA4.22
  ELSIF DELETING
        OR UPDATING THEN
    INSERT INTO cut_block_audit
           (--audit_table_sequence,
		    action
          , cb_skey
          , hva_skey
          , forest_file_id
          , cutting_permit_id
          , timber_mark
          , cut_block_id
          , cut_block_guid
          , sp_exempt_ind
          , block_status_date
          , cut_block_description
          , cut_regulation_code
          , reforest_declare_type_code
          , block_status_st
          , revision_count
          , entry_userid
          , entry_timestamp
          , update_userid
          , update_timestamp
  		    , is_waste_assessment_required) -- Thursday July 16, 2020 related to FTA4.22
    VALUES (--CUT_BLOCK_AUDIT_SEQ.NEXTVAL,
	        v_action
          , :OLD.cb_skey
          , :OLD.hva_skey
          , :OLD.forest_file_id
          , :OLD.cutting_permit_id
          , :OLD.timber_mark
          , :OLD.cut_block_id
          , :OLD.cut_block_guid
          , :OLD.sp_exempt_ind
          , :OLD.block_status_date
          , :OLD.cut_block_description
          , :OLD.cut_regulation_code
          , :OLD.reforest_declare_type_code
          , :OLD.block_status_st
          , :OLD.revision_count
          , :OLD.entry_userid
          , :OLD.entry_timestamp
          , :OLD.update_userid
          , :OLD.update_timestamp
  		    , :OLD.is_waste_assessment_required); -- Thursday July 16, 2020 related to FTA4.22
  END IF;
EXCEPTION
  WHEN OTHERS THEN
    -- Consider logging the error and then re-raise
    RAISE;
END;


/
ALTER TRIGGER "THE"."CBLK_B_I_U_D" ENABLE;

  CREATE OR REPLACE EDITIONABLE TRIGGER "THE"."FTA_SYNC_CB_PAB" 
/******************************************************************************
   TRIGGER:  FTA_SYNC_CB_PAB
   PURPOSE: THIS TRIGGER WILL INSERT/UPDATE/DELETE FROM PRMT_AUTHZD_BLK
   			WHEN CUT_BLOCK IS UPDATED.
			TO BE ENABLED ONLY AFTER DATA CONVERSION IS COMPLETE.
   REVISION HISTORY
   PERSON               DATE       COMMENTS
   -----------------    ---------  --------------------------------
   M.DELLAVIOLA        2006-01-02 CREATED.
******************************************************************************/
AFTER INSERT OR UPDATE OR DELETE
  ON CUT_BLOCK
  FOR EACH ROW
DECLARE

CURSOR curHVA(p_hva_skey number) is
select a.forest_file_id, cutting_permit_id, b.timber_mark, file_type_code
from harvesting_authority a,
	 hauling_authority b,
	 harvesting_hauling_xref c,
	 prov_forest_use d
where a.hva_skey = :new.hva_skey
and c.hva_skey = a.hva_skey
and b.timber_mark = c.timber_mark
and b.forest_file_id = a.forest_file_id
and d.forest_file_id = a.forest_file_id;

v_file		   HARVESTING_AUTHORITY.FOREST_FILE_ID%TYPE;
v_cp		   HARVESTING_AUTHORITY.CUTTING_PERMIT_ID%TYPE;
v_mark		   HAULING_AUTHORITY.TIMBER_MARK%TYPE;
v_file_type	   PROV_FOREST_USE.FILE_TYPE_CODE%TYPE;

v_old_file		   HARVESTING_AUTHORITY.FOREST_FILE_ID%TYPE;
v_old_cp		   HARVESTING_AUTHORITY.CUTTING_PERMIT_ID%TYPE;
v_old_mark		   HAULING_AUTHORITY.TIMBER_MARK%TYPE;
v_old_file_type	   PROV_FOREST_USE.FILE_TYPE_CODE%TYPE;

BEGIN
  -- get file/cp/mark using HVA_skey
  open curHVA(:new.hva_skey);
  fetch curHVA into v_file, v_cp, v_mark,v_file_type;
  close curHVA;

  -- get file/cp/mark using HVA_skey
  open curHVA(:old.hva_skey);
  fetch curHVA into v_old_file, v_old_cp,v_old_mark, v_old_file_type;
  close curHVA;
    -- maintain PRMT_AUTHZD_BLK
  IF INSERTING THEN
	  if :new.hva_skey is not null
	  and v_mark != :new.timber_mark
	  and v_file  != :new.forest_file_id
	  and v_file_type in ('A02','A04','A44','A28','A29','A30') then
	        -- create PRMT_AUTHZD_BLK
			INSERT INTO PRMT_AUTHZD_BLK (
			   BLK_FOREST_FILE_ID, BLK_CUT_BLOCK_ID, FOREST_FILE_ID,
			   BLK_CUT_PERMIT_ID, CUTTING_PERMIT_ID, ENTRY_USERID,
			   ENTRY_TIMESTAMP, UPDATE_USERID, UPDATE_TIMESTAMP,
			   REVISION_COUNT)
			VALUES (
			     :new.FOREST_FILE_ID
			   , :new.CUT_BLOCK_ID
			   , v_file
			   , nvl(:new.CUTTING_PERMIT_ID,' ')
			   , v_cp
			   , :new.ENTRY_USERID
			   , :new.ENTRY_TIMESTAMP
			   , :new.UPDATE_USERID
			   , :new.UPDATE_TIMESTAMP
			   , :new.REVISION_COUNT);

  ELSIF UPDATING THEN
	    IF :old.hva_skey is not null
		and :new.hva_skey is null
		and v_old_mark != :new.timber_mark
		and v_old_file_type in ('A02','A04','A44','A28','A29','A30') then
			-- remove PAB record if block is no longer being managed
		  	DELETE FROM PRMT_AUTHZD_BLK
			WHERE BLK_FOREST_FILE_ID = :old.FOREST_FILE_ID
			AND	  BLK_CUT_PERMIT_ID = :old.CUTTING_PERMIT_ID
			AND	  BLK_CUT_BLOCK_ID = :old.CUT_BLOCK_ID
			AND	  FOREST_FILE_ID = v_file
			AND	  CUTTING_PERMIT_ID = v_cp;
		end if;
		if :old.hva_skey is null
		and :new.hva_skey is not null
		and v_mark != :new.timber_mark
		and v_file  != :new.forest_file_id
		and v_file_type in ('A02','A04','A44','A28','A29','A30') then
			    -- insert new owned by relationship
				INSERT INTO PRMT_AUTHZD_BLK (
				   BLK_FOREST_FILE_ID, BLK_CUT_BLOCK_ID, FOREST_FILE_ID,
				   BLK_CUT_PERMIT_ID, CUTTING_PERMIT_ID, ENTRY_USERID,
				   ENTRY_TIMESTAMP, UPDATE_USERID, UPDATE_TIMESTAMP,
				   REVISION_COUNT)
				VALUES (
				     :new.FOREST_FILE_ID
				   , :new.CUT_BLOCK_ID
				   , v_file
				   , nvl(:new.CUTTING_PERMIT_ID,' ')
				   , v_cp
				   , :new.ENTRY_USERID
				   , :new.ENTRY_TIMESTAMP
				   , :new.UPDATE_USERID
				   , :new.UPDATE_TIMESTAMP
				   , :new.REVISION_COUNT);
		elsif (:new.cut_block_id != :old.cut_block_id)
		or nvl(:old.hva_skey,' ') != nvl(:new.hva_skey,' ') then
			if v_mark != :new.timber_mark
			and v_file  != :new.forest_file_id
			and v_file_type in ('A02','A04','A44','A28','A29','A30') then

			    -- update PRMT_AUTHZD_BLK
			    UPDATE PRMT_AUTHZD_BLK
				SET	 FOREST_FILE_ID		 = v_file
					,CUTTING_PERMIT_ID	 = v_cp
					,BLK_CUT_BLOCK_ID	 = :new.CUT_BLOCK_ID
					,BLK_FOREST_FILE_ID	 = :new.FOREST_FILE_ID
					,BLK_CUT_PERMIT_ID	 = :new.CUTTING_PERMIT_ID
					,UPDATE_USERID		 = :new.UPDATE_USERID
					,UPDATE_TIMESTAMP	 = :new.UPDATE_TIMESTAMP
					,REVISION_COUNT 	 = :new.REVISION_COUNT
				WHERE BLK_FOREST_FILE_ID = :old.FOREST_FILE_ID
				AND	  BLK_CUT_PERMIT_ID = :old.CUTTING_PERMIT_ID
				AND	  BLK_CUT_BLOCK_ID = :old.CUT_BLOCK_ID
				AND	  FOREST_FILE_ID = v_old_file
				AND	  CUTTING_PERMIT_ID = v_old_cp;
			end if;
		elsif nvl(:old.forest_file_id,' ') != nvl(:new.forest_file_id,' ')
		or nvl(:old.cutting_permit_id, ' ') != nvl(:new.cutting_permit_id,' ') then
			if v_file_type in ('A02','A04','A44','A28','A29','A30') then

			    -- update PRMT_AUTHZD_BLK
			    UPDATE PRMT_AUTHZD_BLK
				SET	 FOREST_FILE_ID		 = v_file
					,CUTTING_PERMIT_ID	 = v_cp
					,UPDATE_USERID		 = :new.UPDATE_USERID
					,UPDATE_TIMESTAMP	 = :new.UPDATE_TIMESTAMP
					,REVISION_COUNT 	 = :new.REVISION_COUNT
				WHERE FOREST_FILE_ID = v_old_file
				AND	  CUTTING_PERMIT_ID = v_old_cp;
			end if;
		end if;
   ELSIF DELETING THEN
     -- delete PRMT_AUTHZD_BLK
   	DELETE FROM PRMT_AUTHZD_BLK
 	WHERE BLK_FOREST_FILE_ID = :old.FOREST_FILE_ID
 	AND	  BLK_CUT_PERMIT_ID = :old.CUTTING_PERMIT_ID
 	AND	  BLK_CUT_BLOCK_ID = :old.CUT_BLOCK_ID
 	AND	  FOREST_FILE_ID = v_old_file
 	AND	  CUTTING_PERMIT_ID = v_old_cp;

   END IF;
 end if;
END fta_sync_cb_pab;



/
ALTER TRIGGER "THE"."FTA_SYNC_CB_PAB" ENABLE;

  CREATE OR REPLACE EDITIONABLE TRIGGER "THE"."FTA_CPFILL_CB" 
  BEFORE INSERT OR UPDATE
  ON cut_block
  FOR EACH ROW
BEGIN
  if :new.cutting_permit_id is NULL then
  :new.cutting_permit_id := ' ';
  end if;
END fta_cpfill_cb;



/
ALTER TRIGGER "THE"."FTA_CPFILL_CB" ENABLE;

  CREATE OR REPLACE EDITIONABLE TRIGGER "THE"."TRG_ADS_AREA_LOGGED1" 
 AFTER UPDATE OR
	   DELETE OR INSERT
   ON ADS_AREA_LOGGED

FOR EACH ROW

DECLARE

  l_table_name      ECAS_AUDIT_DETAIL.TABLE_NAME%TYPE;
  l_column_name     ECAS_AUDIT_DETAIL.COLUMN_NAME%TYPE;
  l_old_value       ECAS_AUDIT_DETAIL.OLD_VALUE%TYPE;
  l_new_value       ECAS_AUDIT_DETAIL.NEW_VALUE%TYPE;
  l_ecas_id         ECAS_AUDIT_DETAIL.ECAS_ID%TYPE;
  l_audit_event_id  ECAS_AUDIT_DETAIL.AUDIT_EVENT_ID%TYPE;
  l_userid          ECAS_AUDIT_DETAIL.ENTRY_USERID%TYPE;
  l_business_id     ECAS_AUDIT_DETAIL.BUSINESS_IDENTIFIER%TYPE;

  l_error_message VARCHAR2(1500);

BEGIN

  l_table_name := 'ADS_AREA_LOGGED';
  l_audit_event_id := NULL;

  IF UPDATING OR INSERTING THEN
    l_ecas_id := :NEW.ecas_id;
    l_userid := :NEW.update_userid;
    l_business_id := 'Appraisal Year: '||:NEW.APPRAISAL_YEAR;

    Pkg_Ecas_Audit.CHECK_EVENT_ID(l_audit_event_id
                                , l_ecas_id
                                , 'UPD'
                                , l_userid
                                , SYSDATE
                                , l_error_message);

    IF l_audit_event_id IS NOT NULL THEN

      IF NVL(:NEW.APPRAISAL_YEAR,0) != NVL(:OLD.APPRAISAL_YEAR,0) THEN
        l_column_name := 'APPRAISAL_YEAR';
        l_old_value := :OLD.APPRAISAL_YEAR;
        l_new_value := :NEW.APPRAISAL_YEAR;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.AREA_LOGGED,0) != NVL(:OLD.AREA_LOGGED,0) THEN
        l_column_name := 'AREA_LOGGED';
        l_old_value := :OLD.AREA_LOGGED;
        l_new_value := :NEW.AREA_LOGGED;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.APPLICABLE_VOLUME, 0) != NVL(:OLD.APPLICABLE_VOLUME, 0) THEN
        l_column_name := 'APPLICABLE_VOLUME';
        l_old_value := :OLD.APPLICABLE_VOLUME;
        l_new_value := :NEW.APPLICABLE_VOLUME;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

    END IF; --l_audit_event_id is not null

  ELSIF DELETING THEN
    l_ecas_id := :OLD.ecas_id;
    l_business_id := 'Appraisal Year: '||:OLD.APPRAISAL_YEAR;
    Pkg_Ecas_Audit.GET_USERID(l_userid
                            , 'DEL'
                            , l_ecas_id
                            , SYSDATE
                            , l_error_message);

    Pkg_Ecas_Audit.CHECK_EVENT_ID(l_audit_event_id
                                , l_ecas_id
                                , 'DEL'
                                , l_userid
                                , SYSDATE
                                , l_error_message);

    IF l_audit_event_id IS NOT NULL THEN

      l_column_name := 'APPRAISAL_YEAR';
      l_old_value := :OLD.APPRAISAL_YEAR;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'AREA_LOGGED';
      l_old_value := :OLD.AREA_LOGGED;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'APPLICABLE_VOLUME';
      l_old_value := :OLD.APPLICABLE_VOLUME;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

    END IF; --l_audit_event_id is not null

  END IF; --if updating or deleting

  EXCEPTION
    WHEN TOO_MANY_ROWS THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.record.toomany:TRG_ADS_AREA_LOGGED1,'||TRIM(l_table_name)||','||SQLCODE||';';
    WHEN DUP_VAL_ON_INDEX THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.record.duplicate:TRG_ADS_AREA_LOGGED1,'||TRIM(l_table_name)||','||SQLCODE||';';
    WHEN NO_DATA_FOUND THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.record.invalid:TRG_ADS_AREA_LOGGED1,'||TRIM(l_table_name)||';';
    WHEN OTHERS THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.unexpected:TRG_ADS_AREA_LOGGED1,'||SQLCODE||','||SQLERRM||';';
    RAISE;
END TRG_ADS_AREA_LOGGED1;




/
ALTER TRIGGER "THE"."TRG_ADS_AREA_LOGGED1" ENABLE;

  CREATE OR REPLACE EDITIONABLE TRIGGER "THE"."TRG_ADS_BEC_LINK1" 
 AFTER UPDATE OR DELETE OR INSERT
   ON ADS_BEC_LINK

FOR EACH ROW

DECLARE

  l_table_name     ECAS_AUDIT_DETAIL.TABLE_NAME%TYPE;
  l_column_name    ECAS_AUDIT_DETAIL.COLUMN_NAME%TYPE;
  l_old_value      ECAS_AUDIT_DETAIL.OLD_VALUE%TYPE;
  l_new_value      ECAS_AUDIT_DETAIL.NEW_VALUE%TYPE;
  l_ecas_id        ECAS_AUDIT_DETAIL.ECAS_ID%TYPE;
  l_audit_event_id ECAS_AUDIT_DETAIL.AUDIT_EVENT_ID%TYPE;
  l_userid         ECAS_AUDIT_DETAIL.ENTRY_USERID%TYPE;
  l_business_id    ECAS_AUDIT_DETAIL.BUSINESS_IDENTIFIER%TYPE;

  l_error_message VARCHAR2(1500);

BEGIN

  l_table_name     := 'ADS_BEC_LINK';
  l_audit_event_id := NULL;
  l_business_id    := 'Ecas Id: ' || :NEW.ECAS_ID || ' BEC Zn: ' ||
                      :NEW.BGC_ZONE_CODE || ' SubZn: ' ||
                      :NEW.BGC_SUBZONE_CODE || ' Varnt: ' || :NEW.VARIANT;

  IF UPDATING OR INSERTING THEN

    l_userid  := :NEW.update_userid;
    l_ecas_id := :NEW.ecas_id;

    Pkg_Ecas_Audit.CHECK_EVENT_ID(l_audit_event_id,
                                  l_ecas_id,
                                  'UPD',
                                  l_userid,
                                  SYSDATE,
                                  l_error_message);

    IF l_audit_event_id IS NOT NULL THEN

      IF NVL(:NEW.BGC_ZONE_CODE, 0) != NVL(:OLD.BGC_ZONE_CODE, 0) THEN
        l_column_name := 'BGC_ZONE_CODE';
        l_old_value   := :OLD.BGC_ZONE_CODE;
        l_new_value   := :NEW.BGC_ZONE_CODE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id,
                                        l_audit_event_id,
                                        l_table_name,
                                        l_column_name,
                                        l_old_value,
                                        l_new_value,
                                        l_userid,
                                        l_business_id,
                                        l_error_message);
      END IF;

      IF NVL(:NEW.BGC_SUBZONE_CODE, 0) != NVL(:OLD.BGC_SUBZONE_CODE, 0) THEN
        l_column_name := 'BGC_SUBZONE_CODE';
        l_old_value   := :OLD.BGC_SUBZONE_CODE;
        l_new_value   := :NEW.BGC_SUBZONE_CODE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id,
                                        l_audit_event_id,
                                        l_table_name,
                                        l_column_name,
                                        l_old_value,
                                        l_new_value,
                                        l_userid,
                                        l_business_id,
                                        l_error_message);
      END IF;

      IF NVL(:NEW.VARIANT, 0) != NVL(:OLD.VARIANT, 0) THEN
        l_column_name := 'VARIANT';
        l_old_value   := :OLD.VARIANT;
        l_new_value   := :NEW.VARIANT;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id,
                                        l_audit_event_id,
                                        l_table_name,
                                        l_column_name,
                                        l_old_value,
                                        l_new_value,
                                        l_userid,
                                        l_business_id,
                                        l_error_message);
      END IF;

      IF NVL(:NEW.BEC_OCCURANCE_PCT, 0) != NVL(:OLD.BEC_OCCURANCE_PCT, 0) THEN
        l_column_name := 'BEC_OCCURANCE_PCT';
        l_old_value   := :OLD.BEC_OCCURANCE_PCT;
        l_new_value   := :NEW.BEC_OCCURANCE_PCT;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id,
                                        l_audit_event_id,
                                        l_table_name,
                                        l_column_name,
                                        l_old_value,
                                        l_new_value,
                                        l_userid,
                                        l_business_id,
                                        l_error_message);

      END IF;

      IF NVL(:NEW.BEC_ENHANCED_PCT, 0) != NVL(:OLD.BEC_ENHANCED_PCT, 0) THEN
        l_column_name := 'BEC_ENHANCED_PCT';
        l_old_value   := :OLD.BEC_ENHANCED_PCT;
        l_new_value   := :NEW.BEC_ENHANCED_PCT;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id,
                                        l_audit_event_id,
                                        l_table_name,
                                        l_column_name,
                                        l_old_value,
                                        l_new_value,
                                        l_userid,
                                        l_business_id,
                                        l_error_message);
      END IF;

    END IF; --l_audit_event_id is not null

  ELSIF DELETING THEN
    l_ecas_id     := :OLD.ecas_id;
    l_business_id := 'Ecas Id: ' || :OLD.ECAS_ID || ' BEC Zn: ' ||
                     :OLD.BGC_ZONE_CODE || ' SubZn: ' ||
                     :OLD.BGC_SUBZONE_CODE || ' Varnt: ' || :OLD.VARIANT;

    -- Deletes on this table are handled like Updates because the action updates Appraisal Data Submission at the same time
    Pkg_Ecas_Audit.GET_USERID(l_userid,
                              'UPD',
                              l_ecas_id,
                              SYSDATE,
                              l_error_message);

    Pkg_Ecas_Audit.CHECK_EVENT_ID(l_audit_event_id,
                                  l_ecas_id,
                                  'UPD',
                                  l_userid,
                                  SYSDATE,
                                  l_error_message);

    IF l_audit_event_id IS NOT NULL THEN

      l_column_name := 'BGC_ZONE_CODE';
      l_old_value   := :OLD.BGC_ZONE_CODE;
      l_new_value   := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id,
                                        l_audit_event_id,
                                        l_table_name,
                                        l_column_name,
                                        l_old_value,
                                        l_new_value,
                                        l_userid,
                                        l_business_id,
                                        l_error_message);
      END IF;

      l_column_name := 'BGC_SUBZONE_CODE';
      l_old_value   := :OLD.BGC_SUBZONE_CODE;
      l_new_value   := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id,
                                        l_audit_event_id,
                                        l_table_name,
                                        l_column_name,
                                        l_old_value,
                                        l_new_value,
                                        l_userid,
                                        l_business_id,
                                        l_error_message);
      END IF;

      l_column_name := 'VARIANT';
      l_old_value   := :OLD.VARIANT;
      l_new_value   := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id,
                                        l_audit_event_id,
                                        l_table_name,
                                        l_column_name,
                                        l_old_value,
                                        l_new_value,
                                        l_userid,
                                        l_business_id,
                                        l_error_message);
      END IF;

      l_column_name := 'BEC_OCCURANCE_PCT';
      l_old_value   := :OLD.BEC_OCCURANCE_PCT;
      l_new_value   := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id,
                                        l_audit_event_id,
                                        l_table_name,
                                        l_column_name,
                                        l_old_value,
                                        l_new_value,
                                        l_userid,
                                        l_business_id,
                                        l_error_message);
      END IF;

      l_column_name := 'BEC_ENHANCED_PCT';
      l_old_value   := :OLD.BEC_ENHANCED_PCT;
      l_new_value   := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id,
                                        l_audit_event_id,
                                        l_table_name,
                                        l_column_name,
                                        l_old_value,
                                        l_new_value,
                                        l_userid,
                                        l_business_id,
                                        l_error_message);
      END IF;

    END IF; --l_audit_event_id is not null
  END IF; --if updating or deleting

EXCEPTION
  WHEN TOO_MANY_ROWS THEN
    l_error_message := l_error_message ||
                       'ecas.web.usr.database.record.toomany:TRG_ADS_BEC_LINK1,' ||
                       TRIM(l_table_name) || ',' || SQLCODE || ';';
  WHEN DUP_VAL_ON_INDEX THEN
    l_error_message := l_error_message ||
                       'ecas.web.usr.database.record.duplicate:TRG_ADS_BEC_LINK1,' ||
                       TRIM(l_table_name) || ',' || SQLCODE || ';';
  WHEN NO_DATA_FOUND THEN
    l_error_message := l_error_message ||
                       'ecas.web.usr.database.record.invalid:TRG_ADS_BEC_LINK1,' ||
                       TRIM(l_table_name) || ';';
  WHEN OTHERS THEN
    l_error_message := l_error_message ||
                       'ecas.web.usr.database.unexpected:TRG_ADS_BEC_LINK1,' ||
                       SQLCODE || ',' || SQLERRM || ';';
    RAISE;
END TRG_ADS_BEC_LINK1;



/
ALTER TRIGGER "THE"."TRG_ADS_BEC_LINK1" ENABLE;

  CREATE OR REPLACE EDITIONABLE TRIGGER "THE"."TRG_ADS_BCTS_SPECIES_GRADE1" 
AFTER  DELETE  OR UPDATE OR INSERT
ON ADS_BCTS_SPECIES_GRADE
REFERENCING NEW AS NEW OLD AS OLD
FOR EACH ROW

DECLARE
  l_table_name      ECAS_AUDIT_DETAIL.TABLE_NAME%TYPE;
  l_column_name     ECAS_AUDIT_DETAIL.COLUMN_NAME%TYPE;
  l_old_value       ECAS_AUDIT_DETAIL.OLD_VALUE%TYPE;
  l_new_value       ECAS_AUDIT_DETAIL.NEW_VALUE%TYPE;
  l_ecas_id         ECAS_AUDIT_DETAIL.ECAS_ID%TYPE;
  l_audit_event_id  ECAS_AUDIT_DETAIL.AUDIT_EVENT_ID%TYPE;
  l_userid          ECAS_AUDIT_DETAIL.ENTRY_USERID%TYPE;
  l_business_id     ECAS_AUDIT_DETAIL.BUSINESS_IDENTIFIER%TYPE;
  l_error_message VARCHAR2(1500);

BEGIN
  l_table_name := 'ADS_BCTS_SPECIES_GRADE';
  l_audit_event_id := NULL;

  IF UPDATING OR INSERTING THEN
    l_userid := :NEW.entry_userid;
    l_ecas_id := :NEW.ecas_id;
	l_business_id := 'Species Group: '||:NEW.MANDATORY_UTIL_SPECIES_GP_CODE||' Scale Grade: '||:NEW.MANDATORY_UTIL_SCALE_GRD_CODE;

    Pkg_Ecas_Audit.CHECK_EVENT_ID(l_audit_event_id
                                , l_ecas_id
                                , 'UPD'
                                , l_userid
                                , SYSDATE
                                , l_error_message);

    IF l_audit_event_id IS NOT NULL THEN

      IF NVL(:NEW.MANDATORY_UTIL_SPECIES_GP_CODE,0) != NVL(:OLD.MANDATORY_UTIL_SPECIES_GP_CODE,0) THEN
        l_column_name := 'MANDATORY_UTIL_SPECIES_GP_CODE';
        l_old_value := :OLD.MANDATORY_UTIL_SPECIES_GP_CODE;
        l_new_value := :NEW.MANDATORY_UTIL_SPECIES_GP_CODE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      IF NVL(:NEW.MANDATORY_UTIL_SCALE_GRD_CODE,0) != NVL(:OLD.MANDATORY_UTIL_SCALE_GRD_CODE,0) THEN
        l_column_name := 'MANDATORY_UTIL_SCALE_GRD_CODE';
        l_old_value := :OLD.MANDATORY_UTIL_SCALE_GRD_CODE;
        l_new_value := :NEW.MANDATORY_UTIL_SCALE_GRD_CODE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

    END IF; --l_audit_event_id is not null

  ELSIF DELETING THEN
    l_ecas_id := :OLD.ecas_id;
	l_business_id := 'Species Group: '||:OLD.MANDATORY_UTIL_SPECIES_GP_CODE||' Scale Grade: '||:OLD.MANDATORY_UTIL_SCALE_GRD_CODE;
    Pkg_Ecas_Audit.GET_USERID(l_userid
                            , 'DEL'
                            , l_ecas_id
                            , SYSDATE
                            , l_error_message);

    Pkg_Ecas_Audit.CHECK_EVENT_ID(l_audit_event_id
                                , l_ecas_id
                                , 'DEL'
                                , l_userid
                                , SYSDATE
                                , l_error_message);

    IF l_audit_event_id IS NOT NULL THEN
      l_column_name := 'MANDATORY_UTIL_SPECIES_GP_CODE';
      l_old_value := :OLD.MANDATORY_UTIL_SPECIES_GP_CODE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'MANDATORY_UTIL_SCALE_GRD_CODE';
      l_old_value := :OLD.MANDATORY_UTIL_SCALE_GRD_CODE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

    END IF; --l_audit_event_id is not null
  END IF; --if updating or deleting

  EXCEPTION
    WHEN TOO_MANY_ROWS THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.record.toomany:TRG_ADS_BCTS_SPECIES_GRADE1,'||TRIM(l_table_name)||','||SQLCODE||';';
    WHEN DUP_VAL_ON_INDEX THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.record.duplicate:TRG_ADS_BCTS_SPECIES_GRADE1,'||TRIM(l_table_name)||','||SQLCODE||';';
    WHEN NO_DATA_FOUND THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.record.invalid:TRG_ADS_BCTS_SPECIES_GRADE1,'||TRIM(l_table_name)||';';
    WHEN OTHERS THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.unexpected:TRG_ADS_BCTS_SPECIES_GRADE1,'||SQLCODE||','||SQLERRM||';';
    RAISE;

END TRG_ADS_BCTS_SPECIES_GRADE1;



/
ALTER TRIGGER "THE"."TRG_ADS_BCTS_SPECIES_GRADE1" ENABLE;

  CREATE OR REPLACE EDITIONABLE TRIGGER "THE"."TRG_ADS_CULVERT1" 
 AFTER UPDATE OR
	   DELETE OR INSERT
   ON ADS_CULVERT

FOR EACH ROW

DECLARE

  l_table_name      ECAS_AUDIT_DETAIL.TABLE_NAME%TYPE;
  l_column_name     ECAS_AUDIT_DETAIL.COLUMN_NAME%TYPE;
  l_old_value       ECAS_AUDIT_DETAIL.OLD_VALUE%TYPE;
  l_new_value       ECAS_AUDIT_DETAIL.NEW_VALUE%TYPE;
  l_ecas_id         ECAS_AUDIT_DETAIL.ECAS_ID%TYPE;
  l_audit_event_id  ECAS_AUDIT_DETAIL.AUDIT_EVENT_ID%TYPE;
  l_userid          ECAS_AUDIT_DETAIL.ENTRY_USERID%TYPE;
  l_business_id     ECAS_AUDIT_DETAIL.BUSINESS_IDENTIFIER%TYPE;

  l_error_message VARCHAR2(1500);

BEGIN

  l_table_name := 'ADS_CULVERT';
  l_audit_event_id := NULL;

  IF UPDATING OR INSERTING THEN
    l_userid := :NEW.update_userid;
    l_ecas_id := :NEW.ecas_id;
	l_business_id := 'Culvert Type: '||:NEW.APPRAISAL_CULVERT_TYPE_CODE||' Diameter: '||:NEW.CULVERT_DIAMETER||' Length: '||:NEW.CULVERT_LENGTH;

    Pkg_Ecas_Audit.CHECK_EVENT_ID(l_audit_event_id
                                , l_ecas_id
                                , 'UPD'
                                , l_userid
                                , SYSDATE
                                , l_error_message);

    IF l_audit_event_id IS NOT NULL THEN

      IF NVL(:NEW.APPRAISAL_YEAR,0) != NVL(:OLD.APPRAISAL_YEAR,0) THEN
        l_column_name := 'APPRAISAL_YEAR';
        l_old_value := :OLD.APPRAISAL_YEAR;
        l_new_value := :NEW.APPRAISAL_YEAR;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.CULVERT_ID,0) != NVL(:OLD.CULVERT_ID,0) THEN
        l_column_name := 'CULVERT_ID';
        l_old_value := :OLD.CULVERT_ID;
        l_new_value := :NEW.CULVERT_ID;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.APPRAISAL_CULVERT_TYPE_CODE,0) != NVL(:OLD.APPRAISAL_CULVERT_TYPE_CODE,0) THEN
        l_column_name := 'APPRAISAL_CULVERT_TYPE_CODE';
        l_old_value := :OLD.APPRAISAL_CULVERT_TYPE_CODE;
        l_new_value := :NEW.APPRAISAL_CULVERT_TYPE_CODE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.CULVERT_LENGTH, 0) != NVL(:OLD.CULVERT_LENGTH, 0) THEN
        l_column_name := 'CULVERT_LENGTH';
        l_old_value := :OLD.CULVERT_LENGTH;
        l_new_value := :NEW.CULVERT_LENGTH;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.CULVERT_DIAMETER, 0) != NVL(:OLD.CULVERT_DIAMETER, 0) THEN
        l_column_name := 'CULVERT_DIAMETER';
        l_old_value := :OLD.CULVERT_DIAMETER;
        l_new_value := :NEW.CULVERT_DIAMETER;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.QUANTITY_USED, 0) != NVL(:OLD.QUANTITY_USED, 0) THEN
        l_column_name := 'QUANTITY_USED';
        l_old_value := :OLD.QUANTITY_USED;
        l_new_value := :NEW.QUANTITY_USED;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.TOTAL_CULVERT_LENGTH, 0) != NVL(:OLD.TOTAL_CULVERT_LENGTH, 0) THEN
        l_column_name := 'TOTAL_CULVERT_LENGTH';
        l_old_value := :OLD.TOTAL_CULVERT_LENGTH;
        l_new_value := :NEW.TOTAL_CULVERT_LENGTH;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.UNIT_COST_PER_METER, 0) != NVL(:OLD.UNIT_COST_PER_METER, 0) THEN
        l_column_name := 'UNIT_COST_PER_METER';
        l_old_value := :OLD.UNIT_COST_PER_METER;
        l_new_value := :NEW.UNIT_COST_PER_METER;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.AMORTIZED_PCT, 0) != NVL(:OLD.AMORTIZED_PCT, 0) THEN
        l_column_name := 'AMORTIZED_PCT';
        l_old_value := :OLD.AMORTIZED_PCT;
        l_new_value := :NEW.AMORTIZED_PCT;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.CROWN_PCT, 0) != NVL(:OLD.CROWN_PCT, 0) THEN
        l_column_name := 'CROWN_PCT';
        l_old_value := :OLD.CROWN_PCT;
        l_new_value := :NEW.CROWN_PCT;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);

      END IF;

    END IF; --l_audit_event_id is not null

  ELSIF DELETING THEN
    l_ecas_id := :OLD.ecas_id;
	l_business_id := 'Culvert Type: '||:OLD.APPRAISAL_CULVERT_TYPE_CODE||' Diameter: '||:OLD.CULVERT_DIAMETER||' Length: '||:OLD.CULVERT_LENGTH;
    Pkg_Ecas_Audit.GET_USERID(l_userid
                            , 'DEL'
                            , l_ecas_id
                            , SYSDATE
                            , l_error_message);

    Pkg_Ecas_Audit.CHECK_EVENT_ID(l_audit_event_id
                                , l_ecas_id
                                , 'DEL'
                                , l_userid
                                , SYSDATE
                                , l_error_message);

    IF l_audit_event_id IS NOT NULL THEN

      l_column_name := 'APPRAISAL_YEAR';
      l_old_value := :OLD.APPRAISAL_YEAR;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'CULVERT_ID';
      l_old_value := :OLD.CULVERT_ID;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'APPRAISAL_CULVERT_TYPE_CODE';
      l_old_value := :OLD.APPRAISAL_CULVERT_TYPE_CODE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'CULVERT_LENGTH';
      l_old_value := :OLD.CULVERT_LENGTH;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'CULVERT_DIAMETER';
      l_old_value := :OLD.CULVERT_DIAMETER;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'QUANTITY_USED';
      l_old_value := :OLD.QUANTITY_USED;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'TOTAL_CULVERT_LENGTH';
      l_old_value := :OLD.TOTAL_CULVERT_LENGTH;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'UNIT_COST_PER_METER';
      l_old_value := :OLD.UNIT_COST_PER_METER;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'AMORTIZED_PCT';
      l_old_value := :OLD.AMORTIZED_PCT;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'CROWN_PCT';
      l_old_value := :OLD.CROWN_PCT;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
									  , l_business_id
                                      , l_error_message);
      END IF;

    END IF; --l_audit_event_id is not null

  END IF; --if updating or deleting

  EXCEPTION
    WHEN TOO_MANY_ROWS THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.record.toomany:TRG_ADS_CULVERT1,'||TRIM(l_table_name)||','||SQLCODE||';';
    WHEN DUP_VAL_ON_INDEX THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.record.duplicate:TRG_ADS_CULVERT1,'||TRIM(l_table_name)||','||SQLCODE||';';
    WHEN NO_DATA_FOUND THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.record.invalid:TRG_ADS_CULVERT1,'||TRIM(l_table_name)||';';
    WHEN OTHERS THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.unexpected:TRG_ADS_CULVERT1,'||SQLCODE||','||SQLERRM||';';
    RAISE;
END TRG_ADS_CULVERT1;




/
ALTER TRIGGER "THE"."TRG_ADS_CULVERT1" ENABLE;

  CREATE OR REPLACE EDITIONABLE TRIGGER "THE"."TRG_APPRAISAL_DATA_SUBMISSION1" 
 AFTER UPDATE OR DELETE
   ON APPRAISAL_DATA_SUBMISSION

FOR EACH ROW

DECLARE

  l_table_name      ECAS_AUDIT_DETAIL.TABLE_NAME%TYPE;
  l_column_name     ECAS_AUDIT_DETAIL.COLUMN_NAME%TYPE;
  l_old_value       ECAS_AUDIT_DETAIL.OLD_VALUE%TYPE;
  l_new_value       ECAS_AUDIT_DETAIL.NEW_VALUE%TYPE;
  l_ecas_id         ECAS_AUDIT_DETAIL.ECAS_ID%TYPE;
  l_audit_event_id  ECAS_AUDIT_DETAIL.AUDIT_EVENT_ID%TYPE;
  l_userid          ECAS_AUDIT_DETAIL.ENTRY_USERID%TYPE;
  l_business_id     ECAS_AUDIT_DETAIL.BUSINESS_IDENTIFIER%TYPE;

  l_error_message VARCHAR2(1500);

BEGIN

  l_table_name := 'APPRAISAL_DATA_SUBMISSION';
  l_audit_event_id := NULL;
  l_business_id := 'Ecas Id: '||:NEW.ECAS_ID;


  IF UPDATING THEN


    l_ecas_id := :OLD.ecas_id;
    l_userid := :NEW.update_userid;

    Pkg_Ecas_Audit.CHECK_EVENT_ID(l_audit_event_id
                                , l_ecas_id
                                , 'UPD'
                                , l_userid
                                , SYSDATE
                                , l_error_message);

    IF l_audit_event_id IS NOT NULL THEN

      IF NVL(:NEW.FOREST_FILE_ID, 0) != NVL(:OLD.FOREST_FILE_ID, 0) THEN
        l_column_name := 'FOREST_FILE_ID';
        l_old_value := :OLD.FOREST_FILE_ID;
        l_new_value := :NEW.FOREST_FILE_ID;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.CUTTING_PERMIT_ID, 0) != NVL(:OLD.CUTTING_PERMIT_ID, 0) THEN
        l_column_name := 'CUTTING_PERMIT_ID';
        l_old_value := :OLD.CUTTING_PERMIT_ID;
        l_new_value := :NEW.CUTTING_PERMIT_ID;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.APPRAISAL_STATUS_CODE,0) != NVL(:OLD.APPRAISAL_STATUS_CODE,0) THEN
        l_column_name := 'APPRAISAL_STATUS_CODE';
        l_old_value := :OLD.APPRAISAL_STATUS_CODE;
        l_new_value := :NEW.APPRAISAL_STATUS_CODE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.APPRAISAL_CATEGORY_CODE,0) != NVL(:OLD.APPRAISAL_CATEGORY_CODE,0) THEN
        l_column_name := 'APPRAISAL_CATEGORY_CODE';
        l_old_value := :OLD.APPRAISAL_CATEGORY_CODE;
        l_new_value := :NEW.APPRAISAL_CATEGORY_CODE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(Pkg_Sil_Date_Conversion.CONVERT_TO_CHAR(:NEW.APPRAISAL_EFFECTIVE_DATE, 'YYYY-MM-DD', 'Y'), 0)
           != NVL(Pkg_Sil_Date_Conversion.CONVERT_TO_CHAR(:OLD.APPRAISAL_EFFECTIVE_DATE, 'YYYY-MM-DD', 'Y'), 0) THEN
        l_column_name := 'APPRAISAL_EFFECTIVE_DATE';
        l_old_value := :OLD.APPRAISAL_EFFECTIVE_DATE;
        l_new_value := :NEW.APPRAISAL_EFFECTIVE_DATE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(Pkg_Sil_Date_Conversion.CONVERT_TO_CHAR(:NEW.APPRAISAL_EXPIRY_DATE, 'YYYY-MM-DD', 'Y'), 0)
           != NVL(Pkg_Sil_Date_Conversion.CONVERT_TO_CHAR(:OLD.APPRAISAL_EXPIRY_DATE, 'YYYY-MM-DD', 'Y'), 0) THEN
        l_column_name := 'APPRAISAL_EXPIRY_DATE';
        l_old_value := :OLD.APPRAISAL_EXPIRY_DATE;
        l_new_value := :NEW.APPRAISAL_EXPIRY_DATE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.RATE_CALC_METHOD_CODE,0) != NVL(:OLD.RATE_CALC_METHOD_CODE,0) THEN
        l_column_name := 'RATE_CALC_METHOD_CODE';
        l_old_value := :OLD.RATE_CALC_METHOD_CODE;
        l_new_value := :NEW.RATE_CALC_METHOD_CODE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.ADMIN_DISTRICT,0) != NVL(:OLD.ADMIN_DISTRICT,0) THEN
        l_column_name := 'ADMIN_DISTRICT';
        l_old_value := :OLD.ADMIN_DISTRICT;
        l_new_value := :NEW.ADMIN_DISTRICT;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.TSB_NUMBER_CODE, 0) != NVL(:OLD.TSB_NUMBER_CODE, 0) THEN
        l_column_name := 'TSB_NUMBER_CODE';
        l_old_value := :OLD.TSB_NUMBER_CODE;
        l_new_value := :NEW.TSB_NUMBER_CODE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.POINT_OF_APPRAISAL_DISTANCE, 0) != NVL(:OLD.POINT_OF_APPRAISAL_DISTANCE, 0) THEN
        l_column_name := 'POINT_OF_APPRAISAL_DISTANCE';
        l_old_value := :OLD.POINT_OF_APPRAISAL_DISTANCE;
        l_new_value := :NEW.POINT_OF_APPRAISAL_DISTANCE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.HELI_PARTIAL_CUT_VOLUME, 0) != NVL(:OLD.HELI_PARTIAL_CUT_VOLUME, 0) THEN
        l_column_name := 'HELI_PARTIAL_CUT_VOLUME';
        l_old_value := :OLD.HELI_PARTIAL_CUT_VOLUME;
        l_new_value := :NEW.HELI_PARTIAL_CUT_VOLUME;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;


      IF NVL(:NEW.NET_CRUISE_VOLUME, 0) != NVL(:OLD.NET_CRUISE_VOLUME, 0) THEN
        l_column_name := 'NET_CRUISE_VOLUME';
        l_old_value := :OLD.NET_CRUISE_VOLUME;
        l_new_value := :NEW.NET_CRUISE_VOLUME;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.SECOND_GROWTH_CONIFER_VOLUME, 0) != NVL(:OLD.SECOND_GROWTH_CONIFER_VOLUME, 0) THEN
        l_column_name := 'SECOND_GROWTH_CONIFER_VOLUME';
        l_old_value := :OLD.SECOND_GROWTH_CONIFER_VOLUME;
        l_new_value := :NEW.SECOND_GROWTH_CONIFER_VOLUME;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.SECOND_GROWTH_IND, 0) != NVL(:OLD.SECOND_GROWTH_IND, 0) THEN
        l_column_name := 'SECOND_GROWTH_IND';
        l_old_value := :OLD.SECOND_GROWTH_IND;
        l_new_value := :NEW.SECOND_GROWTH_IND;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.SINGLE_TREE_CRUISE_IND, 0) != NVL(:OLD.SINGLE_TREE_CRUISE_IND, 0) THEN
        l_column_name := 'SINGLE_TREE_CRUISE_IND';
        l_old_value := :OLD.SINGLE_TREE_CRUISE_IND;
        l_new_value := :NEW.SINGLE_TREE_CRUISE_IND;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.INITIAL_MERCHANTABLE_AREA, 0) != NVL(:OLD.INITIAL_MERCHANTABLE_AREA, 0) THEN
        l_column_name := 'INITIAL_MERCHANTABLE_AREA';
        l_old_value := :OLD.INITIAL_MERCHANTABLE_AREA;
        l_new_value := :NEW.INITIAL_MERCHANTABLE_AREA;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.SINGLE_TREE_SELECTION_VOLUME, 0) != NVL(:OLD.SINGLE_TREE_SELECTION_VOLUME, 0) THEN
        l_column_name := 'SINGLE_TREE_SELECTION_VOLUME';
        l_old_value := :OLD.SINGLE_TREE_SELECTION_VOLUME;
        l_new_value := :NEW.SINGLE_TREE_SELECTION_VOLUME;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.SALVAGE_IND, 0) != NVL(:OLD.SALVAGE_IND, 0) THEN
        l_column_name := 'SALVAGE_IND';
        l_old_value := :OLD.SALVAGE_IND;
        l_new_value := :NEW.SALVAGE_IND;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.COMPARATIVE_CRUISE_IND, 0) != NVL(:OLD.COMPARATIVE_CRUISE_IND, 0) THEN
        l_column_name := 'COMPARATIVE_CRUISE_IND';
        l_old_value := :OLD.COMPARATIVE_CRUISE_IND;
        l_new_value := :NEW.COMPARATIVE_CRUISE_IND;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.STANDARD_ERROR_PCT, 0) != NVL(:OLD.STANDARD_ERROR_PCT, 0) THEN
        l_column_name := 'STANDARD_ERROR_PCT';
        l_old_value := :OLD.STANDARD_ERROR_PCT;
        l_new_value := :NEW.STANDARD_ERROR_PCT;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.SPECIFIED_OPERATION_IND, 0) != NVL(:OLD.SPECIFIED_OPERATION_IND, 0) THEN
        l_column_name := 'SPECIFIED_OPERATION_IND';
        l_old_value := :OLD.SPECIFIED_OPERATION_IND;
        l_new_value := :NEW.SPECIFIED_OPERATION_IND;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.NET_MERCHANTABLE_AREA, 0) != NVL(:OLD.NET_MERCHANTABLE_AREA, 0) THEN
        l_column_name := 'NET_MERCHANTABLE_AREA';
        l_old_value := :OLD.NET_MERCHANTABLE_AREA;
        l_new_value := :NEW.NET_MERCHANTABLE_AREA;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.REFERENCE_MARK, 0) != NVL(:OLD.REFERENCE_MARK, 0) THEN
        l_column_name := 'REFERENCE_MARK';
        l_old_value := :OLD.REFERENCE_MARK;
        l_new_value := :NEW.REFERENCE_MARK;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.ROAD_USE_CHARGE, 0) != NVL(:OLD.ROAD_USE_CHARGE, 0) THEN
        l_column_name := 'ROAD_USE_CHARGE';
        l_old_value := :OLD.ROAD_USE_CHARGE;
        l_new_value := :NEW.ROAD_USE_CHARGE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.ROUTINE_ROAD_MAINTENANCE_IND, 0) != NVL(:OLD.ROUTINE_ROAD_MAINTENANCE_IND, 0) THEN
        l_column_name := 'ROUTINE_ROAD_MAINTENANCE_IND';
        l_old_value := :OLD.ROUTINE_ROAD_MAINTENANCE_IND;
        l_new_value := :NEW.ROUTINE_ROAD_MAINTENANCE_IND;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.BASIC_SILV_REQUIRED_IND, 0) != NVL(:OLD.BASIC_SILV_REQUIRED_IND, 0) THEN
        l_column_name := 'BASIC_SILV_REQUIRED_IND';
        l_old_value := :OLD.BASIC_SILV_REQUIRED_IND;
        l_new_value := :NEW.BASIC_SILV_REQUIRED_IND;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.ISOLATION_TYPE_CODE, 0) != NVL(:OLD.ISOLATION_TYPE_CODE, 0) THEN
        l_column_name := 'ISOLATION_TYPE_CODE';
        l_old_value := :OLD.ISOLATION_TYPE_CODE;
        l_new_value := :NEW.ISOLATION_TYPE_CODE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.AVERAGE_SIDE_SLOPE_PCT, 0) != NVL(:OLD.AVERAGE_SIDE_SLOPE_PCT, 0) THEN
        l_column_name := 'AVERAGE_SIDE_SLOPE_PCT';
        l_old_value := :OLD.AVERAGE_SIDE_SLOPE_PCT;
        l_new_value := :NEW.AVERAGE_SIDE_SLOPE_PCT;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.CABLE_YARDING_VOLUME, 0) != NVL(:OLD.CABLE_YARDING_VOLUME, 0) THEN
        l_column_name := 'CABLE_YARDING_VOLUME';
        l_old_value := :OLD.CABLE_YARDING_VOLUME;
        l_new_value := :NEW.CABLE_YARDING_VOLUME;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.GROUND_SYSTEMS_VOLUME, 0) != NVL(:OLD.GROUND_SYSTEMS_VOLUME, 0) THEN
        l_column_name := 'GROUND_SYSTEMS_VOLUME';
        l_old_value := :OLD.GROUND_SYSTEMS_VOLUME;
        l_new_value := :NEW.GROUND_SYSTEMS_VOLUME;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.HELI_LAND_DROP_VOLUME, 0) != NVL(:OLD.HELI_LAND_DROP_VOLUME, 0) THEN
        l_column_name := 'HELI_LAND_DROP_VOLUME';
        l_old_value := :OLD.HELI_LAND_DROP_VOLUME;
        l_new_value := :NEW.HELI_LAND_DROP_VOLUME;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.HELI_WATER_DROP_VOLUME, 0) != NVL(:OLD.HELI_WATER_DROP_VOLUME, 0) THEN
        l_column_name := 'HELI_WATER_DROP_VOLUME';
        l_old_value := :OLD.HELI_WATER_DROP_VOLUME;
        l_new_value := :NEW.HELI_WATER_DROP_VOLUME;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.SINGLE_STEM_VOLUME, 0) != NVL(:OLD.SINGLE_STEM_VOLUME, 0) THEN
        l_column_name := 'SINGLE_STEM_VOLUME';
        l_old_value := :OLD.SINGLE_STEM_VOLUME;
        l_new_value := :NEW.SINGLE_STEM_VOLUME;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.CREW_TRANSPORT_DISTANCE, 0) != NVL(:OLD.CREW_TRANSPORT_DISTANCE, 0) THEN
        l_column_name := 'CREW_TRANSPORT_DISTANCE';
        l_old_value := :OLD.CREW_TRANSPORT_DISTANCE;
        l_new_value := :NEW.CREW_TRANSPORT_DISTANCE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.RAIL_LOCATION_CODE, 0) != NVL(:OLD.RAIL_LOCATION_CODE, 0) THEN
        l_column_name := 'RAIL_LOCATION_CODE';
        l_old_value := :OLD.RAIL_LOCATION_CODE;
        l_new_value := :NEW.RAIL_LOCATION_CODE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);
      END IF;

      IF NVL(:NEW.LOCATION_DESCRIPTION, 0) != NVL(:OLD.LOCATION_DESCRIPTION, 0) THEN
        l_column_name := 'LOCATION_DESCRIPTION';
        l_old_value := :OLD.LOCATION_DESCRIPTION;
        l_new_value := :NEW.LOCATION_DESCRIPTION;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.CULVERT_APPLICABLE_VOLUME, 0) != NVL(:OLD.CULVERT_APPLICABLE_VOLUME, 0) THEN
        l_column_name := 'CULVERT_APPLICABLE_VOLUME';
        l_old_value := :OLD.CULVERT_APPLICABLE_VOLUME;
        l_new_value := :NEW.CULVERT_APPLICABLE_VOLUME;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.HAUL_DISTANCE, 0) != NVL(:OLD.HAUL_DISTANCE, 0) THEN
        l_column_name := 'HAUL_DISTANCE';
        l_old_value := :OLD.HAUL_DISTANCE;
        l_new_value := :NEW.HAUL_DISTANCE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.WOODLOT_PERIOD_CODE, 0) != NVL(:OLD.WOODLOT_PERIOD_CODE, 0) THEN
        l_column_name := 'WOODLOT_PERIOD_CODE';
        l_old_value := :OLD.WOODLOT_PERIOD_CODE;
        l_new_value := :NEW.WOODLOT_PERIOD_CODE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.TAB_ROAD_APPLICABLE_VOLUME, 0) != NVL(:OLD.TAB_ROAD_APPLICABLE_VOLUME, 0) THEN
        l_column_name := 'TAB_ROAD_APPLICABLE_VOLUME';
        l_old_value := :OLD.TAB_ROAD_APPLICABLE_VOLUME;
        l_new_value := :NEW.TAB_ROAD_APPLICABLE_VOLUME;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.TRUCK_HAUL_METHOD_CODE, 0) != NVL(:OLD.TRUCK_HAUL_METHOD_CODE, 0) THEN
        l_column_name := 'TRUCK_HAUL_METHOD_CODE';
        l_old_value := :OLD.TRUCK_HAUL_METHOD_CODE;
        l_new_value := :NEW.TRUCK_HAUL_METHOD_CODE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.LAKE_TRANSPORT_CODE, 0) != NVL(:OLD.LAKE_TRANSPORT_CODE, 0) THEN
        l_column_name := 'LAKE_TRANSPORT_CODE';
        l_old_value := :OLD.LAKE_TRANSPORT_CODE;
        l_new_value := :NEW.LAKE_TRANSPORT_CODE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.APP_SUPPORT_CENTRE_CODE, 0) != NVL(:OLD.APP_SUPPORT_CENTRE_CODE, 0) THEN
        l_column_name := 'APP_SUPPORT_CENTRE_CODE';
        l_old_value := :OLD.APP_SUPPORT_CENTRE_CODE;
        l_new_value := :NEW.APP_SUPPORT_CENTRE_CODE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.POINT_OF_APPRAISAL_CODE, 0) != NVL(:OLD.POINT_OF_APPRAISAL_CODE, 0) THEN
        l_column_name := 'POINT_OF_APPRAISAL_CODE';
        l_old_value := :OLD.POINT_OF_APPRAISAL_CODE;
        l_new_value := :NEW.POINT_OF_APPRAISAL_CODE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.WEIGHTED_POINT_OF_ORIGIN_COST, 0) != NVL(:OLD.WEIGHTED_POINT_OF_ORIGIN_COST, 0) THEN
        l_column_name := 'WEIGHTED_POINT_OF_ORIGIN_COST';
        l_old_value := :OLD.WEIGHTED_POINT_OF_ORIGIN_COST;
        l_new_value := :NEW.WEIGHTED_POINT_OF_ORIGIN_COST;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.NRST_POINT_OF_ORIGIN_COST, 0) != NVL(:OLD.NRST_POINT_OF_ORIGIN_COST, 0) THEN
        l_column_name := 'NRST_POINT_OF_ORIGIN_COST';
        l_old_value := :OLD.NRST_POINT_OF_ORIGIN_COST;
        l_new_value := :NEW.NRST_POINT_OF_ORIGIN_COST;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.WTR_PT_OF_ORIGIN1, 0) != NVL(:OLD.WTR_PT_OF_ORIGIN1, 0) THEN
        l_column_name := 'WTR_PT_OF_ORIGIN1';
        l_old_value := :OLD.WTR_PT_OF_ORIGIN1;
        l_new_value := :NEW.WTR_PT_OF_ORIGIN1;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.POINT_OF_ORIGIN_DIFFERENCE, 0) != NVL(:OLD.POINT_OF_ORIGIN_DIFFERENCE, 0) THEN
        l_column_name := 'POINT_OF_ORIGIN_DIFFERENCE';
        l_old_value := :OLD.POINT_OF_ORIGIN_DIFFERENCE;
        l_new_value := :NEW.POINT_OF_ORIGIN_DIFFERENCE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(Pkg_Sil_Date_Conversion.CONVERT_TO_CHAR(:NEW.WTR_PT_OF_ORIGIN1_DATE, 'YYYY-MM-DD', 'Y'), 0)
           != NVL(Pkg_Sil_Date_Conversion.CONVERT_TO_CHAR(:OLD.WTR_PT_OF_ORIGIN1_DATE, 'YYYY-MM-DD', 'Y'), 0) THEN
        l_column_name := 'WTR_PT_OF_ORIGIN1_DATE';
        l_old_value := :OLD.WTR_PT_OF_ORIGIN1_DATE;
        l_new_value := :NEW.WTR_PT_OF_ORIGIN1_DATE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.WEIGHTED_HAUL_DISTANCE, 0) != NVL(:OLD.WEIGHTED_HAUL_DISTANCE, 0) THEN
        l_column_name := 'WEIGHTED_HAUL_DISTANCE';
        l_old_value := :OLD.WEIGHTED_HAUL_DISTANCE;
        l_new_value := :NEW.WEIGHTED_HAUL_DISTANCE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.WTR_PT_OF_ORIGIN2, 0) != NVL(:OLD.WTR_PT_OF_ORIGIN2, 0) THEN
        l_column_name := 'WTR_PT_OF_ORIGIN2';
        l_old_value := :OLD.WTR_PT_OF_ORIGIN2;
        l_new_value := :NEW.WTR_PT_OF_ORIGIN2;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.ACCESSIBLE_ISOLATED_ADDITIVE, 0) != NVL(:OLD.ACCESSIBLE_ISOLATED_ADDITIVE, 0) THEN
        l_column_name := 'ACCESSIBLE_ISOLATED_ADDITIVE';
        l_old_value := :OLD.ACCESSIBLE_ISOLATED_ADDITIVE;
        l_new_value := :NEW.ACCESSIBLE_ISOLATED_ADDITIVE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(Pkg_Sil_Date_Conversion.CONVERT_TO_CHAR(:NEW.WTR_PT_OF_ORIGIN2_DATE, 'YYYY-MM-DD', 'Y'), 0)
           != NVL(Pkg_Sil_Date_Conversion.CONVERT_TO_CHAR(:OLD.WTR_PT_OF_ORIGIN2_DATE, 'YYYY-MM-DD', 'Y'), 0) THEN
        l_column_name := 'WTR_PT_OF_ORIGIN2_DATE';
        l_old_value := :OLD.WTR_PT_OF_ORIGIN2_DATE;
        l_new_value := :NEW.WTR_PT_OF_ORIGIN2_DATE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.ROAD_MAINTENANCE_ADDITIVE, 0) != NVL(:OLD.ROAD_MAINTENANCE_ADDITIVE, 0) THEN
        l_column_name := 'ROAD_MAINTENANCE_ADDITIVE';
        l_old_value := :OLD.ROAD_MAINTENANCE_ADDITIVE;
        l_new_value := :NEW.ROAD_MAINTENANCE_ADDITIVE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.LOG_TRANSPORTATION_ADDITIVE, 0) != NVL(:OLD.LOG_TRANSPORTATION_ADDITIVE, 0) THEN
        l_column_name := 'LOG_TRANSPORTATION_ADDITIVE';
        l_old_value := :OLD.LOG_TRANSPORTATION_ADDITIVE;
        l_new_value := :NEW.LOG_TRANSPORTATION_ADDITIVE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.TOWING_BARGING_ADDITIVE, 0) != NVL(:OLD.TOWING_BARGING_ADDITIVE, 0) THEN
        l_column_name := 'TOWING_BARGING_ADDITIVE';
        l_old_value := :OLD.TOWING_BARGING_ADDITIVE;
        l_new_value := :NEW.TOWING_BARGING_ADDITIVE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.WEIGHTED_CREW_TRANS_DISTANCE, 0) != NVL(:OLD.WEIGHTED_CREW_TRANS_DISTANCE, 0) THEN
        l_column_name := 'WEIGHTED_CREW_TRANS_DISTANCE';
        l_old_value := :OLD.WEIGHTED_CREW_TRANS_DISTANCE;
        l_new_value := :NEW.WEIGHTED_CREW_TRANS_DISTANCE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.NRST_PT_OF_ORIGIN, 0) != NVL(:OLD.NRST_PT_OF_ORIGIN, 0) THEN
        l_column_name := 'NRST_PT_OF_ORIGIN';
        l_old_value := :OLD.NRST_PT_OF_ORIGIN;
        l_new_value := :NEW.NRST_PT_OF_ORIGIN;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.APPRAISAL_SELL_PRICE_ZONE_CODE, 0) != NVL(:OLD.APPRAISAL_SELL_PRICE_ZONE_CODE, 0) THEN
        l_column_name := 'APPRAISAL_SELL_PRICE_ZONE_CODE';
        l_old_value := :OLD.APPRAISAL_SELL_PRICE_ZONE_CODE;
        l_new_value := :NEW.APPRAISAL_SELL_PRICE_ZONE_CODE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.BCTS_SALE_TYPE_CODE, 0) != NVL(:OLD.BCTS_SALE_TYPE_CODE, 0) THEN
        l_column_name := 'BCTS_SALE_TYPE_CODE';
        l_old_value := :OLD.BCTS_SALE_TYPE_CODE;
        l_new_value := :NEW.BCTS_SALE_TYPE_CODE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(Pkg_Sil_Date_Conversion.CONVERT_TO_CHAR(:NEW.BCTS_SALE_DATE, 'YYYY-MM-DD', 'Y'), 0)
           != NVL(Pkg_Sil_Date_Conversion.CONVERT_TO_CHAR(:OLD.BCTS_SALE_DATE, 'YYYY-MM-DD', 'Y'), 0) THEN
        l_column_name := 'BCTS_SALE_DATE';
        l_old_value := :OLD.BCTS_SALE_DATE;
        l_new_value := :NEW.BCTS_SALE_DATE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.SALE_TERM_YEAR, 0) != NVL(:OLD.SALE_TERM_YEAR, 0) THEN
        l_column_name := 'SALE_TERM_YEAR';
        l_old_value := :OLD.SALE_TERM_YEAR;
        l_new_value := :NEW.SALE_TERM_YEAR;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.SALE_TERM_MONTH, 0) != NVL(:OLD.SALE_TERM_MONTH, 0) THEN
        l_column_name := 'SALE_TERM_MONTH';
        l_old_value := :OLD.SALE_TERM_MONTH;
        l_new_value := :NEW.SALE_TERM_MONTH;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.DEVELOPMENT_LEVY_IND, 0) != NVL(:OLD.DEVELOPMENT_LEVY_IND, 0) THEN
        l_column_name := 'DEVELOPMENT_LEVY_IND';
        l_old_value := :OLD.DEVELOPMENT_LEVY_IND;
        l_new_value := :NEW.DEVELOPMENT_LEVY_IND;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.SILVICULTURE_LEVY_IND, 0) != NVL(:OLD.SILVICULTURE_LEVY_IND, 0) THEN
        l_column_name := 'SILVICULTURE_LEVY_IND';
        l_old_value := :OLD.SILVICULTURE_LEVY_IND;
        l_new_value := :NEW.SILVICULTURE_LEVY_IND;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.COAST_BONUS_BID_AMOUNT, 0) != NVL(:OLD.COAST_BONUS_BID_AMOUNT, 0) THEN
        l_column_name := 'COAST_BONUS_BID_AMOUNT';
        l_old_value := :OLD.COAST_BONUS_BID_AMOUNT;
        l_new_value := :NEW.COAST_BONUS_BID_AMOUNT;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.ROAD_LENGTH, 0) != NVL(:OLD.ROAD_LENGTH, 0) THEN
        l_column_name := 'ROAD_LENGTH';
        l_old_value := :OLD.ROAD_LENGTH;
        l_new_value := :NEW.ROAD_LENGTH;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.DEVELOPED_TIMBER_VOLUME, 0) != NVL(:OLD.DEVELOPED_TIMBER_VOLUME, 0) THEN
        l_column_name := 'DEVELOPED_TIMBER_VOLUME';
        l_old_value := :OLD.DEVELOPED_TIMBER_VOLUME;
        l_new_value := :NEW.DEVELOPED_TIMBER_VOLUME;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.ROAD_LENGTH_CONSTRUCTED, 0) != NVL(:OLD.ROAD_LENGTH_CONSTRUCTED, 0) THEN
        l_column_name := 'ROAD_LENGTH_CONSTRUCTED';
        l_old_value := :OLD.ROAD_LENGTH_CONSTRUCTED;
        l_new_value := :NEW.ROAD_LENGTH_CONSTRUCTED;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.DEVELOPMENT_OVERRIDE_LEVY, 0) != NVL(:OLD.DEVELOPMENT_OVERRIDE_LEVY, 0) THEN
        l_column_name := 'DEVELOPMENT_OVERRIDE_LEVY';
        l_old_value := :OLD.DEVELOPMENT_OVERRIDE_LEVY;
        l_new_value := :NEW.DEVELOPMENT_OVERRIDE_LEVY;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.OVERRIDE_CALC_SILV_LEVY_IND, 0) != NVL(:OLD.OVERRIDE_CALC_SILV_LEVY_IND, 0) THEN
        l_column_name := 'OVERRIDE_CALC_SILV_LEVY_IND';
        l_old_value := :OLD.OVERRIDE_CALC_SILV_LEVY_IND;
        l_new_value := :NEW.OVERRIDE_CALC_SILV_LEVY_IND;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.SILVICULTURE_OVERRIDE_LEVY, 0) != NVL(:OLD.SILVICULTURE_OVERRIDE_LEVY, 0) THEN
        l_column_name := 'SILVICULTURE_OVERRIDE_LEVY';
        l_old_value := :OLD.SILVICULTURE_OVERRIDE_LEVY;
        l_new_value := :NEW.SILVICULTURE_OVERRIDE_LEVY;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.FENCING_DISTANCE, 0) != NVL(:OLD.FENCING_DISTANCE, 0) THEN
        l_column_name := 'FENCING_DISTANCE';
        l_old_value := :OLD.FENCING_DISTANCE;
        l_new_value := :NEW.FENCING_DISTANCE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.CATTLE_GUARD_QUANTITY, 0) != NVL(:OLD.CATTLE_GUARD_QUANTITY, 0) THEN
        l_column_name := 'CATTLE_GUARD_QUANTITY';
        l_old_value := :OLD.CATTLE_GUARD_QUANTITY;
        l_new_value := :NEW.CATTLE_GUARD_QUANTITY;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.PIPELINE_CROSSING_QUANTITY, 0) != NVL(:OLD.PIPELINE_CROSSING_QUANTITY, 0) THEN
        l_column_name := 'PIPELINE_CROSSING_QUANTITY';
        l_old_value := :OLD.PIPELINE_CROSSING_QUANTITY;
        l_new_value := :NEW.PIPELINE_CROSSING_QUANTITY;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.OBLIGATORY_DECIDUOUS_VOLUME, 0) != NVL(:OLD.OBLIGATORY_DECIDUOUS_VOLUME, 0) THEN
        l_column_name := 'OBLIGATORY_DECIDUOUS_VOLUME';
        l_old_value := :OLD.OBLIGATORY_DECIDUOUS_VOLUME;
        l_new_value := :NEW.OBLIGATORY_DECIDUOUS_VOLUME;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.NET_CONIFEROUS_VOLUME, 0) != NVL(:OLD.NET_CONIFEROUS_VOLUME, 0) THEN
        l_column_name := 'NET_CONIFEROUS_VOLUME';
        l_old_value := :OLD.NET_CONIFEROUS_VOLUME;
        l_new_value := :NEW.NET_CONIFEROUS_VOLUME;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.CP_AVG_VOL_PER_TREE, 0) != NVL(:OLD.CP_AVG_VOL_PER_TREE, 0) THEN
        l_column_name := 'CP_AVG_VOL_PER_TREE';
        l_old_value := :OLD.CP_AVG_VOL_PER_TREE;
        l_new_value := :NEW.CP_AVG_VOL_PER_TREE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.CP_AVG_VOL_PER_HA, 0) != NVL(:OLD.CP_AVG_VOL_PER_HA, 0) THEN
        l_column_name := 'CP_AVG_VOL_PER_HA';
        l_old_value := :OLD.CP_AVG_VOL_PER_HA;
        l_new_value := :NEW.CP_AVG_VOL_PER_HA;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.CP_AVG_SLOPE_PCT, 0) != NVL(:OLD.CP_AVG_SLOPE_PCT, 0) THEN
        l_column_name := 'CP_AVG_SLOPE_PCT';
        l_old_value := :OLD.CP_AVG_SLOPE_PCT;
        l_new_value := :NEW.CP_AVG_SLOPE_PCT;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.MANDATORY_SMALLLOG_UTIL_IND, 0) != NVL(:OLD.MANDATORY_SMALLLOG_UTIL_IND, 0) THEN
        l_column_name := 'MANDATORY_SMALLLOG_UTIL_IND';
        l_old_value := :OLD.MANDATORY_SMALLLOG_UTIL_IND;
        l_new_value := :NEW.MANDATORY_SMALLLOG_UTIL_IND;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.MANDATORY_GRADE_4_HEMLOCK_IND, 0) != NVL(:OLD.MANDATORY_GRADE_4_HEMLOCK_IND, 0) THEN
        l_column_name := 'MANDATORY_GRADE_4_HEMLOCK_IND';
        l_old_value := :OLD.MANDATORY_GRADE_4_HEMLOCK_IND;
        l_new_value := :NEW.MANDATORY_GRADE_4_HEMLOCK_IND;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.DISTANCE_TO_SUPPORT_CENTRE, 0) != NVL(:OLD.DISTANCE_TO_SUPPORT_CENTRE, 0) THEN
        l_column_name := 'DISTANCE_TO_SUPPORT_CENTRE';
        l_old_value := :OLD.DISTANCE_TO_SUPPORT_CENTRE;
        l_new_value := :NEW.DISTANCE_TO_SUPPORT_CENTRE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.TRUCK_HAUL_PRIMARY_CYCLE_TIME, 0) != NVL(:OLD.TRUCK_HAUL_PRIMARY_CYCLE_TIME, 0) THEN
        l_column_name := 'TRUCK_HAUL_PRIMARY_CYCLE_TIME';
        l_old_value := :OLD.TRUCK_HAUL_PRIMARY_CYCLE_TIME;
        l_new_value := :NEW.TRUCK_HAUL_PRIMARY_CYCLE_TIME;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.TRUCK_HAUL_SECOND_CYCLE_TIME, 0) != NVL(:OLD.TRUCK_HAUL_SECOND_CYCLE_TIME, 0) THEN
        l_column_name := 'TRUCK_HAUL_SECOND_CYCLE_TIME';
        l_old_value := :OLD.TRUCK_HAUL_SECOND_CYCLE_TIME;
        l_new_value := :NEW.TRUCK_HAUL_SECOND_CYCLE_TIME;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.WOODLOT_ROAD_MANAGEMENT_COST, 0) != NVL(:OLD.WOODLOT_ROAD_MANAGEMENT_COST, 0) THEN
        l_column_name := 'WOODLOT_ROAD_MANAGEMENT_COST';
        l_old_value := :OLD.WOODLOT_ROAD_MANAGEMENT_COST;
        l_new_value := :NEW.WOODLOT_ROAD_MANAGEMENT_COST;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.LAKE_TOW_DISTANCE, 0) != NVL(:OLD.LAKE_TOW_DISTANCE, 0) THEN
        l_column_name := 'LAKE_TOW_DISTANCE';
        l_old_value := :OLD.LAKE_TOW_DISTANCE;
        l_new_value := :NEW.LAKE_TOW_DISTANCE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.DEWATER_RELOAD_IND, 0) != NVL(:OLD.DEWATER_RELOAD_IND, 0) THEN
        l_column_name := 'DEWATER_RELOAD_IND';
        l_old_value := :OLD.DEWATER_RELOAD_IND;
        l_new_value := :NEW.DEWATER_RELOAD_IND;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.BARGE_FERRY_IND, 0) != NVL(:OLD.BARGE_FERRY_IND, 0) THEN
        l_column_name := 'BARGE_FERRY_IND';
        l_old_value := :OLD.BARGE_FERRY_IND;
        l_new_value := :NEW.BARGE_FERRY_IND;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.CREW_BARGE_FERRY_IND, 0) != NVL(:OLD.CREW_BARGE_FERRY_IND, 0) THEN
        l_column_name := 'CREW_BARGE_FERRY_IND';
        l_old_value := :OLD.CREW_BARGE_FERRY_IND;
        l_new_value := :NEW.CREW_BARGE_FERRY_IND;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.FRANCOIS_LAKE_FERRY_IND, 0) != NVL(:OLD.FRANCOIS_LAKE_FERRY_IND, 0) THEN
        l_column_name := 'FRANCOIS_LAKE_FERRY_IND';
        l_old_value := :OLD.FRANCOIS_LAKE_FERRY_IND;
        l_new_value := :NEW.FRANCOIS_LAKE_FERRY_IND;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.TRUCK_TO_RAIL_IND, 0) != NVL(:OLD.TRUCK_TO_RAIL_IND, 0) THEN
        l_column_name := 'TRUCK_TO_RAIL_IND';
        l_old_value := :OLD.TRUCK_TO_RAIL_IND;
        l_new_value := :NEW.TRUCK_TO_RAIL_IND;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.DEASE_LAKE_RAIL_IND, 0) != NVL(:OLD.DEASE_LAKE_RAIL_IND, 0) THEN
        l_column_name := 'DEASE_LAKE_RAIL_IND';
        l_old_value := :OLD.DEASE_LAKE_RAIL_IND;
        l_new_value := :NEW.DEASE_LAKE_RAIL_IND;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.APPRSL_CERTIFICATION_TYPE_CODE, 0) != NVL(:OLD.APPRSL_CERTIFICATION_TYPE_CODE, 0) THEN
        l_column_name := 'APPRSL_CERTIFICATION_TYPE_CODE';
        l_old_value := :OLD.APPRSL_CERTIFICATION_TYPE_CODE;
        l_new_value := :NEW.APPRSL_CERTIFICATION_TYPE_CODE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.RPF_USER_ID, 0) != NVL(:OLD.RPF_USER_ID, 0) THEN
        l_column_name := 'RPF_USER_ID';
        l_old_value := :OLD.RPF_USER_ID;
        l_new_value := :NEW.RPF_USER_ID;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;


      IF NVL(Pkg_Sil_Date_Conversion.CONVERT_TO_CHAR(:NEW.RPF_SUBMITTED_DATE, 'YYYY-MM-DD', 'Y'), 0)
           != NVL(Pkg_Sil_Date_Conversion.CONVERT_TO_CHAR(:OLD.RPF_SUBMITTED_DATE, 'YYYY-MM-DD', 'Y'), 0) THEN
        l_column_name := 'RPF_SUBMITTED_DATE';
        l_old_value := :OLD.RPF_SUBMITTED_DATE;
        l_new_value := :NEW.RPF_SUBMITTED_DATE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;


      IF NVL(Pkg_Sil_Date_Conversion.CONVERT_TO_CHAR(:NEW.LICENSEE_REP_ENTERED_DATE, 'YYYY-MM-DD', 'Y'), 0)
           != NVL(Pkg_Sil_Date_Conversion.CONVERT_TO_CHAR(:OLD.LICENSEE_REP_ENTERED_DATE, 'YYYY-MM-DD', 'Y'), 0) THEN
        l_column_name := 'LICENSEE_REP_ENTERED_DATE';
        l_old_value := :OLD.LICENSEE_REP_ENTERED_DATE;
        l_new_value := :NEW.LICENSEE_REP_ENTERED_DATE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(Pkg_Sil_Date_Conversion.CONVERT_TO_CHAR(:NEW.DISTRICT_RECEIVED_DATE, 'YYYY-MM-DD', 'Y'), 0)
           != NVL(Pkg_Sil_Date_Conversion.CONVERT_TO_CHAR(:OLD.DISTRICT_RECEIVED_DATE, 'YYYY-MM-DD', 'Y'), 0) THEN
        l_column_name := 'DISTRICT_RECEIVED_DATE';
        l_old_value := :OLD.DISTRICT_RECEIVED_DATE;
        l_new_value := :NEW.DISTRICT_RECEIVED_DATE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(Pkg_Sil_Date_Conversion.CONVERT_TO_CHAR(:NEW.SENT_DATE, 'YYYY-MM-DD', 'Y'), 0)
           != NVL(Pkg_Sil_Date_Conversion.CONVERT_TO_CHAR(:OLD.SENT_DATE, 'YYYY-MM-DD', 'Y'), 0) THEN
        l_column_name := 'SENT_DATE';
        l_old_value := :OLD.SENT_DATE;
        l_new_value := :NEW.SENT_DATE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.CLIENT_NUMBER, 0) != NVL(:OLD.CLIENT_NUMBER, 0) THEN
        l_column_name := 'CLIENT_NUMBER';
        l_old_value := :OLD.CLIENT_NUMBER;
        l_new_value := :NEW.CLIENT_NUMBER;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.CLIENT_LOCN_CODE, 0) != NVL(:OLD.CLIENT_LOCN_CODE, 0) THEN
        l_column_name := 'CLIENT_LOCN_CODE';
        l_old_value := :OLD.CLIENT_LOCN_CODE;
        l_new_value := :NEW.CLIENT_LOCN_CODE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(Pkg_Sil_Date_Conversion.CONVERT_TO_CHAR(:NEW.TRANSFER_DATE, 'YYYY-MM-DD', 'Y'), 0)
           != NVL(Pkg_Sil_Date_Conversion.CONVERT_TO_CHAR(:OLD.TRANSFER_DATE, 'YYYY-MM-DD', 'Y'), 0) THEN
        l_column_name := 'TRANSFER_DATE';
        l_old_value := :OLD.TRANSFER_DATE;
        l_new_value := :NEW.TRANSFER_DATE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.DATA_FIELD_CHECKED_IND, 0) != NVL(:OLD.DATA_FIELD_CHECKED_IND, 0) THEN
        l_column_name := 'DATA_FIELD_CHECKED_IND';
        l_old_value := :OLD.DATA_FIELD_CHECKED_IND;
        l_new_value := :NEW.DATA_FIELD_CHECKED_IND;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(Pkg_Sil_Date_Conversion.CONVERT_TO_CHAR(:NEW.NRST_PT_EFFECTIVE_DATE, 'YYYY-MM-DD', 'Y'), 0)
           != NVL(Pkg_Sil_Date_Conversion.CONVERT_TO_CHAR(:OLD.NRST_PT_EFFECTIVE_DATE, 'YYYY-MM-DD', 'Y'), 0) THEN
        l_column_name := 'NRST_PT_EFFECTIVE_DATE';
        l_old_value := :OLD.NRST_PT_EFFECTIVE_DATE;
        l_new_value := :NEW.NRST_PT_EFFECTIVE_DATE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.CP_CRUISE_CHECKED_IND, 0) != NVL(:OLD.CP_CRUISE_CHECKED_IND, 0) THEN
        l_column_name := 'CP_CRUISE_CHECKED_IND';
        l_old_value := :OLD.CP_CRUISE_CHECKED_IND;
        l_new_value := :NEW.CP_CRUISE_CHECKED_IND;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.FTAS_DATA_VALIDATED_IND, 0) != NVL(:OLD.FTAS_DATA_VALIDATED_IND, 0) THEN
        l_column_name := 'FTAS_DATA_VALIDATED_IND';
        l_old_value := :OLD.FTAS_DATA_VALIDATED_IND;
        l_new_value := :NEW.FTAS_DATA_VALIDATED_IND;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.MVI_ELIGIBLE_IND, 0) != NVL(:OLD.MVI_ELIGIBLE_IND, 0) THEN
        l_column_name := 'MVI_ELIGIBLE_IND';
        l_old_value := :OLD.MVI_ELIGIBLE_IND;
        l_new_value := :NEW.MVI_ELIGIBLE_IND;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.TOA_ELIGIBLE_IND, 0) != NVL(:OLD.TOA_ELIGIBLE_IND, 0) THEN
        l_column_name := 'TOA_ELIGIBLE_IND';
        l_old_value := :OLD.TOA_ELIGIBLE_IND;
        l_new_value := :NEW.TOA_ELIGIBLE_IND;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.MPS_CVP_DIFFERENTIAL, 0) != NVL(:OLD.MPS_CVP_DIFFERENTIAL, 0) THEN
        l_column_name := 'MPS_CVP_DIFFERENTIAL';
        l_old_value := :OLD.MPS_CVP_DIFFERENTIAL;
        l_new_value := :NEW.MPS_CVP_DIFFERENTIAL;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.RATE_ADJUSTMENT_TYPE_CODE, 0) != NVL(:OLD.RATE_ADJUSTMENT_TYPE_CODE, 0) THEN
        l_column_name := 'RATE_ADJUSTMENT_TYPE_CODE';
        l_old_value := :OLD.RATE_ADJUSTMENT_TYPE_CODE;
        l_new_value := :NEW.RATE_ADJUSTMENT_TYPE_CODE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.NO_CHANGE_IND, 0) != NVL(:OLD.NO_CHANGE_IND, 0) THEN
        l_column_name := 'NO_CHANGE_IND';
        l_old_value := :OLD.NO_CHANGE_IND;
        l_new_value := :NEW.NO_CHANGE_IND;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.STUMPAGE_RATE_EXTENSION_IND, 0) != NVL(:OLD.STUMPAGE_RATE_EXTENSION_IND, 0) THEN
        l_column_name := 'STUMPAGE_RATE_EXTENSION_IND';
        l_old_value := :OLD.STUMPAGE_RATE_EXTENSION_IND;
        l_new_value := :NEW.STUMPAGE_RATE_EXTENSION_IND;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;


      IF NVL(:NEW.BCTS_SALE_ADVERTISED_IND,0) != NVL(:OLD.BCTS_SALE_ADVERTISED_IND,0) THEN

        l_column_name := 'BCTS_SALE_ADVERTISED_IND';
        l_old_value := :OLD.BCTS_SALE_ADVERTISED_IND;
        l_new_value := :NEW.BCTS_SALE_ADVERTISED_IND;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;


      IF NVL(:NEW.BCTS_SALE_READVERTISED_IND,0) != NVL(:OLD.BCTS_SALE_READVERTISED_IND,0) THEN
        l_column_name := 'BCTS_SALE_READVERTISED_IND';
        l_old_value := :OLD.BCTS_SALE_READVERTISED_IND;
        l_new_value := :NEW.BCTS_SALE_READVERTISED_IND;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;


      IF NVL(:NEW.BCTS_VARIABLE_COST, 0) != NVL(:OLD.BCTS_VARIABLE_COST, 0) THEN
        l_column_name := 'BCTS_VARIABLE_COST';
        l_old_value := :OLD.BCTS_VARIABLE_COST;
        l_new_value := :NEW.BCTS_VARIABLE_COST;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									                    , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.SKYLINE_VOLUME, 0) != NVL(:OLD.SKYLINE_VOLUME, 0) THEN
        l_column_name := 'SKYLINE_VOLUME';
        l_old_value := :OLD.SKYLINE_VOLUME;
        l_new_value := :NEW.SKYLINE_VOLUME;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
                                      , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.ADS_LOCATION_DISTANCE_AVERAGE, 0) != NVL(:OLD.ADS_LOCATION_DISTANCE_AVERAGE, 0) THEN
        l_column_name := 'ADS_LOCATION_DISTANCE_AVERAGE';
        l_old_value := :OLD.ADS_LOCATION_DISTANCE_AVERAGE;
        l_new_value := :NEW.ADS_LOCATION_DISTANCE_AVERAGE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
                                      , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.CP_PLOTS_PER_HECTARE, 0) != NVL(:OLD.CP_PLOTS_PER_HECTARE, 0) THEN
        l_column_name := 'CP_PLOTS_PER_HECTARE';
        l_old_value := :OLD.CP_PLOTS_PER_HECTARE;
        l_new_value := :NEW.CP_PLOTS_PER_HECTARE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
                                      , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.CP_TREES_PER_PLOT, 0) != NVL(:OLD.CP_TREES_PER_PLOT, 0) THEN
        l_column_name := 'CP_TREES_PER_PLOT';
        l_old_value := :OLD.CP_TREES_PER_PLOT;
        l_new_value := :NEW.CP_TREES_PER_PLOT;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
                                      , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(Pkg_Sil_Date_Conversion.CONVERT_TO_CHAR(:NEW.CP_CRUISE_DATE, 'YYYY-MM-DD', 'Y'), 0)
           != NVL(Pkg_Sil_Date_Conversion.CONVERT_TO_CHAR(:OLD.CP_CRUISE_DATE, 'YYYY-MM-DD', 'Y'), 0) THEN
        l_column_name := 'CP_CRUISE_DATE';
        l_old_value := :OLD.CP_CRUISE_DATE;
        l_new_value := :NEW.CP_CRUISE_DATE;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
                                      , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.ROOT_DISEASE_CONTROL_IND, 0) != NVL(:OLD.ROOT_DISEASE_CONTROL_IND, 0) THEN
        l_column_name := 'ROOT_DISEASE_CONTROL_IND';
        l_old_value := :OLD.ROOT_DISEASE_CONTROL_IND;
        l_new_value := :NEW.ROOT_DISEASE_CONTROL_IND;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
                                      , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.ROOT_DISEASE_CONTROL_COST, 0) != NVL(:OLD.ROOT_DISEASE_CONTROL_COST, 0) THEN
        l_column_name := 'ROOT_DISEASE_CONTROL_COST';
        l_old_value := :OLD.ROOT_DISEASE_CONTROL_COST;
        l_new_value := :NEW.ROOT_DISEASE_CONTROL_COST;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
                                      , l_business_id
                                      , l_error_message);

      END IF;

      IF NVL(:NEW.CONIF_STAND_RATE_ELIG_CODE, 0) != NVL(:OLD.CONIF_STAND_RATE_ELIG_CODE, 0) THEN
      	l_column_name := 'CONIF_STAND_RATE_ELIG_CODE';
      	l_old_value := :OLD.CONIF_STAND_RATE_ELIG_CODE;
      	l_new_value := :NEW.CONIF_STAND_RATE_ELIG_CODE;
      	Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
      				      , l_audit_event_id
      				      , l_table_name
      				      , l_column_name
      				      , l_old_value
      				      , l_new_value
      				      , l_userid
      				      , l_business_id
      				      , l_error_message);
      END IF;

      IF NVL(:NEW.DECID_STAND_RATE_ELIG_CODE, 0) != NVL(:OLD.DECID_STAND_RATE_ELIG_CODE, 0) THEN
	      l_column_name := 'DECID_STAND_RATE_ELIG_CODE';
	      l_old_value := :OLD.DECID_STAND_RATE_ELIG_CODE;
	      l_new_value := :NEW.DECID_STAND_RATE_ELIG_CODE;
	      Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
				            , l_audit_event_id
				            , l_table_name
				            , l_column_name
				            , l_old_value
				            , l_new_value
				            , l_userid
				            , l_business_id
				            , l_error_message);
      END IF;

      IF NVL(:NEW.NRFL_BONUS_BID_AMOUNT, 0) != NVL(:OLD.NRFL_BONUS_BID_AMOUNT, 0) THEN
        l_column_name := 'NRFL_BONUS_BID_AMOUNT';
        l_old_value := :OLD.NRFL_BONUS_BID_AMOUNT;
        l_new_value := :NEW.NRFL_BONUS_BID_AMOUNT;
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                      , l_audit_event_id
                      , l_table_name
                      , l_column_name
                      , l_old_value
                      , l_new_value
                      , l_userid
                      , l_business_id
                      , l_error_message);
      END IF;

      IF NVL(:NEW.RW_VOLUME, 0) != NVL(:OLD.RW_VOLUME, 0) THEN
        l_column_name := 'RW_VOLUME';
        l_old_value := :OLD.RW_VOLUME;
        l_new_value := :NEW.RW_VOLUME;
	      Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
				      , l_audit_event_id
				      , l_table_name
				      , l_column_name
				      , l_old_value
				      , l_new_value
				      , l_userid
				      , l_business_id
				      , l_error_message);
      END IF;

      IF NVL(:NEW.RW_AREA, 0) != NVL(:OLD.RW_AREA, 0) THEN
        l_column_name := 'RW_AREA';
        l_old_value := :OLD.RW_AREA;
        l_new_value := :NEW.RW_AREA;
      	Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
      				      , l_audit_event_id
      				      , l_table_name
      				      , l_column_name
      				      , l_old_value
      				      , l_new_value
      				      , l_userid
      				      , l_business_id
      				      , l_error_message);
      END IF;

      IF NVL(:NEW.UPSET_VALUE, 0) != NVL(:OLD.UPSET_VALUE, 0) THEN
        l_column_name := 'UPSET_VALUE';
        l_old_value := :OLD.UPSET_VALUE;
        l_new_value := :NEW.UPSET_VALUE;
      	Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
      				      , l_audit_event_id
      				      , l_table_name
      				      , l_column_name
      				      , l_old_value
      				      , l_new_value
      				      , l_userid
      				      , l_business_id
      				      , l_error_message);
      END IF;

      IF NVL(:NEW.BONUS_OFFER, 0) != NVL(:OLD.BONUS_OFFER, 0) THEN
        l_column_name := 'BONUS_OFFER';
        l_old_value := :OLD.BONUS_OFFER;
        l_new_value := :NEW.BONUS_OFFER;
      	Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
      				      , l_audit_event_id
      				      , l_table_name
      				      , l_column_name
      				      , l_old_value
      				      , l_new_value
      				      , l_userid
      				      , l_business_id
      				      , l_error_message);
      END IF;

      IF NVL(:NEW.INTERIOR_BONUS_BID_AMOUNT, 0) != NVL(:OLD.INTERIOR_BONUS_BID_AMOUNT, 0) THEN
        l_column_name := 'INTERIOR_BONUS_BID_AMOUNT';
        l_old_value := :OLD.INTERIOR_BONUS_BID_AMOUNT;
        l_new_value := :NEW.INTERIOR_BONUS_BID_AMOUNT;
      	Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
      				      , l_audit_event_id
      				      , l_table_name
      				      , l_column_name
      				      , l_old_value
      				      , l_new_value
      				      , l_userid
      				      , l_business_id
      				      , l_error_message);
      END IF;

      IF NVL(:NEW.CRUISE_COMPILATION_TYPE_CODE, 0) != NVL(:OLD.CRUISE_COMPILATION_TYPE_CODE, 0) THEN
        l_column_name := 'CRUISE_COMPILATION_TYPE_CODE';
        l_old_value := :OLD.CRUISE_COMPILATION_TYPE_CODE;
        l_new_value := :NEW.CRUISE_COMPILATION_TYPE_CODE;
      	Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
      				      , l_audit_event_id
      				      , l_table_name
      				      , l_column_name
      				      , l_old_value
      				      , l_new_value
      				      , l_userid
      				      , l_business_id
      				      , l_error_message);
      END IF;

      IF NVL(:NEW.LOG_DEBRIS_FENCING, 0) != NVL(:OLD.LOG_DEBRIS_FENCING, 0) THEN
        l_column_name := 'LOG_DEBRIS_FENCING';
        l_old_value := :OLD.LOG_DEBRIS_FENCING;
        l_new_value := :NEW.LOG_DEBRIS_FENCING;
      	Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
      				      , l_audit_event_id
      				      , l_table_name
      				      , l_column_name
      				      , l_old_value
      				      , l_new_value
      				      , l_userid
      				      , l_business_id
      				      , l_error_message);
      END IF;

      IF NVL(:NEW.CERTIFIED_FLAG, 0) != NVL(:OLD.CERTIFIED_FLAG, 0) THEN
        l_column_name := 'CERTIFIED_FLAG';
        l_old_value := :OLD.CERTIFIED_FLAG;
        l_new_value := :NEW.CERTIFIED_FLAG;
      	Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
      				      , l_audit_event_id
      				      , l_table_name
      				      , l_column_name
      				      , l_old_value
      				      , l_new_value
      				      , l_userid
      				      , l_business_id
      				      , l_error_message);
      END IF;

      IF NVL(Pkg_Sil_Date_Conversion.CONVERT_TO_CHAR(:NEW.CERTIFIED_TIMESTAMP, 'YYYY-MM-DD', 'Y'), 0) != NVL(Pkg_Sil_Date_Conversion.CONVERT_TO_CHAR(:OLD.CERTIFIED_TIMESTAMP, 'YYYY-MM-DD', 'Y'), 0) THEN
        l_column_name := 'CERTIFIED_TIMESTAMP';
        l_old_value := :OLD.CERTIFIED_TIMESTAMP;
        l_new_value := :NEW.CERTIFIED_TIMESTAMP;
      	Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
      				      , l_audit_event_id
      				      , l_table_name
      				      , l_column_name
      				      , l_old_value
      				      , l_new_value
      				      , l_userid
      				      , l_business_id
      				      , l_error_message);
      END IF;

      IF NVL(:NEW.NUMBER_OF_ISSUES, 0) != NVL(:OLD.NUMBER_OF_ISSUES, 0) THEN
        l_column_name := 'NUMBER_OF_ISSUES';
        l_old_value := :OLD.NUMBER_OF_ISSUES;
        l_new_value := :NEW.NUMBER_OF_ISSUES;
      	Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
      				      , l_audit_event_id
      				      , l_table_name
      				      , l_column_name
      				      , l_old_value
      				      , l_new_value
      				      , l_userid
      				      , l_business_id
      				      , l_error_message);
      END IF;

      IF NVL(:NEW.DISTRICT_NUMBER_OF_ISSUES, 0) != NVL(:OLD.DISTRICT_NUMBER_OF_ISSUES, 0) THEN
        l_column_name := 'DISTRICT_NUMBER_OF_ISSUES';
        l_old_value := :OLD.DISTRICT_NUMBER_OF_ISSUES;
        l_new_value := :NEW.DISTRICT_NUMBER_OF_ISSUES;
      	Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
      				      , l_audit_event_id
      				      , l_table_name
      				      , l_column_name
      				      , l_old_value
      				      , l_new_value
      				      , l_userid
      				      , l_business_id
      				      , l_error_message);
      END IF;

      IF NVL(:NEW.RESIDE_IN_CAMP_IND, 0) != NVL(:OLD.RESIDE_IN_CAMP_IND, 0) THEN
        l_column_name := 'RESIDE_IN_CAMP_IND';
        l_old_value := :OLD.RESIDE_IN_CAMP_IND;
        l_new_value := :NEW.RESIDE_IN_CAMP_IND;
      	Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
      				      , l_audit_event_id
      				      , l_table_name
      				      , l_column_name
      				      , l_old_value
      				      , l_new_value
      				      , l_userid
      				      , l_business_id
      				      , l_error_message);
      END IF;

      IF NVL(:NEW.TRANSPORT_BY_BARGE_IND, 0) != NVL(:OLD.TRANSPORT_BY_BARGE_IND, 0) THEN
        l_column_name := 'TRANSPORT_BY_BARGE_IND';
        l_old_value := :OLD.TRANSPORT_BY_BARGE_IND;
        l_new_value := :NEW.TRANSPORT_BY_BARGE_IND;
      	Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
      				      , l_audit_event_id
      				      , l_table_name
      				      , l_column_name
      				      , l_old_value
      				      , l_new_value
      				      , l_userid
      				      , l_business_id
      				      , l_error_message);
      END IF;

      IF NVL(:NEW.WT_APPLICABLE_VOLUME, 0) != NVL(:OLD.WT_APPLICABLE_VOLUME, 0) THEN
        l_column_name := 'WT_APPLICABLE_VOLUME';
        l_old_value := :OLD.WT_APPLICABLE_VOLUME;
        l_new_value := :NEW.WT_APPLICABLE_VOLUME;
      	Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
      				      , l_audit_event_id
      				      , l_table_name
      				      , l_column_name
      				      , l_old_value
      				      , l_new_value
      				      , l_userid
      				      , l_business_id
      				      , l_error_message);
      END IF;

      IF NVL(:NEW.STS_APPLICABLE_VOLUME, 0) != NVL(:OLD.STS_APPLICABLE_VOLUME, 0) THEN
        l_column_name := 'STS_APPLICABLE_VOLUME';
        l_old_value := :OLD.STS_APPLICABLE_VOLUME;
        l_new_value := :NEW.STS_APPLICABLE_VOLUME;
      	Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
      				      , l_audit_event_id
      				      , l_table_name
      				      , l_column_name
      				      , l_old_value
      				      , l_new_value
      				      , l_userid
      				      , l_business_id
      				      , l_error_message);
      END IF;

      IF NVL(:NEW.CAMPS_APPLICABLE_VOLUME, 0) != NVL(:OLD.CAMPS_APPLICABLE_VOLUME, 0) THEN
        l_column_name := 'CAMPS_APPLICABLE_VOLUME';
        l_old_value := :OLD.CAMPS_APPLICABLE_VOLUME;
        l_new_value := :NEW.CAMPS_APPLICABLE_VOLUME;
      	Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
      				      , l_audit_event_id
      				      , l_table_name
      				      , l_column_name
      				      , l_old_value
      				      , l_new_value
      				      , l_userid
      				      , l_business_id
      				      , l_error_message);
      END IF;

      IF NVL(:NEW.FIBRE_RECOVERY_ZONE_FRACTION, 0) != NVL(:OLD.FIBRE_RECOVERY_ZONE_FRACTION, 0) THEN
        l_column_name := 'FIBRE_RECOVERY_ZONE_FRACTION';
        l_old_value := :OLD.FIBRE_RECOVERY_ZONE_FRACTION;
        l_new_value := :NEW.FIBRE_RECOVERY_ZONE_FRACTION;
      	Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
      				      , l_audit_event_id
      				      , l_table_name
      				      , l_column_name
      				      , l_old_value
      				      , l_new_value
      				      , l_userid
      				      , l_business_id
      				      , l_error_message);
      END IF;

      IF NVL(:NEW.TCM_OLD_GRWTH_NUMBER_OF_TREES, 0) != NVL(:OLD.TCM_OLD_GRWTH_NUMBER_OF_TREES, 0) THEN
        l_column_name := 'TCM_OLD_GRWTH_NUMBER_OF_TREES';
        l_old_value := :OLD.TCM_OLD_GRWTH_NUMBER_OF_TREES;
        l_new_value := :NEW.TCM_OLD_GRWTH_NUMBER_OF_TREES;
      	Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
      				      , l_audit_event_id
      				      , l_table_name
      				      , l_column_name
      				      , l_old_value
      				      , l_new_value
      				      , l_userid
      				      , l_business_id
      				      , l_error_message);
      END IF;

      IF NVL(:NEW.TCM_2ND_GRWTH_NUMBER_OF_TREES, 0) != NVL(:OLD.TCM_2ND_GRWTH_NUMBER_OF_TREES, 0) THEN
        l_column_name := 'TCM_2ND_GRWTH_NUMBER_OF_TREES';
        l_old_value := :OLD.TCM_2ND_GRWTH_NUMBER_OF_TREES;
        l_new_value := :NEW.TCM_2ND_GRWTH_NUMBER_OF_TREES;
      	Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
      				      , l_audit_event_id
      				      , l_table_name
      				      , l_column_name
      				      , l_old_value
      				      , l_new_value
      				      , l_userid
      				      , l_business_id
      				      , l_error_message);
      END IF;


    END IF; --l_audit_event_id is not null

  ELSIF DELETING THEN
    l_ecas_id := :OLD.ecas_id;
    l_business_id := 'Ecas Id: '||:OLD.ECAS_ID;

    Pkg_Ecas_Audit.GET_USERID(l_userid
                            , 'DEL'
                            , l_ecas_id
                            , SYSDATE
                            , l_error_message);

    Pkg_Ecas_Audit.CHECK_EVENT_ID(l_audit_event_id
                                , l_ecas_id
                                , 'DEL'
                                , l_userid
                                , SYSDATE
                                , l_error_message);

    IF l_audit_event_id IS NOT NULL THEN

      l_column_name := 'FOREST_FILE_ID';
      l_old_value := :OLD.FOREST_FILE_ID;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'CUTTING_PERMIT_ID';
      l_old_value := :OLD.CUTTING_PERMIT_ID;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'APPRAISAL_STATUS_CODE';
      l_old_value := :OLD.APPRAISAL_STATUS_CODE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'APPRAISAL_CATEGORY_CODE';
      l_old_value := :OLD.APPRAISAL_CATEGORY_CODE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'APPRAISAL_EFFECTIVE_DATE';
      l_old_value := :OLD.APPRAISAL_EFFECTIVE_DATE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'APPRAISAL_EXPIRY_DATE';
      l_old_value := :OLD.APPRAISAL_EXPIRY_DATE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'RATE_CALC_METHOD_CODE';
      l_old_value := :OLD.RATE_CALC_METHOD_CODE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'ADMIN_DISTRICT';
      l_old_value := :OLD.ADMIN_DISTRICT;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'TSB_NUMBER_CODE';
      l_old_value := :OLD.TSB_NUMBER_CODE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'NET_CRUISE_VOLUME';
      l_old_value := :OLD.NET_CRUISE_VOLUME;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'SECOND_GROWTH_CONIFER_VOLUME';
      l_old_value := :OLD.SECOND_GROWTH_CONIFER_VOLUME;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'SECOND_GROWTH_IND';
      l_old_value := :OLD.SECOND_GROWTH_IND;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'SINGLE_TREE_CRUISE_IND';
      l_old_value := :OLD.SINGLE_TREE_CRUISE_IND;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'INITIAL_MERCHANTABLE_AREA';
      l_old_value := :OLD.INITIAL_MERCHANTABLE_AREA;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'SINGLE_TREE_SELECTION_VOLUME';
      l_old_value := :OLD.SINGLE_TREE_SELECTION_VOLUME;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'SALVAGE_IND';
      l_old_value := :OLD.SALVAGE_IND;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'COMPARATIVE_CRUISE_IND';
      l_old_value := :OLD.COMPARATIVE_CRUISE_IND;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'STANDARD_ERROR_PCT';
      l_old_value := :OLD.STANDARD_ERROR_PCT;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'SPECIFIED_OPERATION_IND';
      l_old_value := :OLD.SPECIFIED_OPERATION_IND;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'NET_MERCHANTABLE_AREA';
      l_old_value := :OLD.NET_MERCHANTABLE_AREA;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'REFERENCE_MARK';
      l_old_value := :OLD.REFERENCE_MARK;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'ROAD_USE_CHARGE';
      l_old_value := :OLD.ROAD_USE_CHARGE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'ROUTINE_ROAD_MAINTENANCE_IND';
      l_old_value := :OLD.ROUTINE_ROAD_MAINTENANCE_IND;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'BASIC_SILV_REQUIRED_IND';
      l_old_value := :OLD.BASIC_SILV_REQUIRED_IND;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'ISOLATION_TYPE_CODE';
      l_old_value := :OLD.ISOLATION_TYPE_CODE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'AVERAGE_SIDE_SLOPE_PCT';
      l_old_value := :OLD.AVERAGE_SIDE_SLOPE_PCT;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'CABLE_YARDING_VOLUME';
      l_old_value := :OLD.CABLE_YARDING_VOLUME;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'GROUND_SYSTEMS_VOLUME';
      l_old_value := :OLD.GROUND_SYSTEMS_VOLUME;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'HELI_LAND_DROP_VOLUME';
      l_old_value := :OLD.HELI_LAND_DROP_VOLUME;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'HELI_WATER_DROP_VOLUME';
      l_old_value := :OLD.HELI_WATER_DROP_VOLUME;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'SINGLE_STEM_VOLUME';
      l_old_value := :OLD.SINGLE_STEM_VOLUME;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'CREW_TRANSPORT_DISTANCE';
      l_old_value := :OLD.CREW_TRANSPORT_DISTANCE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'RAIL_LOCATION_CODE';
      l_old_value := :OLD.RAIL_LOCATION_CODE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'LOCATION_DESCRIPTION';
      l_old_value := :OLD.LOCATION_DESCRIPTION;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'CULVERT_APPLICABLE_VOLUME';
      l_old_value := :OLD.CULVERT_APPLICABLE_VOLUME;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'HAUL_DISTANCE';
      l_old_value := :OLD.HAUL_DISTANCE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'WOODLOT_PERIOD_CODE';
      l_old_value := :OLD.WOODLOT_PERIOD_CODE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'TAB_ROAD_APPLICABLE_VOLUME';
      l_old_value := :OLD.TAB_ROAD_APPLICABLE_VOLUME;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'TRUCK_HAUL_METHOD_CODE';
      l_old_value := :OLD.TRUCK_HAUL_METHOD_CODE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'LAKE_TRANSPORT_CODE';
      l_old_value := :OLD.LAKE_TRANSPORT_CODE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'APP_SUPPORT_CENTRE_CODE';
      l_old_value := :OLD.APP_SUPPORT_CENTRE_CODE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'POINT_OF_APPRAISAL_CODE';
      l_old_value := :OLD.POINT_OF_APPRAISAL_CODE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'WEIGHTED_POINT_OF_ORIGIN_COST';
      l_old_value := :OLD.WEIGHTED_POINT_OF_ORIGIN_COST;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'NRST_POINT_OF_ORIGIN_COST';
      l_old_value := :OLD.NRST_POINT_OF_ORIGIN_COST;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'WTR_PT_OF_ORIGIN1';
      l_old_value := :OLD.WTR_PT_OF_ORIGIN1;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'POINT_OF_ORIGIN_DIFFERENCE';
      l_old_value := :OLD.POINT_OF_ORIGIN_DIFFERENCE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'WTR_PT_OF_ORIGIN1_DATE';
      l_old_value := :OLD.WTR_PT_OF_ORIGIN1_DATE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'WEIGHTED_HAUL_DISTANCE';
      l_old_value := :OLD.WEIGHTED_HAUL_DISTANCE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'WTR_PT_OF_ORIGIN2';
      l_old_value := :OLD.WTR_PT_OF_ORIGIN2;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'ACCESSIBLE_ISOLATED_ADDITIVE';
      l_old_value := :OLD.ACCESSIBLE_ISOLATED_ADDITIVE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'WTR_PT_OF_ORIGIN2_DATE';
      l_old_value := :OLD.WTR_PT_OF_ORIGIN2_DATE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'ROAD_MAINTENANCE_ADDITIVE';
      l_old_value := :OLD.ROAD_MAINTENANCE_ADDITIVE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
      Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                    , l_audit_event_id
                                    , l_table_name
                                    , l_column_name
                                    , l_old_value
                                    , l_new_value
                                    , l_userid
 									, l_business_id
                                    , l_error_message);
      END IF;

      l_column_name := 'LOG_TRANSPORTATION_ADDITIVE';
      l_old_value := :OLD.LOG_TRANSPORTATION_ADDITIVE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'TOWING_BARGING_ADDITIVE';
      l_old_value := :OLD.TOWING_BARGING_ADDITIVE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'WEIGHTED_CREW_TRANS_DISTANCE';
      l_old_value := :OLD.WEIGHTED_CREW_TRANS_DISTANCE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'NRST_PT_OF_ORIGIN';
      l_old_value := :OLD.NRST_PT_OF_ORIGIN;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'APPRAISAL_SELL_PRICE_ZONE_CODE';
      l_old_value := :OLD.APPRAISAL_SELL_PRICE_ZONE_CODE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'BCTS_SALE_TYPE_CODE';
      l_old_value := :OLD.BCTS_SALE_TYPE_CODE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'BCTS_SALE_DATE';
      l_old_value := :OLD.BCTS_SALE_DATE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'SALE_TERM_YEAR';
      l_old_value := :OLD.SALE_TERM_YEAR;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'SALE_TERM_MONTH';
      l_old_value := :OLD.SALE_TERM_MONTH;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'DEVELOPMENT_LEVY_IND';
      l_old_value := :OLD.DEVELOPMENT_LEVY_IND;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'SILVICULTURE_LEVY_IND';
      l_old_value := :OLD.SILVICULTURE_LEVY_IND;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'COAST_BONUS_BID_AMOUNT';
      l_old_value := :OLD.COAST_BONUS_BID_AMOUNT;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'ROAD_LENGTH';
      l_old_value := :OLD.ROAD_LENGTH;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'DEVELOPED_TIMBER_VOLUME';
      l_old_value := :OLD.DEVELOPED_TIMBER_VOLUME;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'ROAD_LENGTH_CONSTRUCTED';
      l_old_value := :OLD.ROAD_LENGTH_CONSTRUCTED;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'DEVELOPMENT_OVERRIDE_LEVY';
      l_old_value := :OLD.DEVELOPMENT_OVERRIDE_LEVY;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'OVERRIDE_CALC_SILV_LEVY_IND';
      l_old_value := :OLD.OVERRIDE_CALC_SILV_LEVY_IND;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'SILVICULTURE_OVERRIDE_LEVY';
      l_old_value := :OLD.SILVICULTURE_OVERRIDE_LEVY;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'FENCING_DISTANCE';
      l_old_value := :OLD.FENCING_DISTANCE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'CATTLE_GUARD_QUANTITY';
      l_old_value := :OLD.CATTLE_GUARD_QUANTITY;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'PIPELINE_CROSSING_QUANTITY';
      l_old_value := :OLD.PIPELINE_CROSSING_QUANTITY;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'OBLIGATORY_DECIDUOUS_VOLUME';
      l_old_value := :OLD.OBLIGATORY_DECIDUOUS_VOLUME;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'NET_CONIFEROUS_VOLUME';
      l_old_value := :OLD.NET_CONIFEROUS_VOLUME;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'CP_AVG_VOL_PER_TREE';
      l_old_value := :OLD.CP_AVG_VOL_PER_TREE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'CP_AVG_VOL_PER_HA';
      l_old_value := :OLD.CP_AVG_VOL_PER_HA;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'CP_AVG_SLOPE_PCT';
      l_old_value := :OLD.CP_AVG_SLOPE_PCT;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'MANDATORY_SMALLLOG_UTIL_IND';
      l_old_value := :OLD.MANDATORY_SMALLLOG_UTIL_IND;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'MANDATORY_GRADE_4_HEMLOCK_IND';
      l_old_value := :OLD.MANDATORY_GRADE_4_HEMLOCK_IND;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'DISTANCE_TO_SUPPORT_CENTRE';
      l_old_value := :OLD.DISTANCE_TO_SUPPORT_CENTRE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'TRUCK_HAUL_PRIMARY_CYCLE_TIME';
      l_old_value := :OLD.TRUCK_HAUL_PRIMARY_CYCLE_TIME;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'TRUCK_HAUL_SECOND_CYCLE_TIME';
      l_old_value := :OLD.TRUCK_HAUL_SECOND_CYCLE_TIME;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'WOODLOT_ROAD_MANAGEMENT_COST';
      l_old_value := :OLD.WOODLOT_ROAD_MANAGEMENT_COST;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'LAKE_TOW_DISTANCE';
      l_old_value := :OLD.LAKE_TOW_DISTANCE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'DEWATER_RELOAD_IND';
      l_old_value := :OLD.DEWATER_RELOAD_IND;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'BARGE_FERRY_IND';
      l_old_value := :OLD.BARGE_FERRY_IND;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'CREW_BARGE_FERRY_IND';
      l_old_value := :OLD.CREW_BARGE_FERRY_IND;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'FRANCOIS_LAKE_FERRY_IND';
      l_old_value := :OLD.FRANCOIS_LAKE_FERRY_IND;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'TRUCK_TO_RAIL_IND';
      l_old_value := :OLD.TRUCK_TO_RAIL_IND;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'DEASE_LAKE_RAIL_IND';
      l_old_value := :OLD.DEASE_LAKE_RAIL_IND;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'APPRSL_CERTIFICATION_TYPE_CODE';
      l_old_value := :OLD.APPRSL_CERTIFICATION_TYPE_CODE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'RPF_USER_ID';
      l_old_value := :OLD.RPF_USER_ID;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'RPF_SUBMITTED_DATE';
      l_old_value := :OLD.RPF_SUBMITTED_DATE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;


      l_column_name := 'LICENSEE_REP_ENTERED_DATE';
      l_old_value := :OLD.LICENSEE_REP_ENTERED_DATE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'DISTRICT_RECEIVED_DATE';
      l_old_value := :OLD.DISTRICT_RECEIVED_DATE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'SENT_DATE';
      l_old_value := :OLD.SENT_DATE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'CLIENT_NUMBER';
      l_old_value := :OLD.CLIENT_NUMBER;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'CLIENT_LOCN_CODE';
      l_old_value := :OLD.CLIENT_LOCN_CODE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'TRANSFER_DATE';
      l_old_value := :OLD.TRANSFER_DATE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;


      l_column_name := 'DATA_FIELD_CHECKED_IND';
      l_old_value := :OLD.DATA_FIELD_CHECKED_IND;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'NRST_PT_EFFECTIVE_DATE';
      l_old_value := :OLD.NRST_PT_EFFECTIVE_DATE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'CP_CRUISE_CHECKED_IND';
      l_old_value := :OLD.CP_CRUISE_CHECKED_IND;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'FTAS_DATA_VALIDATED_IND';
      l_old_value := :OLD.FTAS_DATA_VALIDATED_IND;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'MVI_ELIGIBLE_IND';
      l_old_value := :OLD.MVI_ELIGIBLE_IND;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'TOA_ELIGIBLE_IND';
      l_old_value := :OLD.TOA_ELIGIBLE_IND;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'MPS_CVP_DIFFERENTIAL';
      l_old_value := :OLD.MPS_CVP_DIFFERENTIAL;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'RATE_ADJUSTMENT_TYPE_CODE';
      l_old_value := :OLD.RATE_ADJUSTMENT_TYPE_CODE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'NO_CHANGE_IND';
      l_old_value := :OLD.NO_CHANGE_IND;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'STUMPAGE_RATE_EXTENSION_IND';
      l_old_value := :OLD.STUMPAGE_RATE_EXTENSION_IND;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
 									  , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'SKYLINE_VOLUME';
      l_old_value := :OLD.SKYLINE_VOLUME;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
                             , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'ADS_LOCATION_DISTANCE_AVERAGE';
      l_old_value := :OLD.ADS_LOCATION_DISTANCE_AVERAGE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
                             , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'CP_PLOTS_PER_HECTARE';
      l_old_value := :OLD.CP_PLOTS_PER_HECTARE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
                                      , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'CP_TREES_PER_PLOT';
      l_old_value := :OLD.CP_TREES_PER_PLOT;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
                                      , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'CP_CRUISE_DATE';
      l_old_value := :OLD.CP_CRUISE_DATE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
                                      , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'ROOT_DISEASE_CONTROL_IND';
      l_old_value := :OLD.ROOT_DISEASE_CONTROL_IND;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
                                      , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'ROOT_DISEASE_CONTROL_COST';
      l_old_value := :OLD.ROOT_DISEASE_CONTROL_COST;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
        Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
                                      , l_audit_event_id
                                      , l_table_name
                                      , l_column_name
                                      , l_old_value
                                      , l_new_value
                                      , l_userid
                                      , l_business_id
                                      , l_error_message);
      END IF;

      l_column_name := 'CONIF_STAND_RATE_ELIG_CODE';
      l_old_value := :OLD.CONIF_STAND_RATE_ELIG_CODE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
      	Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
      				      , l_audit_event_id
      				      , l_table_name
      				      , l_column_name
      				      , l_old_value
      				      , l_new_value
      				      , l_userid
      				      , l_business_id
      				      , l_error_message);
      END IF;

      l_column_name := 'DECID_STAND_RATE_ELIG_CODE';
      l_old_value := :OLD.DECID_STAND_RATE_ELIG_CODE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
	Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
				      , l_audit_event_id
				      , l_table_name
				      , l_column_name
				      , l_old_value
				      , l_new_value
				      , l_userid
				      , l_business_id
				      , l_error_message);
      END IF;

      l_column_name := 'NRFL_BONUS_BID_AMOUNT';
      l_old_value := :OLD.NRFL_BONUS_BID_AMOUNT;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
      	Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
      				      , l_audit_event_id
      				      , l_table_name
      				      , l_column_name
      				      , l_old_value
      				      , l_new_value
      				      , l_userid
      				      , l_business_id
      				      , l_error_message);
      END IF;

      l_column_name := 'RW_VOLUME';
      l_old_value := :OLD.RW_VOLUME;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
	Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
				      , l_audit_event_id
				      , l_table_name
				      , l_column_name
				      , l_old_value
				      , l_new_value
				      , l_userid
				      , l_business_id
				      , l_error_message);
      END IF;

      l_column_name := 'RW_AREA';
      l_old_value := :OLD.RW_AREA;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
	Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
				      , l_audit_event_id
				      , l_table_name
				      , l_column_name
				      , l_old_value
				      , l_new_value
				      , l_userid
				      , l_business_id
				      , l_error_message);
      END IF;

      l_column_name := 'UPSET_VALUE';
      l_old_value := :OLD.UPSET_VALUE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
      	Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
      				      , l_audit_event_id
      				      , l_table_name
      				      , l_column_name
      				      , l_old_value
      				      , l_new_value
      				      , l_userid
      				      , l_business_id
      				      , l_error_message);
      END IF;

      l_column_name := 'BONUS_OFFER';
      l_old_value := :OLD.BONUS_OFFER;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
      	Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
      				      , l_audit_event_id
      				      , l_table_name
      				      , l_column_name
      				      , l_old_value
      				      , l_new_value
      				      , l_userid
      				      , l_business_id
      				      , l_error_message);
      END IF;

      l_column_name := 'INTERIOR_BONUS_BID_AMOUNT';
      l_old_value := :OLD.INTERIOR_BONUS_BID_AMOUNT;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
      	Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
      				      , l_audit_event_id
      				      , l_table_name
      				      , l_column_name
      				      , l_old_value
      				      , l_new_value
      				      , l_userid
      				      , l_business_id
      				      , l_error_message);
      END IF;

      l_column_name := 'CRUISE_COMPILATION_TYPE_CODE';
      l_old_value := :OLD.CRUISE_COMPILATION_TYPE_CODE;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
      	Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
      				      , l_audit_event_id
      				      , l_table_name
      				      , l_column_name
      				      , l_old_value
      				      , l_new_value
      				      , l_userid
      				      , l_business_id
      				      , l_error_message);
      END IF;

      l_column_name := 'LOG_DEBRIS_FENCING';
      l_old_value := :OLD.LOG_DEBRIS_FENCING;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
      	Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
      				      , l_audit_event_id
      				      , l_table_name
      				      , l_column_name
      				      , l_old_value
      				      , l_new_value
      				      , l_userid
      				      , l_business_id
      				      , l_error_message);
      END IF;

      l_column_name := 'CERTIFIED_FLAG';
      l_old_value := :OLD.CERTIFIED_FLAG;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
      	Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
      				      , l_audit_event_id
      				      , l_table_name
      				      , l_column_name
      				      , l_old_value
      				      , l_new_value
      				      , l_userid
      				      , l_business_id
      				      , l_error_message);
      END IF;

      l_column_name := 'CERTIFIED_TIMESTAMP';
      l_old_value := :OLD.CERTIFIED_TIMESTAMP;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
      	Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
      				      , l_audit_event_id
      				      , l_table_name
      				      , l_column_name
      				      , l_old_value
      				      , l_new_value
      				      , l_userid
      				      , l_business_id
      				      , l_error_message);
      END IF;

      l_column_name := 'NUMBER_OF_ISSUES';
      l_old_value := :OLD.NUMBER_OF_ISSUES;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
      	Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
      				      , l_audit_event_id
      				      , l_table_name
      				      , l_column_name
      				      , l_old_value
      				      , l_new_value
      				      , l_userid
      				      , l_business_id
      				      , l_error_message);
      END IF;

      l_column_name := 'DISTRICT_NUMBER_OF_ISSUES';
      l_old_value := :OLD.DISTRICT_NUMBER_OF_ISSUES;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
      	Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
      				      , l_audit_event_id
      				      , l_table_name
      				      , l_column_name
      				      , l_old_value
      				      , l_new_value
      				      , l_userid
      				      , l_business_id
      				      , l_error_message);
      END IF;

      l_column_name := 'RESIDE_IN_CAMP_IND';
      l_old_value := :OLD.RESIDE_IN_CAMP_IND;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
      	Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
      				      , l_audit_event_id
      				      , l_table_name
      				      , l_column_name
      				      , l_old_value
      				      , l_new_value
      				      , l_userid
      				      , l_business_id
      				      , l_error_message);
      END IF;

      l_column_name := 'TRANSPORT_BY_BARGE_IND';
      l_old_value := :OLD.TRANSPORT_BY_BARGE_IND;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
      	Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
      				      , l_audit_event_id
      				      , l_table_name
      				      , l_column_name
      				      , l_old_value
      				      , l_new_value
      				      , l_userid
      				      , l_business_id
      				      , l_error_message);
      END IF;

      l_column_name := 'WT_APPLICABLE_VOLUME';
      l_old_value := :OLD.WT_APPLICABLE_VOLUME;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
      	Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
      				      , l_audit_event_id
      				      , l_table_name
      				      , l_column_name
      				      , l_old_value
      				      , l_new_value
      				      , l_userid
      				      , l_business_id
      				      , l_error_message);
      END IF;

      l_column_name := 'STS_APPLICABLE_VOLUME';
      l_old_value := :OLD.STS_APPLICABLE_VOLUME;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
      	Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
      				      , l_audit_event_id
      				      , l_table_name
      				      , l_column_name
      				      , l_old_value
      				      , l_new_value
      				      , l_userid
      				      , l_business_id
      				      , l_error_message);
      END IF;

      l_column_name := 'CAMPS_APPLICABLE_VOLUME';
      l_old_value := :OLD.CAMPS_APPLICABLE_VOLUME;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
      	Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
      				      , l_audit_event_id
      				      , l_table_name
      				      , l_column_name
      				      , l_old_value
      				      , l_new_value
      				      , l_userid
      				      , l_business_id
      				      , l_error_message);
      END IF;

      l_column_name := 'FIBRE_RECOVERY_ZONE_FRACTION';
      l_old_value := :OLD.FIBRE_RECOVERY_ZONE_FRACTION;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
      	Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
      				      , l_audit_event_id
      				      , l_table_name
      				      , l_column_name
      				      , l_old_value
      				      , l_new_value
      				      , l_userid
      				      , l_business_id
      				      , l_error_message);
      END IF;

      l_column_name := 'TCM_OLD_GRWTH_NUMBER_OF_TREES';
      l_old_value := :OLD.TCM_OLD_GRWTH_NUMBER_OF_TREES;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
      	Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
      				      , l_audit_event_id
      				      , l_table_name
      				      , l_column_name
      				      , l_old_value
      				      , l_new_value
      				      , l_userid
      				      , l_business_id
      				      , l_error_message);
      END IF;

      l_column_name := 'TCM_2ND_GRWTH_NUMBER_OF_TREES';
      l_old_value := :OLD.TCM_2ND_GRWTH_NUMBER_OF_TREES;
      l_new_value := NULL;
      IF l_old_value IS NOT NULL THEN
      	Pkg_Ecas_Audit.ADD_AUDIT_DETAIL(l_ecas_id
      				      , l_audit_event_id
      				      , l_table_name
      				      , l_column_name
      				      , l_old_value
      				      , l_new_value
      				      , l_userid
      				      , l_business_id
      				      , l_error_message);
      END IF;

    END IF; --l_audit_event_id is not null
  END IF; --if updating or deleting

  EXCEPTION
    WHEN TOO_MANY_ROWS THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.record.toomany:TRG_APPRAISAL_DATA_SUBMISSION1,'||TRIM(l_table_name)||','||SQLCODE||';';
    WHEN DUP_VAL_ON_INDEX THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.record.duplicate:TRG_APPRAISAL_DATA_SUBMISSION1,'||TRIM(l_table_name)||','||SQLCODE||';';
    WHEN NO_DATA_FOUND THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.record.invalid:TRG_APPRAISAL_DATA_SUBMISSION1,'||TRIM(l_table_name)||';';
    WHEN OTHERS THEN
      l_error_message := l_error_message || 'ecas.web.usr.database.unexpected:TRG_APPRAISAL_DATA_SUBMISSION1,'||SQLCODE||','||SQLERRM||';';
    RAISE;
END TRG_APPRAISAL_DATA_SUBMISSION1;


/
ALTER TRIGGER "THE"."TRG_APPRAISAL_DATA_SUBMISSION1" ENABLE;

  CREATE OR REPLACE EDITIONABLE TRIGGER "THE"."APPRAISED_WORKSHEET_AUD_TGR" 
 BEFORE INSERT OR UPDATE
 ON APPRAISED_WORKSHEET
 FOR EACH ROW
DECLARE
     V_TRANSACTION_ID NUMBER;
BEGIN

    SELECT GAS_TRANSACTION_SEQ.NEXTVAL INTO V_TRANSACTION_ID FROM DUAL;

    INSERT INTO GAS_TRANSACTION (
          GAS_TRANSACTION_ID,
          TRANSACTION_COMMENT,
          ENTRY_TIMESTAMP,
          ENTRY_USERID,
          UPDATE_TIMESTAMP,
          UPDATE_USERID
    ) VALUES (
          V_TRANSACTION_ID,
          NULL,
          :NEW.ENTRY_TIMESTAMP,
          :NEW.ENTRY_USERID,
          :NEW.UPDATE_TIMESTAMP,
          :NEW.UPDATE_USERID
    );

  INSERT INTO APPRAISED_WORKSHEET_AUD (
         APPRAISED_WORKSHEET_AUD_ID,
         APPRAISED_WORKSHEET_ID,
         GAS_TRANSACTION_ID,
         ECAS_ID,
         DISCOUNT_PERCENT,
         CEASE_ADJUSTMENT_DATE,
         SDM_DECLARATION_ACCEPTANCE_DT,
         SILVICULTURE_COST_OVERRIDE,
         LOGGING_COST_OVERRIDE,
         MANUFACTURING_COST_OVERRIDE,
         ENTRY_TIMESTAMP,
         ENTRY_USERID,
         UPDATE_TIMESTAMP,
         UPDATE_USERID
    ) VALUES (
         APPRAISED_WORKSHEET_AUD_SEQ.NEXTVAL,
         :NEW.APPRAISED_WORKSHEET_ID,
         V_TRANSACTION_ID,
         :NEW.ECAS_ID,
         :NEW.DISCOUNT_PERCENT,
         :NEW.CEASE_ADJUSTMENT_DATE,
         :NEW.SDM_DECLARATION_ACCEPTANCE_DT,
         :NEW.SILVICULTURE_COST_OVERRIDE,
         :NEW.LOGGING_COST_OVERRIDE,
         :NEW.MANUFACTURING_COST_OVERRIDE,
         :NEW.ENTRY_TIMESTAMP,
         :NEW.ENTRY_USERID,
         :NEW.UPDATE_TIMESTAMP,
         :NEW.UPDATE_USERID
    );

END;




/
ALTER TRIGGER "THE"."APPRAISED_WORKSHEET_AUD_TGR" ENABLE;

  CREATE OR REPLACE EDITIONABLE TRIGGER "THE"."APRRAISED_STMPG_RATE_AUD_TGR" 
 BEFORE INSERT OR UPDATE
 ON APPRAISED_STUMPAGE_RATE
 FOR EACH ROW
DECLARE
     V_TRANSACTION_ID NUMBER;
BEGIN

    SELECT GAS_TRANSACTION_SEQ.NEXTVAL INTO V_TRANSACTION_ID FROM DUAL;

    INSERT INTO GAS_TRANSACTION (
          GAS_TRANSACTION_ID,
          TRANSACTION_COMMENT,
          ENTRY_TIMESTAMP,
          ENTRY_USERID,
          UPDATE_TIMESTAMP,
          UPDATE_USERID
    ) VALUES (
          V_TRANSACTION_ID,
          NULL,
          :NEW.ENTRY_TIMESTAMP,
          :NEW.ENTRY_USERID,
          :NEW.UPDATE_TIMESTAMP,
          :NEW.UPDATE_USERID
    );

  INSERT INTO APPRAISED_STUMPAGE_RATE_AUD (
         APPRAISED_STUMPAGE_RATE_AUD_ID,
         APPRAISED_STUMPAGE_RATE_ID,
         GAS_TRANSACTION_ID,
         STUMPAGE_RATE_EFFECTIVE_DATE,
         TOTAL_STUMPAGE_RATE_AMOUNT,
         STUMPAGE_RATE_OVERRIDE_IND,
         UPSET_STUMPAGE_RATE_OVERRIDE,
         LICENSEE_NOTICE_DELIVERY_IND,
         ARCHIVE_NOTICE_DELIVERY_IND,
         NOTICE_CREATE_DATE,
         ADJUSTMENT_NOTICE_MESSAGE,
         APPRAISED_WORKSHEET_ID,
         HISTORIC_APPRAISED_WRKSHEET_ID,
         ENTRY_TIMESTAMP,
         ENTRY_USERID,
         UPDATE_TIMESTAMP,
         UPDATE_USERID
    ) VALUES (
         APPRAISED_STUMPAGE_RTE_AUD_SEQ.NEXTVAL,
         :NEW.APPRAISED_STUMPAGE_RATE_ID,
         V_TRANSACTION_ID,
         :NEW.STUMPAGE_RATE_EFFECTIVE_DATE,
         :NEW.TOTAL_STUMPAGE_RATE_AMOUNT,
         :NEW.STUMPAGE_RATE_OVERRIDE_IND,
         :NEW.UPSET_STUMPAGE_RATE_OVERRIDE,
         :NEW.LICENSEE_NOTICE_DELIVERY_IND,
         :NEW.ARCHIVE_NOTICE_DELIVERY_IND,
         :NEW.NOTICE_CREATE_DATE,
         :NEW.ADJUSTMENT_NOTICE_MESSAGE,
         :NEW.APPRAISED_WORKSHEET_ID,
         :NEW.HISTORIC_APPRAISED_WRKSHEET_ID,
         :NEW.ENTRY_TIMESTAMP,
         :NEW.ENTRY_USERID,
         :NEW.UPDATE_TIMESTAMP,
         :NEW.UPDATE_USERID
    );

END;




/
ALTER TRIGGER "THE"."APRRAISED_STMPG_RATE_AUD_TGR" ENABLE;

  CREATE OR REPLACE EDITIONABLE TRIGGER "THE"."CLIENT_CLI_CONTACT_AR_IUD_TRG" 
/******************************************************************************
   Trigger: CLIENT_CLI_CONTACT_AR_IUD_TRG
   Purpose: This trigger audits changes to the CLIENT_CONTACT table
   Revision History
   Person               Date       Comments
   -----------------    ---------  --------------------------------
   R.A.Robb             2006-12-27 Created
   E.Wong               2007-01-24 Added Client Contact Id auditing
******************************************************************************/
AFTER INSERT OR UPDATE OR DELETE
  OF client_contact_id
   , client_number
   , client_locn_code
   , bus_contact_code
   , contact_name
   , business_phone
   , cell_phone
   , fax_number
   , email_address
  ON client_contact
  FOR EACH ROW
DECLARE
  v_client_audit_code                for_cli_audit.client_audit_code%TYPE;
BEGIN
  IF INSERTING THEN
    v_client_audit_code := client_constants.c_audit_insert;
  ELSIF UPDATING THEN
    v_client_audit_code := client_constants.c_audit_update;
  ELSE
    v_client_audit_code := client_constants.c_audit_delete;
  END IF;

  IF    INSERTING
     OR UPDATING THEN
    --Put the new row into the audit table
    INSERT INTO cli_con_audit
           (client_contact_audit_id
          , client_audit_code
          , client_contact_id
          , client_number
          , client_locn_code
          , bus_contact_code
          , contact_name
          , business_phone
          , cell_phone
          , fax_number
          , email_address
          , update_timestamp
          , update_userid
          , update_org_unit_no
          , add_timestamp
          , add_userid
          , add_org_unit)
    VALUES (client_contact_audit_seq.NEXTVAL
          , v_client_audit_code
          , :NEW.client_contact_id
          , :NEW.client_number
          , :NEW.client_locn_code
          , :NEW.bus_contact_code
          , :NEW.contact_name
          , :NEW.business_phone
          , :NEW.cell_phone
          , :NEW.fax_number
          , :NEW.email_address
          , :NEW.update_timestamp
          , :NEW.update_userid
          , :NEW.update_org_unit
          , :NEW.add_timestamp
          , :NEW.add_userid
          , :NEW.add_org_unit);
  ELSE
    --DELETING: Put the last row into the audit table before deleting
    --          replacing update userid/timestamp/org
    -->check PK to make sure we are deleting the record in progress
    IF client_client_contact.get_client_contact_id = :OLD.client_contact_id
    -->check that userid and timestamp are available
    AND client_client_contact.get_update_timestamp IS NOT NULL
    AND client_client_contact.get_update_userid IS NOT NULL
    AND client_client_contact.get_update_org_unit IS NOT NULL THEN
      INSERT INTO cli_con_audit
             (client_contact_audit_id
            , client_audit_code
            , client_contact_id
            , client_number
            , client_locn_code
            , bus_contact_code
            , contact_name
            , business_phone
            , cell_phone
            , fax_number
            , email_address
            , update_timestamp
            , update_userid
            , update_org_unit_no
            , add_timestamp
            , add_userid
            , add_org_unit)
      VALUES (client_contact_audit_seq.NEXTVAL
            , v_client_audit_code
            , :OLD.client_contact_id
            , :OLD.client_number
            , :OLD.client_locn_code
            , :OLD.bus_contact_code
            , :OLD.contact_name
            , :OLD.business_phone
            , :OLD.cell_phone
            , :OLD.fax_number
            , :OLD.email_address
            , client_client_contact.get_update_timestamp
            , client_client_contact.get_update_userid
            , client_client_contact.get_update_org_unit
            , :OLD.add_timestamp
            , :OLD.add_userid
            , :OLD.add_org_unit);
    ELSE
      RAISE_APPLICATION_ERROR(-20500,'Data consistency error in auditing deletion of CLIENT_CONTACT');
    END IF;
  END IF;
END client_cli_contact_ar_iud_trg;




/
ALTER TRIGGER "THE"."CLIENT_CLI_CONTACT_AR_IUD_TRG" ENABLE;

  CREATE OR REPLACE EDITIONABLE TRIGGER "THE"."TB1$_INVC_NOTATION" 
   BEFORE INSERT OR UPDATE ON THE.INVC_NOTATION
   FOR EACH ROW
BEGIN
  :NEW.INVOICE_NUMBER :=     NVL ( RTRIM ( :NEW.INVOICE_NUMBER ), ' ' ) ;
  :NEW.CANCELLATION_IND :=   NVL ( RTRIM ( :NEW.CANCELLATION_IND ), ' ' ) ;
  :NEW.NOTATION_TYPE_CD :=   NVL ( RTRIM ( :NEW.NOTATION_TYPE_CD ), ' ' ) ;
  :NEW.ENTRY_USERID :=       NVL ( RTRIM ( :NEW.ENTRY_USERID ), ' ' ) ;
  :NEW.UPDATE_USERID :=      NVL ( RTRIM ( :NEW.UPDATE_USERID ), ' ' ) ;
  :NEW.NOTATION_TEXT :=      NVL ( RTRIM ( :NEW.NOTATION_TEXT ), ' ' ) ;
END ;




/
ALTER TRIGGER "THE"."TB1$_INVC_NOTATION" ENABLE;

  CREATE OR REPLACE EDITIONABLE TRIGGER "THE"."TB1$_ACK_MASK" 
   BEFORE INSERT OR UPDATE ON THE.ACK_MASK
   FOR EACH ROW
BEGIN
   :NEW.ACK_MASK_ACODE :=     NVL ( RTRIM ( :NEW.ACK_MASK_ACODE ), ' ' ) ;
   :NEW.DESCRIPTION :=        NVL ( RTRIM ( :NEW.DESCRIPTION ), ' ' ) ;
   :NEW.DFLT_OCG_MNSTRY_ID := NVL ( RTRIM ( :NEW.DFLT_OCG_MNSTRY_ID ), ' ' ) ;
   :NEW.RESPONSE_CENTRE_CD := NVL ( RTRIM ( :NEW.RESPONSE_CENTRE_CD ), ' ' ) ;
   :NEW.RCC_VALIDATION_TBL := NVL ( RTRIM ( :NEW.RCC_VALIDATION_TBL ), ' ' ) ;
   :NEW.DEFAULT_ACCT_NUM :=   NVL ( RTRIM ( :NEW.DEFAULT_ACCT_NUM ), ' ' ) ;
   :NEW.DEFAULT_STOB :=       NVL ( RTRIM ( :NEW.DEFAULT_STOB ), ' ' ) ;
   :NEW.GL_PROJ_VLDTN_CD :=   NVL ( RTRIM ( :NEW.GL_PROJ_VLDTN_CD ), ' ' ) ;
   :NEW.GL_SPLR_VLDTN_CD :=   NVL ( RTRIM ( :NEW.GL_SPLR_VLDTN_CD ), ' ' ) ;
   :NEW.SUMMARIZE_IND :=      NVL ( RTRIM ( :NEW.SUMMARIZE_IND ), ' ' ) ;
   :NEW.UPDATE_USERID :=      NVL ( RTRIM ( :NEW.UPDATE_USERID ), ' ' ) ;
END ;





/
ALTER TRIGGER "THE"."TB1$_ACK_MASK" ENABLE;

  CREATE OR REPLACE EDITIONABLE TRIGGER "THE"."TB1$_INVOICE_DTL_TXN" 
   BEFORE INSERT OR UPDATE ON THE.INVOICE_DTL_TXN
   FOR EACH ROW
BEGIN
  :NEW.INVOICE_NUMBER     := NVL(RTRIM(:NEW.INVOICE_NUMBER), ' ');
  :NEW.UNIT_OF_MEASURE_CD := NVL(RTRIM(:NEW.UNIT_OF_MEASURE_CD), ' ');
  :NEW.OCG_SUPPLIER_NMBR  := NVL(RTRIM(:NEW.OCG_SUPPLIER_NMBR), ' ');
  :NEW.ACK_MASK_ACODE     := NVL(RTRIM(:NEW.ACK_MASK_ACODE), ' ');
  :NEW.UPDATE_USERID      := NVL(RTRIM(:NEW.UPDATE_USERID), ' ');
  :NEW.ENTRY_USERID       := NVL(RTRIM(:NEW.ENTRY_USERID), ' ');
  :NEW.LINE_ITEM_DESCR    := NVL(RTRIM(:NEW.LINE_ITEM_DESCR), ' ');
END;




/
ALTER TRIGGER "THE"."TB1$_INVOICE_DTL_TXN" ENABLE;

  CREATE OR REPLACE EDITIONABLE TRIGGER "THE"."TB1$_NOTATION_TXN" 
     BEFORE INSERT OR UPDATE ON THE.NOTATION_TXN
     FOR EACH ROW
BEGIN
   :NEW.INVOICE_NUMBER :=       NVL ( RTRIM ( :NEW.INVOICE_NUMBER ), ' ' ) ;
   :NEW.ENTRY_USERID :=         NVL ( RTRIM ( :NEW.ENTRY_USERID ), ' ' ) ;
   :NEW.UPDATE_USERID :=        NVL ( RTRIM ( :NEW.UPDATE_USERID ), ' ' ) ;
   :NEW.NOTATION_TEXT :=        NVL ( RTRIM ( :NEW.NOTATION_TEXT ), ' ' ) ;
 END ;




/
ALTER TRIGGER "THE"."TB1$_NOTATION_TXN" ENABLE;

  CREATE OR REPLACE EDITIONABLE TRIGGER "THE"."TB1$_GEN_INVOICE_DTL" 
        BEFORE INSERT OR UPDATE ON THE.GEN_INVOICE_DTL
        FOR EACH ROW
BEGIN
  :NEW.INVOICE_NUMBER :=        NVL ( RTRIM ( :NEW.INVOICE_NUMBER ), ' ' ) ;
  :NEW.CANCELLATION_IND :=      NVL ( RTRIM ( :NEW.CANCELLATION_IND ), ' ' ) ;
  :NEW.REVENUE_CLASSN_CD :=     NVL ( RTRIM ( :NEW.REVENUE_CLASSN_CD ), ' ' ) ;
--  :NEW.RESPONSE_CENTRE_CD :=    NVL ( RTRIM ( :NEW.RESPONSE_CENTRE_CD ), ' ' ) ;
  :NEW.UNIT_OF_MEASURE_CD :=    NVL ( RTRIM ( :NEW.UNIT_OF_MEASURE_CD ), ' ' ) ;
  :NEW.OCG_SUPPLIER_NMBR :=     NVL ( RTRIM ( :NEW.OCG_SUPPLIER_NMBR ), ' ' ) ;
  :NEW.LINE_ITEM_DESCR :=       NVL ( RTRIM ( :NEW.LINE_ITEM_DESCR ), ' ' ) ;
  :NEW.UPDATE_USERID :=         NVL ( RTRIM ( SUBSTR(:NEW.UPDATE_USERID,0,8) ), ' ' ) ;
  :NEW.ENTRY_USERID :=          NVL ( RTRIM ( SUBSTR(:NEW.ENTRY_USERID,0,8) ), ' ' ) ;
 END ;




/
ALTER TRIGGER "THE"."TB1$_GEN_INVOICE_DTL" ENABLE;

  CREATE OR REPLACE EDITIONABLE TRIGGER "THE"."FTA_SYNC_PMC_TM" 
/******************************************************************************
   TRIGGER:  FTA_SYNC_PMC_TM
   PURPOSE: THIS TRIGGER WILL UPDATE/DELETE TIMBER_MARK TO KEEP IN SYNC WITH
            PRIVATE_MARK_CERTIFICATE/HAULING_AUTHORITY
      WILL BE DROPPED WHEN TIMBER_MARK IS NO LONGER REQUIRED
   REVISION HISTORY
   PERSON               DATE             COMMENTS
   -----------------    ---------      --------------------------------
   M.DELLAVIOLA         2005-01-02       CREATED
******************************************************************************/
AFTER INSERT OR UPDATE OR DELETE
   ON private_mark_certificate
   FOR EACH ROW
DECLARE
   v_admin_zone   prov_forest_use.district_admin_zone%TYPE;
   v_marking      hauling_authority.marking_method_code%TYPE;
   v_instrument   hauling_authority.marking_instrument_code%TYPE;

   CURSOR curadminzone
   IS
      SELECT marking_method_code
           , marking_instrument_code
           , district_admin_zone
        FROM hauling_authority a
           , prov_forest_use b
       WHERE a.timber_mark = b.forest_file_id
         AND a.forest_file_id = :NEW.timber_mark;
BEGIN
-- get additional fields from prov_forest_use and hauling_authority
   OPEN curadminzone;

   FETCH curadminzone
    INTO v_marking
       , v_instrument
       , v_admin_zone;

   CLOSE curadminzone;

   IF :NEW.forest_file_id IS NOT NULL
   THEN
      IF :OLD.forest_file_id IS NULL
      THEN
         -- create TIMBER_MARK when the forest file id is being created only
         INSERT INTO timber_mark
           (timber_mark
          , CERTIFICATE
          , forest_file_id
          , cutting_permit_id
          , forest_district
          , geographic_distrct
          , district_admn_zone
          , mark_appl_date
          , mark_amend_date
          , cascade_split_code
          , quota_type_code
          , crown_granted_acq_desc
          , granted_acqrd_date
          , crown_granted_ind
          , activated_userid
          , amended_userid
          , tenure_term
          , mark_status_st
          , mark_status_date
          , mark_extend_date
          , mark_extend_rsn_cd
          , mark_extend_count
          , mark_issue_date
          , mark_expiry_date
          , marking_method_cd
          , markng_instrmnt_cd
          , entry_userid
          , entry_timestamp
          , update_userid
          , update_timestamp
          , revision_count
           )
         VALUES (NVL (:NEW.timber_mark, :NEW.CERTIFICATE)
           , :NEW.CERTIFICATE
           , :NEW.forest_file_id
           , ' '
           , :NEW.forest_district
           , :NEW.forest_district
           , v_admin_zone
           , :NEW.private_mark_application_date
           , :NEW.private_mark_amend_date
           , :NEW.cascade_split_code
           , :NEW.quota_type_code
           , :NEW.crown_granted_acq_desc
           , :NEW.granted_acqrd_date
           , :NEW.crown_granted_ind
           , :NEW.private_mark_activated_userid
           , :NEW.private_mark_amended_userid
           , :NEW.private_mark_tenure_term
           , :NEW.private_mark_status_code
           , :NEW.private_mark_status_date
           , :NEW.private_mark_extend_date
           , :NEW.private_mark_extend_reas_code
           , NVL (:NEW.private_mark_extend_count, 0)
           , :NEW.private_mark_issue_date
           , :NEW.private_mark_expiry_date
           , v_marking
           , v_instrument
           , :NEW.entry_userid
           , :NEW.entry_timestamp
           , :NEW.update_userid
           , :NEW.update_timestamp
           , :NEW.revision_count
            );
      ELSIF :OLD.forest_file_id IS NOT NULL
      THEN
         -- UPDATE TIMBER_MARK
         UPDATE timber_mark
            SET forest_district = :NEW.forest_district
              , geographic_distrct = :NEW.forest_district
              , mark_appl_date = :NEW.private_mark_application_date
              , mark_amend_date = :NEW.private_mark_amend_date
              , cascade_split_code = :NEW.cascade_split_code
              , quota_type_code = :NEW.quota_type_code
              , crown_granted_acq_desc = :NEW.crown_granted_acq_desc
              , granted_acqrd_date = :NEW.granted_acqrd_date
              , crown_granted_ind = :NEW.crown_granted_ind
              , activated_userid = :NEW.private_mark_activated_userid
              , amended_userid = :NEW.private_mark_amended_userid
              , tenure_term = :NEW.private_mark_tenure_term
              , mark_status_st = :NEW.private_mark_status_code
              , mark_status_date = :NEW.private_mark_status_date
              , mark_extend_date = :NEW.private_mark_extend_date
              , mark_extend_rsn_cd = :NEW.private_mark_extend_reas_code
              , mark_extend_count = :NEW.private_mark_extend_count
              , mark_issue_date = :NEW.private_mark_issue_date
              , MARK_CANCEL_DATE = :NEW.PRIVATE_MARK_CANCEL_DATE
              , mark_expiry_date = :NEW.private_mark_expiry_date
              , entry_userid = :NEW.entry_userid
              , entry_timestamp = :NEW.entry_timestamp
              , update_userid = :NEW.update_userid
              , update_timestamp = :NEW.update_timestamp
              , district_admn_zone = v_admin_zone
              , marking_method_cd = v_marking
              , markng_instrmnt_cd = v_instrument
              , revision_count = :NEW.revision_count
          WHERE timber_mark = :NEW.timber_mark;
      ELSIF DELETING
      THEN
         -- Assumes that delete process has already deleted child records
         -- The only time this should fails is due to other RI from non-FTA systems (eg. CIMS,HBS)
         DELETE FROM timber_mark
               WHERE timber_mark = :OLD.timber_mark;
      END IF;
   END IF;
END fta_sync_pmc_tm;



/
ALTER TRIGGER "THE"."FTA_SYNC_PMC_TM" ENABLE;

  CREATE OR REPLACE EDITIONABLE TRIGGER "THE"."NON_APPRAISED_WRKSHT_AUD_TRG" 
 BEFORE INSERT OR UPDATE
 ON NON_APPRAISED_WORKSHEET
 FOR EACH ROW
DECLARE
     V_TRANSACTION_ID NUMBER;
BEGIN

    SELECT GAS_TRANSACTION_SEQ.NEXTVAL INTO V_TRANSACTION_ID FROM DUAL;

    INSERT INTO GAS_TRANSACTION (
          GAS_TRANSACTION_ID,
          TRANSACTION_COMMENT,
          ENTRY_TIMESTAMP,
          ENTRY_USERID,
          UPDATE_TIMESTAMP,
          UPDATE_USERID
    ) VALUES (
          V_TRANSACTION_ID,
          NULL,
          :NEW.ENTRY_TIMESTAMP,
          :NEW.ENTRY_USERID,
          :NEW.UPDATE_TIMESTAMP,
          :NEW.UPDATE_USERID
    );

  INSERT INTO NON_APPRAISED_WORKSHEET_AUD (
         NON_APPRAISED_WORKSHEET_AUD_ID,
         NON_APPRAISED_WORKSHEET_ID,
         GAS_TRANSACTION_ID,
         TIMBER_MARK,
         EFFECTIVE_DATE,
         EXPIRY_DATE,
         APPRAISAL_METHOD_CODE,
         NON_APPRAISED_STATUS_CODE,
         WORKSHEET_REFERENCE_TYPE_CODE,
         SDM_DECLARATION_ACCEPTANCE_DT,
         TSB_NUMBER_CODE,
         APPRAISAL_FOREST_ZONE_CODE,
         NON_APPRAISED_RATE_TYPE_CODE,
         RATE_ADJUSTMENT_TYPE_CODE,
         ENTRY_TIMESTAMP,
         ENTRY_USERID,
         UPDATE_TIMESTAMP,
         UPDATE_USERID
    ) VALUES (
         NON_APPRAISD_WORKSHEET_AUD_SEQ.NEXTVAL,
         :NEW.NON_APPRAISED_WORKSHEET_ID,
         V_TRANSACTION_ID,
         :NEW.TIMBER_MARK,
         :NEW.EFFECTIVE_DATE,
         :NEW.EXPIRY_DATE,
         :NEW.APPRAISAL_METHOD_CODE,
         :NEW.NON_APPRAISED_STATUS_CODE,
         :NEW.WORKSHEET_REFERENCE_TYPE_CODE,
         :NEW.SDM_DECLARATION_ACCEPTANCE_DT,
         :NEW.TSB_NUMBER_CODE,
         :NEW.APPRAISAL_FOREST_ZONE_CODE,
         :NEW.NON_APPRAISED_RATE_TYPE_CODE,
         :NEW.RATE_ADJUSTMENT_TYPE_CODE,
         :NEW.ENTRY_TIMESTAMP,
         :NEW.ENTRY_USERID,
         :NEW.UPDATE_TIMESTAMP,
         :NEW.UPDATE_USERID
    );

END;


/
ALTER TRIGGER "THE"."NON_APPRAISED_WRKSHT_AUD_TRG" ENABLE;
