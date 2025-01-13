CREATE TABLE "THE"."SEEDLOT_AUDIT"
   (	"SEEDLOT_AUDIT_ID" NUMBER(10,0) NOT NULL ENABLE,
	"AUDIT_DATE" DATE NOT NULL ENABLE,
	"SPAR_AUDIT_CODE" VARCHAR2(1) NOT NULL ENABLE,
	"SEEDLOT_NUMBER" VARCHAR2(5) NOT NULL ENABLE,
	"SEEDLOT_STATUS_CODE" VARCHAR2(3) NOT NULL ENABLE,
	"VEGETATION_CODE" VARCHAR2(8),
	"GENETIC_CLASS_CODE" VARCHAR2(1),
	"COLLECTION_SOURCE_CODE" VARCHAR2(2),
	"SUPERIOR_PRVNC_IND" VARCHAR2(1),
	"ORG_UNIT_NO" NUMBER(10,0),
	"REGISTERED_SEED_IND" VARCHAR2(1),
	"TO_BE_REGISTRD_IND" VARCHAR2(1),
	"REGISTERED_DATE" DATE,
	"FS721A_SIGNED_IND" VARCHAR2(1),
	"BC_SOURCE_IND" VARCHAR2(1),
	"NAD_DATUM_CODE" VARCHAR2(2),
	"UTM_ZONE" NUMBER(5,0),
	"UTM_EASTING" NUMBER(10,0),
	"UTM_NORTHING" NUMBER(10,0),
	"LONGITUDE_DEGREES" NUMBER(3,0),
	"LONGITUDE_MINUTES" NUMBER(2,0),
	"LONGITUDE_SECONDS" NUMBER(2,0),
	"LONGITUDE_DEG_MIN" NUMBER(3,0),
	"LONGITUDE_MIN_MIN" NUMBER(2,0),
	"LONGITUDE_SEC_MIN" NUMBER(2,0),
	"LONGITUDE_DEG_MAX" NUMBER(3,0),
	"LONGITUDE_MIN_MAX" NUMBER(2,0),
	"LONGITUDE_SEC_MAX" NUMBER(2,0),
	"LATITUDE_DEGREES" NUMBER(2,0),
	"LATITUDE_MINUTES" NUMBER(2,0),
	"LATITUDE_SECONDS" NUMBER(2,0),
	"LATITUDE_DEG_MIN" NUMBER(2,0),
	"LATITUDE_MIN_MIN" NUMBER(2,0),
	"LATITUDE_SEC_MIN" NUMBER(2,0),
	"LATITUDE_DEG_MAX" NUMBER(2,0),
	"LATITUDE_MIN_MAX" NUMBER(2,0),
	"LATITUDE_SEC_MAX" NUMBER(2,0),
	"SEED_COAST_AREA_CODE" VARCHAR2(3),
	"ELEVATION" NUMBER(5,0),
	"ELEVATION_MIN" NUMBER(5,0),
	"ELEVATION_MAX" NUMBER(5,0),
	"SEED_PLAN_UNIT_ID" NUMBER(10,0),
	"ORCHARD_ID" VARCHAR2(3),
	"SECONDARY_ORCHARD_ID" VARCHAR2(3),
	"COLLECTION_LOCN_DESC" VARCHAR2(30),
	"COLLECTION_CLI_NUMBER" VARCHAR2(8),
	"COLLECTION_CLI_LOCN_CD" VARCHAR2(2),
	"COLLECTION_START_DATE" DATE,
	"COLLECTION_END_DATE" DATE,
	"CONE_COLLECTION_METHOD_CODE" VARCHAR2(3),
	"CONE_COLLECTION_METHOD2_CODE" VARCHAR2(3),
	"COLLECTION_LAT_DEG" NUMBER(2,0),
	"COLLECTION_LAT_MIN" NUMBER(2,0),
	"COLLECTION_LAT_SEC" NUMBER(2,0),
	"COLLECTION_LATITUDE_CODE" VARCHAR2(2),
	"COLLECTION_LONG_DEG" NUMBER(3,0),
	"COLLECTION_LONG_MIN" NUMBER(2,0),
	"COLLECTION_LONG_SEC" NUMBER(2,0),
	"COLLECTION_LONGITUDE_CODE" VARCHAR2(2),
	"COLLECTION_ELEVATION" NUMBER(5,0),
	"COLLECTION_ELEVATION_MIN" NUMBER(5,0),
	"COLLECTION_ELEVATION_MAX" NUMBER(5,0),
	"COLLECTION_AREA_RADIUS" NUMBER(5,1),
	"COLLECTION_SEED_PLAN_ZONE_IND" VARCHAR2(1),
	"COLLECTION_BGC_IND" VARCHAR2(1),
	"NO_OF_CONTAINERS" NUMBER(6,2),
	"CLCTN_VOLUME" NUMBER(6,2),
	"VOL_PER_CONTAINER" NUMBER(6,2),
	"NMBR_TREES_FROM_CODE" VARCHAR2(1),
	"EFFECTIVE_POP_SIZE" NUMBER(5,1),
	"ORIGINAL_SEED_QTY" NUMBER(10,0),
	"INTERM_STRG_CLIENT_NUMBER" VARCHAR2(8),
	"INTERM_STRG_CLIENT_LOCN" VARCHAR2(2),
	"INTERM_STRG_ST_DATE" DATE,
	"INTERM_STRG_END_DATE" DATE,
	"INTERM_FACILITY_CODE" VARCHAR2(3),
	"INTERM_STRG_LOCN" VARCHAR2(55),
	"INTERM_STRG_CMT" VARCHAR2(125),
	"EXTRACTION_ST_DATE" DATE,
	"EXTRACTION_END_DATE" DATE,
	"EXTRACTION_VOLUME" NUMBER(6,2),
	"EXTRCT_CLI_NUMBER" VARCHAR2(8),
	"EXTRCT_CLI_LOCN_CD" VARCHAR2(2),
	"EXTRACTION_COMMENT" VARCHAR2(125),
	"STORED_CLI_NUMBER" VARCHAR2(8),
	"STORED_CLI_LOCN_CD" VARCHAR2(2),
	"LNGTERM_STRG_ST_DATE" DATE,
	"HISTORICAL_TSR_DATE" DATE,
	"OWNERSHIP_COMMENT" VARCHAR2(4000),
	"CONE_SEED_DESC" VARCHAR2(250),
	"SEEDLOT_COMMENT" VARCHAR2(2000),
	"TEMPORARY_STORAGE_START_DATE" DATE,
	"TEMPORARY_STORAGE_END_DATE" DATE,
	"COLLECTION_STANDARD_MET_IND" VARCHAR2(1),
	"APPLICANT_EMAIL_ADDRESS" VARCHAR2(100),
	"BIOTECH_PROCESSES_IND" VARCHAR2(1),
	"POLLEN_CONTAMINATION_IND" VARCHAR2(1),
	"POLLEN_CONTAMINATION_PCT" NUMBER(3,0),
	"CONTROLLED_CROSS_IND" VARCHAR2(1),
	"ORCHARD_COMMENT" VARCHAR2(2000),
	"TOTAL_PARENT_TREES" NUMBER(5,0),
	"SMP_PARENTS_OUTSIDE" NUMBER(5,0),
	"SMP_MEAN_BV_GROWTH" NUMBER(4,1),
	"SMP_SUCCESS_PCT" NUMBER(3,0),
	"CONTAMINANT_POLLEN_BV" NUMBER(4,1),
	"ORCHARD_CONTAMINATION_PCT" NUMBER(3,0),
	"COANCESTRY" NUMBER(20,10),
	"PROVENANCE_ID" NUMBER(5,0),
	"SEED_PLAN_ZONE_CODE" VARCHAR2(3),
	"POLLEN_CONTAMINATION_MTHD_CODE" VARCHAR2(3),
	"APPLICANT_CLIENT_NUMBER" VARCHAR2(8),
	"APPLICANT_CLIENT_LOCN" VARCHAR2(2),
	"SEED_STORE_CLIENT_NUMBER" VARCHAR2(8),
	"SEED_STORE_CLIENT_LOCN" VARCHAR2(2),
	"SEEDLOT_SOURCE_CODE" VARCHAR2(3),
	"FEMALE_GAMETIC_MTHD_CODE" VARCHAR2(3),
	"MALE_GAMETIC_MTHD_CODE" VARCHAR2(3),
	"BGC_ZONE_CODE" VARCHAR2(4),
	"BGC_SUBZONE_CODE" VARCHAR2(3),
	"VARIANT" VARCHAR2(1),
	"BEC_VERSION_ID" NUMBER(10,0),
	"PRICE_PER_KG" NUMBER(7,2),
	"PRICE_COMMENT" VARCHAR2(2000),
	"APPROVED_USERID" VARCHAR2(30),
	"APPROVED_TIMESTAMP" DATE,
	"DECLARED_USERID" VARCHAR2(30),
	"DECLARED_TIMESTAMP" DATE,
	"ENTRY_USERID" VARCHAR2(30) NOT NULL ENABLE,
	"ENTRY_TIMESTAMP" DATE NOT NULL ENABLE,
	"UPDATE_USERID" VARCHAR2(30) NOT NULL ENABLE,
	"UPDATE_TIMESTAMP" DATE NOT NULL ENABLE,
	"REVISION_COUNT" NUMBER(5,0) NOT NULL ENABLE,
	 CONSTRAINT "SAUD_PK" PRIMARY KEY ("SEEDLOT_AUDIT_ID")
  USING INDEX  ENABLE,
	 CONSTRAINT "SAUD_CLC1_FK" FOREIGN KEY ("COLLECTION_LONGITUDE_CODE")
	  REFERENCES "THE"."COLLECTION_LONGITUDE_CODE" ("COLLECTION_LONGITUDE_CODE") ENABLE,
	 CONSTRAINT "SAUD_GMC1_FK" FOREIGN KEY ("FEMALE_GAMETIC_MTHD_CODE")
	  REFERENCES "THE"."GAMETIC_MTHD_CODE" ("GAMETIC_MTHD_CODE") ENABLE,
	 CONSTRAINT "SAUD_SUPR_FK" FOREIGN KEY ("PROVENANCE_ID")
	  REFERENCES "THE"."SUPERIOR_PROVENANCE" ("PROVENANCE_ID") ENABLE,
	 CONSTRAINT "SAUD_NDC_FK" FOREIGN KEY ("NAD_DATUM_CODE")
	  REFERENCES "THE"."NAD_DATUM_CODE" ("NAD_DATUM_CODE") ENABLE,
	 CONSTRAINT "SAUD_CSC2_FK" FOREIGN KEY ("COLLECTION_SOURCE_CODE")
	  REFERENCES "THE"."COLLECTION_SOURCE_CODE" ("COLLECTION_SOURCE_CODE") ENABLE,
	 CONSTRAINT "SAUD_VC_FK" FOREIGN KEY ("VEGETATION_CODE")
	  REFERENCES "THE"."VEGETATION_CODE" ("VEGETATION_CODE") ENABLE,
	 CONSTRAINT "SAUD_CLC_FK" FOREIGN KEY ("COLLECTION_LATITUDE_CODE")
	  REFERENCES "THE"."COLLECTION_LATITUDE_CODE" ("COLLECTION_LATITUDE_CODE") ENABLE,
	 CONSTRAINT "SAUD_SAUDC_FK" FOREIGN KEY ("SPAR_AUDIT_CODE")
	  REFERENCES "THE"."SPAR_AUDIT_CODE" ("SPAR_AUDIT_CODE") ENABLE,
	 CONSTRAINT "SAUD_GMC2_FK" FOREIGN KEY ("MALE_GAMETIC_MTHD_CODE")
	  REFERENCES "THE"."GAMETIC_MTHD_CODE" ("GAMETIC_MTHD_CODE") ENABLE,
	 CONSTRAINT "SAUD_SSCD_FK" FOREIGN KEY ("SEEDLOT_SOURCE_CODE")
	  REFERENCES "THE"."SEEDLOT_SOURCE_CODE" ("SEEDLOT_SOURCE_CODE") ENABLE,
	 CONSTRAINT "SAUD_CCMC_FK" FOREIGN KEY ("CONE_COLLECTION_METHOD_CODE")
	  REFERENCES "THE"."CONE_COLLECTION_METHOD_CODE" ("CONE_COLLECTION_METHOD_CODE") ENABLE,
	 CONSTRAINT "SAUD_NTFC_FK" FOREIGN KEY ("NMBR_TREES_FROM_CODE")
	  REFERENCES "THE"."NMBR_TREES_FROM_CODE" ("NMBR_TREES_FROM_CODE") ENABLE,
	 CONSTRAINT "SAUD_SSC3_FK" FOREIGN KEY ("SEEDLOT_STATUS_CODE")
	  REFERENCES "THE"."SEEDLOT_STATUS_CODE" ("SEEDLOT_STATUS_CODE") ENABLE,
	 CONSTRAINT "SAUD_GCC_FK" FOREIGN KEY ("GENETIC_CLASS_CODE")
	  REFERENCES "THE"."GENETIC_CLASS_CODE" ("GENETIC_CLASS_CODE") ENABLE,
	 CONSTRAINT "SAUD_SCAC_FK" FOREIGN KEY ("SEED_COAST_AREA_CODE")
	  REFERENCES "THE"."SEED_COAST_AREA_CODE" ("SEED_COAST_AREA_CODE") ENABLE,
	 CONSTRAINT "SAUD_IFC_FK" FOREIGN KEY ("INTERM_FACILITY_CODE")
	  REFERENCES "THE"."INTERM_FACILITY_CODE" ("INTERM_FACILITY_CODE") ENABLE,
	 CONSTRAINT "SAUD_BZC_FK" FOREIGN KEY ("BGC_ZONE_CODE")
	  REFERENCES "THE"."BEC_ZONE_CODE" ("BEC_ZONE_CODE") ENABLE,
	 CONSTRAINT "SAUD_PCMC_FK" FOREIGN KEY ("POLLEN_CONTAMINATION_MTHD_CODE")
	  REFERENCES "THE"."POLLEN_CONTAMINATION_MTHD_CODE" ("POLLEN_CONTAMINATION_MTHD_CODE") ENABLE,
	 CONSTRAINT "SAUD_SPZC_FK" FOREIGN KEY ("SEED_PLAN_ZONE_CODE")
	  REFERENCES "THE"."SEED_PLAN_ZONE_CODE" ("SEED_PLAN_ZONE_CODE") ENABLE,
	 CONSTRAINT "SAUD_CCMC2_FK" FOREIGN KEY ("CONE_COLLECTION_METHOD2_CODE")
	  REFERENCES "THE"."CONE_COLLECTION_METHOD_CODE" ("CONE_COLLECTION_METHOD_CODE") ENABLE
   ) ;

   CREATE SEQUENCE  "THE"."SAUD_SEQ"  MINVALUE 1 MAXVALUE 999999999999999999999999999 INCREMENT BY 1 START WITH 314438 NOCACHE  NOORDER  NOCYCLE  NOKEEP  NOSCALE  GLOBAL ;

  CREATE OR REPLACE EDITIONABLE TRIGGER "THE"."SPR_SEEDLOT_AR_IUD_TRG"
/******************************************************************************
   Trigger: SPR_SEEDLOT_AR_IUD_TRG
   Purpose: This trigger audits changes to the SEEDLOT table

   Revision History
   Person               Date       Comments
   -----------------    ---------  --------------------------------
   R.A.Robb             2005-01-20 Created for PT#25601
   R.A.Robb             2005-03-14 PT#26063 - added COLLECTION_LAT_SEC, COLLECTION_LONG_SEC,
                                   LATITUDE_SECONDS, LONGITUDE_SECONDS
   R.A.Robb             2006-01-12 PT#28861 - added COANCESTRY
   E.Wong               2008-07-16 PT#41508-added BEC_VERSION_ID.
******************************************************************************/
AFTER INSERT OR UPDATE OR DELETE ON seedlot
FOR EACH ROW
DECLARE
  v_spar_audit_code    seedlot_audit.spar_audit_code%TYPE;
BEGIN
  IF INSERTING THEN
    v_spar_audit_code := 'I';
  ELSIF UPDATING THEN
    v_spar_audit_code := 'U';
  ELSE
    v_spar_audit_code := 'D';
  END IF;

  IF INSERTING OR UPDATING THEN
    --Put the new row into the audit table
    INSERT INTO seedlot_audit (
            seedlot_audit_id
          , audit_date
          , spar_audit_code
          , seedlot_number
          , seedlot_status_code
          , vegetation_code
          , genetic_class_code
          , collection_source_code
          , superior_prvnc_ind
          , org_unit_no
          , registered_seed_ind
          , to_be_registrd_ind
          , registered_date
          , fs721a_signed_ind
          , nad_datum_code
          , utm_zone
          , utm_easting
          , utm_northing
          , longitude_degrees
          , longitude_minutes
          , longitude_seconds
          , longitude_deg_min
          , longitude_min_min
          , longitude_deg_max
          , longitude_min_max
          , latitude_degrees
          , latitude_minutes
          , latitude_seconds
          , latitude_deg_min
          , latitude_min_min
          , latitude_deg_max
          , latitude_min_max
          , seed_coast_area_code
          , elevation
          , elevation_min
          , elevation_max
          , orchard_id
          , collection_locn_desc
          , collection_cli_number
          , collection_cli_locn_cd
          , collection_start_date
          , collection_end_date
          , cone_collection_method_code
          , no_of_containers
          , clctn_volume
          , vol_per_container
          , nmbr_trees_from_code
          , effective_pop_size
          , original_seed_qty
          , interm_strg_st_date
          , interm_strg_end_date
          , interm_facility_code
          , extraction_st_date
          , extraction_end_date
          , extraction_volume
          , extrct_cli_number
          , extrct_cli_locn_cd
          , stored_cli_number
          , stored_cli_locn_cd
          , lngterm_strg_st_date
          , historical_tsr_date
          , collection_lat_deg
          , collection_lat_min
          , collection_lat_sec
          , collection_latitude_code
          , collection_long_deg
          , collection_long_min
          , collection_long_sec
          , collection_longitude_code
          , collection_elevation
          , collection_elevation_min
          , collection_elevation_max
          , entry_timestamp
          , entry_userid
          , update_timestamp
          , update_userid
          , approved_timestamp
          , approved_userid
          , revision_count
          , interm_strg_locn
          , interm_strg_cmt
          , ownership_comment
          , cone_seed_desc
          , extraction_comment
          , seedlot_comment
          , bgc_zone_code
          , bgc_subzone_code
          , variant
          , bec_version_id
          , seed_plan_zone_code
          , applicant_client_locn
          , applicant_client_number
          , applicant_email_address
          , bc_source_ind
          , biotech_processes_ind
          , collection_area_radius
          , collection_bgc_ind
          , collection_seed_plan_zone_ind
          , collection_standard_met_ind
          , cone_collection_method2_code
          , contaminant_pollen_bv
          , controlled_cross_ind
          , declared_userid
          , female_gametic_mthd_code
          , latitude_sec_max
          , latitude_sec_min
          , longitude_sec_max
          , longitude_sec_min
          , male_gametic_mthd_code
          , orchard_comment
          , orchard_contamination_pct
          , pollen_contamination_ind
          , pollen_contamination_mthd_code
          , pollen_contamination_pct
          , provenance_id
          , secondary_orchard_id
          , seed_plan_unit_id
          , seed_store_client_locn
          , seed_store_client_number
          , seedlot_source_code
          , smp_mean_bv_growth
          , smp_parents_outside
          , smp_success_pct
          , temporary_storage_end_date
          , temporary_storage_start_date
          , total_parent_trees
          , interm_strg_client_number
          , interm_strg_client_locn
          , declared_timestamp
          , coancestry
          , price_per_kg
          , price_comment
        ) VALUES (
            saud_seq.NEXTVAL
          , SYSDATE
          , v_spar_audit_code
          , :NEW.seedlot_number
          , :NEW.seedlot_status_code
          , :NEW.vegetation_code
          , :NEW.genetic_class_code
          , :NEW.collection_source_code
          , :NEW.superior_prvnc_ind
          , :NEW.org_unit_no
          , :NEW.registered_seed_ind
          , :NEW.to_be_registrd_ind
          , :NEW.registered_date
          , :NEW.fs721a_signed_ind
          , :NEW.nad_datum_code
          , :NEW.utm_zone
          , :NEW.utm_easting
          , :NEW.utm_northing
          , :NEW.longitude_degrees
          , :NEW.longitude_minutes
          , :NEW.longitude_seconds
          , :NEW.longitude_deg_min
          , :NEW.longitude_min_min
          , :NEW.longitude_deg_max
          , :NEW.longitude_min_max
          , :NEW.latitude_degrees
          , :NEW.latitude_minutes
          , :NEW.latitude_seconds
          , :NEW.latitude_deg_min
          , :NEW.latitude_min_min
          , :NEW.latitude_deg_max
          , :NEW.latitude_min_max
          , :NEW.seed_coast_area_code
          , :NEW.elevation
          , :NEW.elevation_min
          , :NEW.elevation_max
          , :NEW.orchard_id
          , :NEW.collection_locn_desc
          , :NEW.collection_cli_number
          , :NEW.collection_cli_locn_cd
          , :NEW.collection_start_date
          , :NEW.collection_end_date
          , :NEW.cone_collection_method_code
          , :NEW.no_of_containers
          , :NEW.clctn_volume
          , :NEW.vol_per_container
          , :NEW.nmbr_trees_from_code
          , :NEW.effective_pop_size
          , :NEW.original_seed_qty
          , :NEW.interm_strg_st_date
          , :NEW.interm_strg_end_date
          , :NEW.interm_facility_code
          , :NEW.extraction_st_date
          , :NEW.extraction_end_date
          , :NEW.extraction_volume
          , :NEW.extrct_cli_number
          , :NEW.extrct_cli_locn_cd
          , :NEW.stored_cli_number
          , :NEW.stored_cli_locn_cd
          , :NEW.lngterm_strg_st_date
          , :NEW.historical_tsr_date
          , :NEW.collection_lat_deg
          , :NEW.collection_lat_min
          , :NEW.collection_lat_sec
          , :NEW.collection_latitude_code
          , :NEW.collection_long_deg
          , :NEW.collection_long_min
          , :NEW.collection_long_sec
          , :NEW.collection_longitude_code
          , :NEW.collection_elevation
          , :NEW.collection_elevation_min
          , :NEW.collection_elevation_max
          , :NEW.entry_timestamp
          , :NEW.entry_userid
          , :NEW.update_timestamp
          , :NEW.update_userid
          , :NEW.approved_timestamp
          , :NEW.approved_userid
          , :NEW.revision_count
          , :NEW.interm_strg_locn
          , :NEW.interm_strg_cmt
          , :NEW.ownership_comment
          , :NEW.cone_seed_desc
          , :NEW.extraction_comment
          , :NEW.seedlot_comment
          , :NEW.bgc_zone_code
          , :NEW.bgc_subzone_code
          , :NEW.variant
          , :NEW.bec_version_id
          , :NEW.seed_plan_zone_code
          , :NEW.applicant_client_locn
          , :NEW.applicant_client_number
          , :NEW.applicant_email_address
          , :NEW.bc_source_ind
          , :NEW.biotech_processes_ind
          , :NEW.collection_area_radius
          , :NEW.collection_bgc_ind
          , :NEW.collection_seed_plan_zone_ind
          , :NEW.collection_standard_met_ind
          , :NEW.cone_collection_method2_code
          , :NEW.contaminant_pollen_bv
          , :NEW.controlled_cross_ind
          , :NEW.declared_userid
          , :NEW.female_gametic_mthd_code
          , :NEW.latitude_sec_max
          , :NEW.latitude_sec_min
          , :NEW.longitude_sec_max
          , :NEW.longitude_sec_min
          , :NEW.male_gametic_mthd_code
          , :NEW.orchard_comment
          , :NEW.orchard_contamination_pct
          , :NEW.pollen_contamination_ind
          , :NEW.pollen_contamination_mthd_code
          , :NEW.pollen_contamination_pct
          , :NEW.provenance_id
          , :NEW.secondary_orchard_id
          , :NEW.seed_plan_unit_id
          , :NEW.seed_store_client_locn
          , :NEW.seed_store_client_number
          , :NEW.seedlot_source_code
          , :NEW.smp_mean_bv_growth
          , :NEW.smp_parents_outside
          , :NEW.smp_success_pct
          , :NEW.temporary_storage_end_date
          , :NEW.temporary_storage_start_date
          , :NEW.total_parent_trees
          , :NEW.interm_strg_client_number
          , :NEW.interm_strg_client_locn
          , :NEW.declared_timestamp
          , :NEW.coancestry
          , :NEW.PRICE_PER_KG
          , :NEW.PRICE_COMMENT
       );
  ELSE
    --DELETING: Put the last row into the audit table before deleting
    INSERT INTO seedlot_audit (
            seedlot_audit_id
          , audit_date
          , spar_audit_code
          , seedlot_number
          , seedlot_status_code
          , vegetation_code
          , genetic_class_code
          , collection_source_code
          , superior_prvnc_ind
          , org_unit_no
          , registered_seed_ind
          , to_be_registrd_ind
          , registered_date
          , fs721a_signed_ind
          , nad_datum_code
          , utm_zone
          , utm_easting
          , utm_northing
          , longitude_degrees
          , longitude_minutes
          , longitude_seconds
          , longitude_deg_min
          , longitude_min_min
          , longitude_deg_max
          , longitude_min_max
          , latitude_degrees
          , latitude_minutes
          , latitude_seconds
          , latitude_deg_min
          , latitude_min_min
          , latitude_deg_max
          , latitude_min_max
          , seed_coast_area_code
          , elevation
          , elevation_min
          , elevation_max
          , orchard_id
          , collection_locn_desc
          , collection_cli_number
          , collection_cli_locn_cd
          , collection_start_date
          , collection_end_date
          , cone_collection_method_code
          , no_of_containers
          , clctn_volume
          , vol_per_container
          , nmbr_trees_from_code
          , effective_pop_size
          , original_seed_qty
          , interm_strg_st_date
          , interm_strg_end_date
          , interm_facility_code
          , extraction_st_date
          , extraction_end_date
          , extraction_volume
          , extrct_cli_number
          , extrct_cli_locn_cd
          , stored_cli_number
          , stored_cli_locn_cd
          , lngterm_strg_st_date
          , historical_tsr_date
          , collection_lat_deg
          , collection_lat_min
          , collection_lat_sec
          , collection_latitude_code
          , collection_long_deg
          , collection_long_min
          , collection_long_sec
          , collection_longitude_code
          , collection_elevation
          , collection_elevation_min
          , collection_elevation_max
          , entry_timestamp
          , entry_userid
          , update_timestamp
          , update_userid
          , approved_timestamp
          , approved_userid
          , revision_count
          , interm_strg_locn
          , interm_strg_cmt
          , ownership_comment
          , cone_seed_desc
          , extraction_comment
          , seedlot_comment
          , bgc_zone_code
          , bgc_subzone_code
          , variant
          , bec_version_id
          , seed_plan_zone_code
          , applicant_client_locn
          , applicant_client_number
          , applicant_email_address
          , bc_source_ind
          , biotech_processes_ind
          , collection_area_radius
          , collection_bgc_ind
          , collection_seed_plan_zone_ind
          , collection_standard_met_ind
          , cone_collection_method2_code
          , contaminant_pollen_bv
          , controlled_cross_ind
          , declared_userid
          , female_gametic_mthd_code
          , latitude_sec_max
          , latitude_sec_min
          , longitude_sec_max
          , longitude_sec_min
          , male_gametic_mthd_code
          , orchard_comment
          , orchard_contamination_pct
          , pollen_contamination_ind
          , pollen_contamination_mthd_code
          , pollen_contamination_pct
          , provenance_id
          , secondary_orchard_id
          , seed_plan_unit_id
          , seed_store_client_locn
          , seed_store_client_number
          , seedlot_source_code
          , smp_mean_bv_growth
          , smp_parents_outside
          , smp_success_pct
          , temporary_storage_end_date
          , temporary_storage_start_date
          , total_parent_trees
          , interm_strg_client_number
          , interm_strg_client_locn
          , declared_timestamp
          , coancestry
          , price_per_kg
          , price_comment
        ) VALUES (
            saud_seq.NEXTVAL
          , SYSDATE
          , v_spar_audit_code
          , :OLD.seedlot_number
          , :OLD.seedlot_status_code
          , :OLD.vegetation_code
          , :OLD.genetic_class_code
          , :OLD.collection_source_code
          , :OLD.superior_prvnc_ind
          , :OLD.org_unit_no
          , :OLD.registered_seed_ind
          , :OLD.to_be_registrd_ind
          , :OLD.registered_date
          , :OLD.fs721a_signed_ind
          , :OLD.nad_datum_code
          , :OLD.utm_zone
          , :OLD.utm_easting
          , :OLD.utm_northing
          , :OLD.longitude_degrees
          , :OLD.longitude_minutes
          , :OLD.longitude_seconds
          , :OLD.longitude_deg_min
          , :OLD.longitude_min_min
          , :OLD.longitude_deg_max
          , :OLD.longitude_min_max
          , :OLD.latitude_degrees
          , :OLD.latitude_minutes
          , :OLD.latitude_seconds
          , :OLD.latitude_deg_min
          , :OLD.latitude_min_min
          , :OLD.latitude_deg_max
          , :OLD.latitude_min_max
          , :OLD.seed_coast_area_code
          , :OLD.elevation
          , :OLD.elevation_min
          , :OLD.elevation_max
          , :OLD.orchard_id
          , :OLD.collection_locn_desc
          , :OLD.collection_cli_number
          , :OLD.collection_cli_locn_cd
          , :OLD.collection_start_date
          , :OLD.collection_end_date
          , :OLD.cone_collection_method_code
          , :OLD.no_of_containers
          , :OLD.clctn_volume
          , :OLD.vol_per_container
          , :OLD.nmbr_trees_from_code
          , :OLD.effective_pop_size
          , :OLD.original_seed_qty
          , :OLD.interm_strg_st_date
          , :OLD.interm_strg_end_date
          , :OLD.interm_facility_code
          , :OLD.extraction_st_date
          , :OLD.extraction_end_date
          , :OLD.extraction_volume
          , :OLD.extrct_cli_number
          , :OLD.extrct_cli_locn_cd
          , :OLD.stored_cli_number
          , :OLD.stored_cli_locn_cd
          , :OLD.lngterm_strg_st_date
          , :OLD.historical_tsr_date
          , :OLD.collection_lat_deg
          , :OLD.collection_lat_min
          , :OLD.collection_lat_sec
          , :OLD.collection_latitude_code
          , :OLD.collection_long_deg
          , :OLD.collection_long_min
          , :OLD.collection_long_sec
          , :OLD.collection_longitude_code
          , :OLD.collection_elevation
          , :OLD.collection_elevation_min
          , :OLD.collection_elevation_max
          , :OLD.entry_timestamp
          , :OLD.entry_userid
          , :OLD.update_timestamp
          , :OLD.update_userid
          , :OLD.approved_timestamp
          , :OLD.approved_userid
          , :OLD.revision_count
          , :OLD.interm_strg_locn
          , :OLD.interm_strg_cmt
          , :OLD.ownership_comment
          , :OLD.cone_seed_desc
          , :OLD.extraction_comment
          , :OLD.seedlot_comment
          , :OLD.bgc_zone_code
          , :OLD.bgc_subzone_code
          , :OLD.variant
          , :OLD.bec_version_id
          , :OLD.seed_plan_zone_code
          , :OLD.applicant_client_locn
          , :OLD.applicant_client_number
          , :OLD.applicant_email_address
          , :OLD.bc_source_ind
          , :OLD.biotech_processes_ind
          , :OLD.collection_area_radius
          , :OLD.collection_bgc_ind
          , :OLD.collection_seed_plan_zone_ind
          , :OLD.collection_standard_met_ind
          , :OLD.cone_collection_method2_code
          , :OLD.contaminant_pollen_bv
          , :OLD.controlled_cross_ind
          , :OLD.declared_userid
          , :OLD.female_gametic_mthd_code
          , :OLD.latitude_sec_max
          , :OLD.latitude_sec_min
          , :OLD.longitude_sec_max
          , :OLD.longitude_sec_min
          , :OLD.male_gametic_mthd_code
          , :OLD.orchard_comment
          , :OLD.orchard_contamination_pct
          , :OLD.pollen_contamination_ind
          , :OLD.pollen_contamination_mthd_code
          , :OLD.pollen_contamination_pct
          , :OLD.provenance_id
          , :OLD.secondary_orchard_id
          , :OLD.seed_plan_unit_id
          , :OLD.seed_store_client_locn
          , :OLD.seed_store_client_number
          , :OLD.seedlot_source_code
          , :OLD.smp_mean_bv_growth
          , :OLD.smp_parents_outside
          , :OLD.smp_success_pct
          , :OLD.temporary_storage_end_date
          , :OLD.temporary_storage_start_date
          , :OLD.total_parent_trees
          , :OLD.interm_strg_client_number
          , :OLD.interm_strg_client_locn
          , :OLD.declared_timestamp
          , :OLD.coancestry
          , :OLD.PRICE_PER_KG
          , :OLD.PRICE_COMMENT
      );
  END IF;

END spr_seedlot_ar_iud_trg;



/
ALTER TRIGGER "THE"."SPR_SEEDLOT_AR_IUD_TRG" ENABLE;

  CREATE TABLE "THE"."SEEDLOT"
   (	"SEEDLOT_NUMBER" VARCHAR2(5) NOT NULL ENABLE,
	"SEEDLOT_STATUS_CODE" VARCHAR2(3) NOT NULL ENABLE,
	"VEGETATION_CODE" VARCHAR2(8),
	"GENETIC_CLASS_CODE" VARCHAR2(1),
	"COLLECTION_SOURCE_CODE" VARCHAR2(2),
	"SUPERIOR_PRVNC_IND" VARCHAR2(1),
	"ORG_UNIT_NO" NUMBER(10,0),
	"REGISTERED_SEED_IND" VARCHAR2(1),
	"TO_BE_REGISTRD_IND" VARCHAR2(1),
	"REGISTERED_DATE" DATE,
	"FS721A_SIGNED_IND" VARCHAR2(1),
	"BC_SOURCE_IND" VARCHAR2(1),
	"NAD_DATUM_CODE" VARCHAR2(2),
	"UTM_ZONE" NUMBER(5,0),
	"UTM_EASTING" NUMBER(10,0),
	"UTM_NORTHING" NUMBER(10,0),
	"LONGITUDE_DEGREES" NUMBER(3,0),
	"LONGITUDE_MINUTES" NUMBER(2,0),
	"LONGITUDE_SECONDS" NUMBER(2,0),
	"LONGITUDE_DEG_MIN" NUMBER(3,0),
	"LONGITUDE_MIN_MIN" NUMBER(2,0),
	"LONGITUDE_SEC_MIN" NUMBER(2,0),
	"LONGITUDE_DEG_MAX" NUMBER(3,0),
	"LONGITUDE_MIN_MAX" NUMBER(2,0),
	"LONGITUDE_SEC_MAX" NUMBER(2,0),
	"LATITUDE_DEGREES" NUMBER(2,0),
	"LATITUDE_MINUTES" NUMBER(2,0),
	"LATITUDE_SECONDS" NUMBER(2,0),
	"LATITUDE_DEG_MIN" NUMBER(2,0),
	"LATITUDE_MIN_MIN" NUMBER(2,0),
	"LATITUDE_SEC_MIN" NUMBER(2,0),
	"LATITUDE_DEG_MAX" NUMBER(2,0),
	"LATITUDE_MIN_MAX" NUMBER(2,0),
	"LATITUDE_SEC_MAX" NUMBER(2,0),
	"SEED_COAST_AREA_CODE" VARCHAR2(3),
	"ELEVATION" NUMBER(5,0),
	"ELEVATION_MIN" NUMBER(5,0),
	"ELEVATION_MAX" NUMBER(5,0),
	"SEED_PLAN_UNIT_ID" NUMBER(10,0),
	"ORCHARD_ID" VARCHAR2(3),
	"SECONDARY_ORCHARD_ID" VARCHAR2(3),
	"COLLECTION_LOCN_DESC" VARCHAR2(30),
	"COLLECTION_CLI_NUMBER" VARCHAR2(8),
	"COLLECTION_CLI_LOCN_CD" VARCHAR2(2),
	"COLLECTION_START_DATE" DATE,
	"COLLECTION_END_DATE" DATE,
	"CONE_COLLECTION_METHOD_CODE" VARCHAR2(3),
	"CONE_COLLECTION_METHOD2_CODE" VARCHAR2(3),
	"COLLECTION_LAT_DEG" NUMBER(2,0),
	"COLLECTION_LAT_MIN" NUMBER(2,0),
	"COLLECTION_LAT_SEC" NUMBER(2,0),
	"COLLECTION_LATITUDE_CODE" VARCHAR2(2),
	"COLLECTION_LONG_DEG" NUMBER(3,0),
	"COLLECTION_LONG_MIN" NUMBER(2,0),
	"COLLECTION_LONG_SEC" NUMBER(2,0),
	"COLLECTION_LONGITUDE_CODE" VARCHAR2(2),
	"COLLECTION_ELEVATION" NUMBER(5,0),
	"COLLECTION_ELEVATION_MIN" NUMBER(5,0),
	"COLLECTION_ELEVATION_MAX" NUMBER(5,0),
	"COLLECTION_AREA_RADIUS" NUMBER(5,1),
	"COLLECTION_SEED_PLAN_ZONE_IND" VARCHAR2(1),
	"COLLECTION_BGC_IND" VARCHAR2(1),
	"NO_OF_CONTAINERS" NUMBER(6,2),
	"CLCTN_VOLUME" NUMBER(6,2),
	"VOL_PER_CONTAINER" NUMBER(6,2),
	"NMBR_TREES_FROM_CODE" VARCHAR2(1),
	"EFFECTIVE_POP_SIZE" NUMBER(5,1),
	"ORIGINAL_SEED_QTY" NUMBER(10,0),
	"INTERM_STRG_CLIENT_NUMBER" VARCHAR2(8),
	"INTERM_STRG_CLIENT_LOCN" VARCHAR2(2),
	"INTERM_STRG_ST_DATE" DATE,
	"INTERM_STRG_END_DATE" DATE,
	"INTERM_FACILITY_CODE" VARCHAR2(3),
	"INTERM_STRG_LOCN" VARCHAR2(55),
	"INTERM_STRG_CMT" VARCHAR2(125),
	"EXTRACTION_ST_DATE" DATE,
	"EXTRACTION_END_DATE" DATE,
	"EXTRACTION_VOLUME" NUMBER(6,2),
	"EXTRCT_CLI_NUMBER" VARCHAR2(8),
	"EXTRCT_CLI_LOCN_CD" VARCHAR2(2),
	"EXTRACTION_COMMENT" VARCHAR2(125),
	"STORED_CLI_NUMBER" VARCHAR2(8),
	"STORED_CLI_LOCN_CD" VARCHAR2(2),
	"LNGTERM_STRG_ST_DATE" DATE,
	"HISTORICAL_TSR_DATE" DATE,
	"OWNERSHIP_COMMENT" VARCHAR2(4000),
	"CONE_SEED_DESC" VARCHAR2(250),
	"SEEDLOT_COMMENT" VARCHAR2(2000),
	"TEMPORARY_STORAGE_START_DATE" DATE,
	"TEMPORARY_STORAGE_END_DATE" DATE,
	"COLLECTION_STANDARD_MET_IND" VARCHAR2(1),
	"APPLICANT_EMAIL_ADDRESS" VARCHAR2(100),
	"BIOTECH_PROCESSES_IND" VARCHAR2(1),
	"POLLEN_CONTAMINATION_IND" VARCHAR2(1),
	"POLLEN_CONTAMINATION_PCT" NUMBER(3,0),
	"CONTROLLED_CROSS_IND" VARCHAR2(1),
	"ORCHARD_COMMENT" VARCHAR2(2000),
	"TOTAL_PARENT_TREES" NUMBER(5,0),
	"SMP_PARENTS_OUTSIDE" NUMBER(5,0),
	"SMP_MEAN_BV_GROWTH" NUMBER(4,1),
	"SMP_SUCCESS_PCT" NUMBER(3,0),
	"CONTAMINANT_POLLEN_BV" NUMBER(4,1),
	"ORCHARD_CONTAMINATION_PCT" NUMBER(3,0),
	"COANCESTRY" NUMBER(20,10),
	"PROVENANCE_ID" NUMBER(5,0),
	"SEED_PLAN_ZONE_CODE" VARCHAR2(3),
	"POLLEN_CONTAMINATION_MTHD_CODE" VARCHAR2(4),
	"APPLICANT_CLIENT_NUMBER" VARCHAR2(8),
	"APPLICANT_CLIENT_LOCN" VARCHAR2(2),
	"SEED_STORE_CLIENT_NUMBER" VARCHAR2(8),
	"SEED_STORE_CLIENT_LOCN" VARCHAR2(2),
	"SEEDLOT_SOURCE_CODE" VARCHAR2(3),
	"FEMALE_GAMETIC_MTHD_CODE" VARCHAR2(4),
	"MALE_GAMETIC_MTHD_CODE" VARCHAR2(4),
	"BGC_ZONE_CODE" VARCHAR2(4),
	"BGC_SUBZONE_CODE" VARCHAR2(3),
	"VARIANT" VARCHAR2(1),
	"BEC_VERSION_ID" NUMBER(10,0),
	"PRICE_PER_KG" NUMBER(7,2),
	"PRICE_COMMENT" VARCHAR2(2000),
	"APPROVED_USERID" VARCHAR2(30),
	"APPROVED_TIMESTAMP" DATE,
	"DECLARED_USERID" VARCHAR2(30),
	"DECLARED_TIMESTAMP" DATE,
	"ENTRY_USERID" VARCHAR2(30) NOT NULL ENABLE,
	"ENTRY_TIMESTAMP" DATE NOT NULL ENABLE,
	"UPDATE_USERID" VARCHAR2(30) NOT NULL ENABLE,
	"UPDATE_TIMESTAMP" DATE NOT NULL ENABLE,
	"REVISION_COUNT" NUMBER(5,0) NOT NULL ENABLE,
	 CONSTRAINT "SEEDLOT_PK" PRIMARY KEY ("SEEDLOT_NUMBER")
  USING INDEX  ENABLE,
	 CONSTRAINT "SPR_SEE_CSC2_FK" FOREIGN KEY ("COLLECTION_SOURCE_CODE")
	  REFERENCES "THE"."COLLECTION_SOURCE_CODE" ("COLLECTION_SOURCE_CODE") ENABLE,
	 CONSTRAINT "SPR_SEE_CL2_FK" FOREIGN KEY ("APPLICANT_CLIENT_NUMBER", "APPLICANT_CLIENT_LOCN")
	  REFERENCES "THE"."CLIENT_LOCATION" ("CLIENT_NUMBER", "CLIENT_LOCN_CODE") ENABLE,
	 CONSTRAINT "SPR_SEE_ORC2_FK" FOREIGN KEY ("SECONDARY_ORCHARD_ID")
	  REFERENCES "THE"."ORCHARD" ("ORCHARD_ID") ENABLE,
	 CONSTRAINT "SPR_SEE_SSCD_FK" FOREIGN KEY ("SEEDLOT_SOURCE_CODE")
	  REFERENCES "THE"."SEEDLOT_SOURCE_CODE" ("SEEDLOT_SOURCE_CODE") ENABLE,
	 CONSTRAINT "SPR_SEE_PCMC_FK" FOREIGN KEY ("POLLEN_CONTAMINATION_MTHD_CODE")
	  REFERENCES "THE"."POLLEN_CONTAMINATION_MTHD_CODE" ("POLLEN_CONTAMINATION_MTHD_CODE") ENABLE,
	 CONSTRAINT "SPR_SEE_SSC3_FK" FOREIGN KEY ("SEEDLOT_STATUS_CODE")
	  REFERENCES "THE"."SEEDLOT_STATUS_CODE" ("SEEDLOT_STATUS_CODE") ENABLE,
	 CONSTRAINT "SPR_SEE_CLC_FK" FOREIGN KEY ("COLLECTION_LATITUDE_CODE")
	  REFERENCES "THE"."COLLECTION_LATITUDE_CODE" ("COLLECTION_LATITUDE_CODE") ENABLE,
	 CONSTRAINT "SPR_SEE_SCAC_FK" FOREIGN KEY ("SEED_COAST_AREA_CODE")
	  REFERENCES "THE"."SEED_COAST_AREA_CODE" ("SEED_COAST_AREA_CODE") ENABLE,
	 CONSTRAINT "SPR_SEE_SPZC_FK" FOREIGN KEY ("SEED_PLAN_ZONE_CODE")
	  REFERENCES "THE"."SEED_PLAN_ZONE_CODE" ("SEED_PLAN_ZONE_CODE") ENABLE,
	 CONSTRAINT "SPR_SEE_CCMC2_FK" FOREIGN KEY ("CONE_COLLECTION_METHOD2_CODE")
	  REFERENCES "THE"."CONE_COLLECTION_METHOD_CODE" ("CONE_COLLECTION_METHOD_CODE") ENABLE,
	 CONSTRAINT "SPR_SEE_SPU_FK" FOREIGN KEY ("SEED_PLAN_UNIT_ID")
	  REFERENCES "THE"."SEED_PLAN_UNIT" ("SEED_PLAN_UNIT_ID") ENABLE,
	 CONSTRAINT "SPR_SEE_GMC1_FK" FOREIGN KEY ("FEMALE_GAMETIC_MTHD_CODE")
	  REFERENCES "THE"."GAMETIC_MTHD_CODE" ("GAMETIC_MTHD_CODE") ENABLE,
	 CONSTRAINT "SPR_SEE_CL3_FK" FOREIGN KEY ("INTERM_STRG_CLIENT_NUMBER", "INTERM_STRG_CLIENT_LOCN")
	  REFERENCES "THE"."CLIENT_LOCATION" ("CLIENT_NUMBER", "CLIENT_LOCN_CODE") ENABLE,
	 CONSTRAINT "SPR_SEE_SUPR_FK" FOREIGN KEY ("PROVENANCE_ID")
	  REFERENCES "THE"."SUPERIOR_PROVENANCE" ("PROVENANCE_ID") ENABLE,
	 CONSTRAINT "SPR_SEE_NDC_FK" FOREIGN KEY ("NAD_DATUM_CODE")
	  REFERENCES "THE"."NAD_DATUM_CODE" ("NAD_DATUM_CODE") ENABLE,
	 CONSTRAINT "SPR_SEE_NTFC_FK" FOREIGN KEY ("NMBR_TREES_FROM_CODE")
	  REFERENCES "THE"."NMBR_TREES_FROM_CODE" ("NMBR_TREES_FROM_CODE") ENABLE,
	 CONSTRAINT "SPR_SEE_VC_FK" FOREIGN KEY ("VEGETATION_CODE")
	  REFERENCES "THE"."VEGETATION_CODE" ("VEGETATION_CODE") ENABLE,
	 CONSTRAINT "SPR_SEE_CLC1_FK" FOREIGN KEY ("COLLECTION_LONGITUDE_CODE")
	  REFERENCES "THE"."COLLECTION_LONGITUDE_CODE" ("COLLECTION_LONGITUDE_CODE") ENABLE,
	 CONSTRAINT "SPR_SEE_CL1_FK" FOREIGN KEY ("SEED_STORE_CLIENT_NUMBER", "SEED_STORE_CLIENT_LOCN")
	  REFERENCES "THE"."CLIENT_LOCATION" ("CLIENT_NUMBER", "CLIENT_LOCN_CODE") ENABLE,
	 CONSTRAINT "SPR_SEE_CCMC_FK" FOREIGN KEY ("CONE_COLLECTION_METHOD_CODE")
	  REFERENCES "THE"."CONE_COLLECTION_METHOD_CODE" ("CONE_COLLECTION_METHOD_CODE") ENABLE,
	 CONSTRAINT "SPR_SEE_GMC2_FK" FOREIGN KEY ("MALE_GAMETIC_MTHD_CODE")
	  REFERENCES "THE"."GAMETIC_MTHD_CODE" ("GAMETIC_MTHD_CODE") ENABLE,
	 CONSTRAINT "SPR_SEE_GCC_FK" FOREIGN KEY ("GENETIC_CLASS_CODE")
	  REFERENCES "THE"."GENETIC_CLASS_CODE" ("GENETIC_CLASS_CODE") ENABLE,
	 CONSTRAINT "SPR_SEE_IFC_FK" FOREIGN KEY ("INTERM_FACILITY_CODE")
	  REFERENCES "THE"."INTERM_FACILITY_CODE" ("INTERM_FACILITY_CODE") ENABLE,
	 CONSTRAINT "SPR_SEE_ORC_FK" FOREIGN KEY ("ORCHARD_ID")
	  REFERENCES "THE"."ORCHARD" ("ORCHARD_ID") ENABLE,
	 CONSTRAINT "SPR_SEE_OU1_FK" FOREIGN KEY ("ORG_UNIT_NO")
	  REFERENCES "THE"."ORG_UNIT" ("ORG_UNIT_NO") ENABLE,
	 CONSTRAINT "SPR_SEE_BVC_FK" FOREIGN KEY ("BEC_VERSION_ID")
	  REFERENCES "THE"."BEC_VERSION_CONTROL" ("BEC_VERSION_ID") ENABLE
   ) ;
