
  CREATE OR REPLACE EDITIONABLE PACKAGE "THE"."LEXIS_REPORTING" IS

  TYPE REF_CUR_GENERAL IS REF CURSOR;

  FUNCTION RETRIEVE_SPECIES_ENDUSE(P_APPLICATION_NUMBER IN EXPORT_EXEMPTION_APPLICATION.APPLICATION_NUMBER%TYPE)
    RETURN VARCHAR2;

  FUNCTION APPLICATION_HAS_OFFERS(P_APPLICATION_NUMBER IN EXPORT_EXEMPTION_APPLICATION.APPLICATION_NUMBER%TYPE)
    RETURN VARCHAR2;

  FUNCTION RETRIEVE_LOG_AMV_EFFCTVE_DATE(P_DATE EXPORT_LOG_AMV.EFFECTIVE_DATE%TYPE)
    RETURN DATE;

  FUNCTION GET_APP_REMARK_AT_ROW(P_APPLICATION_NUMBER IN EXPORT_EXEMPTION_APPLICATION.APPLICATION_NUMBER%TYPE,
                                 P_ROWNUMBER          IN NUMBER)
    RETURN VARCHAR2;

  PROCEDURE BIWEEKLY_RPT(P_ORG_UNIT     VARCHAR2,
                         P_JURISDICTION VARCHAR2,
                         P_FROM_DATE    VARCHAR2,
                         P_TO_DATE      VARCHAR2,
                         P_BIWEEKLY     IN OUT REF_CUR_GENERAL);

  PROCEDURE BIWEEKLY_SUBREPORT_RPT(P_APPLICATION_NUMBER EXPORT_EXEMPTION_APPLICATION.APPLICATION_NUMBER%TYPE,
                                   P_JURISDICTION       EXPORT_EXEMPTION_APPLICATION.EXPORT_JURISDICTION_CODE%TYPE,
                                   P_BIWEEKLY_PACKAGE   IN OUT REF_CUR_GENERAL);

  PROCEDURE BIWEEKLY_REPORT_CSV(P_WHERE_CLAUSE IN VARCHAR,
                                P_PARAMS_ARRAY IN CBR_VARCHAR2_ARRAY,
                                P_NUM_PARAMS   IN NUMBER,
                                P_BIWEEKLY     IN OUT REF_CUR_GENERAL);

  PROCEDURE APPLICATION_RPT(P_ORG_UNIT         ORG_UNIT.ORG_UNIT_NO%TYPE,
                            P_JURISDICTION     VARCHAR2,
                            P_EXEMPTION_REASON VARCHAR2,
                            P_RECEIVED_FROM    VARCHAR2,
                            P_RECEIVED_TO      VARCHAR2,
                            P_CLIENT_NUMBER    VARCHAR2,
                            P_GROWTH_TYPE      VARCHAR2,
                            P_APPLICATION      IN OUT REF_CUR_GENERAL);

  PROCEDURE APP_SUBREPORT_RPT(P_APPLICATION_NUMBER EXPORT_EXEMPTION_APPLICATION.APPLICATION_NUMBER%TYPE,
                              P_REMARKS            IN OUT REF_CUR_GENERAL);

  PROCEDURE APP_REPORT_CSV(P_WHERE_CLAUSE IN VARCHAR,
                           P_PARAMS_ARRAY IN CBR_VARCHAR2_ARRAY,
                           P_NUM_PARAMS   IN NUMBER,
                           P_BIWEEKLY     IN OUT REF_CUR_GENERAL);

  PROCEDURE OFFERS_LEDGER_RPT(P_APPLICATION_DATE_FROM VARCHAR2,
                              P_APPLICATION_DATE_TO   VARCHAR2,
                              P_ORG_UNIT              VARCHAR2,
                              P_CLIENT_NUMBER         EXPORT_EXEMPTION_APPLICATION.OWNER_CLIENT_NUMBER%TYPE,
                              P_WITHDRAWN_DATE_FROM   VARCHAR2,
                              P_WITHDRAWN_DATE_TO     VARCHAR2,
                              P_JURISDICTION          EXPORT_EXEMPTION_APPLICATION.EXPORT_JURISDICTION_CODE%TYPE,
                              P_OFFERS                IN OUT REF_CUR_GENERAL);

  PROCEDURE OFFERS_REPORT_CSV(P_WHERE_CLAUSE IN VARCHAR,
                              P_PARAMS_ARRAY IN CBR_VARCHAR2_ARRAY,
                              P_NUM_PARAMS   IN NUMBER,
                              P_OFFERS       IN OUT REF_CUR_GENERAL);

  PROCEDURE SPECIES_GRADE_RPT(P_DATE_FROM        VARCHAR2,
                              P_DATE_TO          VARCHAR2,
                              P_ORG_UNIT         VARCHAR2,
                              P_EXEMPTION_NUMBER EXPORT_EXEMPTION_APPLICATION.OWNER_CLIENT_NUMBER%TYPE,
                              P_EXEMPTION_TYPE   EXPORT_EXEMPTION.EXPORT_EXEMPTION_TYPE_CODE%TYPE,
                              P_EXEMPTION_REASON EXPORT_EXEMPTION_APPLICATION.EXPORT_EXEMPTION_REASON_CODE%TYPE,
                              P_GROWTH_TYPE      EXPORT_EXEMPTION_APPLICATION.EXPORT_GROWTH_TYPE_CODE%TYPE,
                              P_TIMBER_MARK      EXPORT_SCALE_DETAIL.TIMBER_MARK%TYPE,
                              P_FOREST_FILE_ID   HAULING_AUTHORITY.FOREST_FILE_ID%TYPE,
                              P_PERMIT_STATUS    EXPORT_PERMIT_DETAIL.EXPORT_PERMIT_STATUS_CODE%TYPE,
                              P_SPECIES_GRADE    IN OUT REF_CUR_GENERAL);

  PROCEDURE SPECIES_GRADE_REPORT_CSV(P_DATE_FROM          IN EXPORT_PERMIT_DETAIL.EXPORT_PERMIT_ISSUE_DATE%TYPE,
                                     P_DATE_TO            IN EXPORT_PERMIT_DETAIL.EXPORT_PERMIT_ISSUE_DATE%TYPE,
                                     P_ORG_UNIT_NO        IN VARCHAR2,
                                     P_PERMIT_STATUS      IN EXPORT_PERMIT_DETAIL.EXPORT_PERMIT_STATUS_CODE%TYPE,
                                     P_EXEMPTION_NUMBER   IN EXPORT_PERMIT_DETAIL.Exemption_Number%TYPE,
                                     P_EXEMPTION_TYPE     IN EXPORT_EXEMPTION.Export_Exemption_Type_Code%TYPE,
                                     P_EXEMPTION_REASON   IN EXPORT_EXEMPTION_REASON_CODE.EXPORT_EXEMPTION_REASON_CODE%TYPE,
                                     P_GROWTH_TYPE        IN EXPORT_GROWTH_TYPE_CODE.EXPORT_GROWTH_TYPE_CODE%TYPE,
                                     P_TIMBER_MARK        IN HAULING_AUTHORITY.Timber_Mark%TYPE,
                                     P_FOREST_FILE_ID     IN HAULING_AUTHORITY.FOREST_FILE_ID%TYPE,
                                     P_SPECIES_GRADE      IN OUT REF_CUR_GENERAL);

  PROCEDURE SPECIES_GRADE_REGION_SUBRPT(P_DATE_FROM        VARCHAR2,
                                        P_DATE_TO          VARCHAR2,
                                        P_ORG_UNIT         VARCHAR2,
                                        P_EXEMPTION_NUMBER EXPORT_EXEMPTION_APPLICATION.OWNER_CLIENT_NUMBER%TYPE,
                                        P_EXEMPTION_TYPE   EXPORT_EXEMPTION.EXPORT_EXEMPTION_TYPE_CODE%TYPE,
                                        P_EXEMPTION_REASON EXPORT_EXEMPTION_APPLICATION.EXPORT_EXEMPTION_REASON_CODE%TYPE,
                                        P_GROWTH_TYPE      EXPORT_EXEMPTION_APPLICATION.EXPORT_GROWTH_TYPE_CODE%TYPE,
                                        P_TIMBER_MARK      EXPORT_SCALE_DETAIL.TIMBER_MARK%TYPE,
                                        P_FOREST_FILE_ID   HAULING_AUTHORITY.FOREST_FILE_ID%TYPE,
                                        P_PERMIT_STATUS    EXPORT_PERMIT_DETAIL.EXPORT_PERMIT_STATUS_CODE%TYPE,
                                        P_SPECIES_GRADE    IN OUT REF_CUR_GENERAL);

  PROCEDURE EXEMPTION_LEDGER_RPT(P_FROM_DATE        VARCHAR2,
                                 P_TO_DATE          VARCHAR2,
                                 P_LISTING_FROM_DATE VARCHAR2,
                                 P_LISTING_TO_DATE  VARCHAR2,
                                 P_ORG_UNIT         VARCHAR2,
                                 P_EXEMPTION_REASON EXPORT_EXEMPTION_REASON_CODE.EXPORT_EXEMPTION_REASON_CODE%TYPE,
                                 P_EXEMPTION_TYPE   EXPORT_EXEMPTION_TYPE_CODE.EXPORT_EXEMPTION_TYPE_CODE%TYPE,
                                 P_CLIENT           CLIENT_LOCATION.CLIENT_NUMBER%TYPE,
                                 P_GROWTH_TYPE      EXPORT_EXEMPTION_APPLICATION.EXPORT_GROWTH_TYPE_CODE%TYPE,
                                 P_EXEMPTION_NUMBER EXPORT_EXEMPTION_APPLICATION.EXEMPTION_NUMBER%TYPE,
                                 P_EXEMPTION_STATUS EXPORT_EXEMPTION.EXPORT_EXEMPTION_STATUS_CODE%TYPE,
                                 P_EXEMPTIONS       IN OUT REF_CUR_GENERAL);

  PROCEDURE EXEMPTION_LEDGER_RPT_CSV(P_WHERE_CLAUSE IN VARCHAR,
                                     P_PARAMS_ARRAY IN CBR_VARCHAR2_ARRAY,
                                     P_NUM_PARAMS   IN NUMBER,
                                     P_EXEMPTION    IN OUT REF_CUR_GENERAL);

  PROCEDURE PERMIT_LEDGER_REPORT(P_FROM_DATE        VARCHAR2,
                                 P_TO_DATE          VARCHAR2,
                                 P_CLIENT_NUMBER    EXPORT_PERMIT_DETAIL.CLIENT_NUMBER%TYPE,
                                 P_ORG_UNIT_NUMBER  VARCHAR2,
                                 P_EXEMPTION_NUMBER EXPORT_EXEMPTION.EXEMPTION_NUMBER%TYPE,
                                 P_PERMIT_STATUS    EXPORT_PERMIT_DETAIL.EXPORT_PERMIT_STATUS_CODE%TYPE,
                                 P_EXEMPTION_TYPE   EXPORT_EXEMPTION.EXPORT_EXEMPTION_TYPE_CODE%TYPE,
                                 P_EXEMPTION_REASON EXPORT_EXEMPTION_APPLICATION.EXPORT_EXEMPTION_REASON_CODE%TYPE,
                                 P_GROWTH_TYPE      EXPORT_EXEMPTION_APPLICATION.EXPORT_GROWTH_TYPE_CODE%TYPE,
                                 P_TIMBER_MARK      EXPORT_SCALE_DETAIL.TIMBER_MARK%TYPE,
                                 P_DEST_COUNTRY     EXPORT_PERMIT_DETAIL.EXPORT_COUNTRY_CODE%TYPE,
                                 P_PERMIT_REPORT    IN OUT REF_CUR_GENERAL);

  PROCEDURE PERMIT_REPORT_CSV(P_WHERE_CLAUSE IN VARCHAR,
                              P_PARAMS_ARRAY IN CBR_VARCHAR2_ARRAY,
                              P_NUM_PARAMS   IN NUMBER,
                              P_PERMITS      IN OUT REF_CUR_GENERAL);

  PROCEDURE PERMIT_REPORT(P_EXPORT_PERMIT_NUMBER IN EXPORT_PERMIT_DETAIL.EXPORT_PERMIT_DETAIL_NUMBER%TYPE,
                          P_PERMIT_REPORT        IN OUT REF_CUR_GENERAL);

  PROCEDURE PERMIT_REPORT_SUB(P_EXPORT_PERMIT_NUMBER IN EXPORT_PERMIT.EXPORT_PERMIT_NMBR%TYPE,
                              P_PERMIT_REPORT_SUB IN OUT REF_CUR_GENERAL);

  PROCEDURE TRANSPORT_LEDGER_RPT(P_FROM_DATE           VARCHAR2,
                                 P_TO_DATE             VARCHAR2,
                                 P_JURISDICTION        EXPORT_JURISDICTION_CODE.EXPORT_JURISDICTION_CODE%TYPE,
                                 P_ORG_UNIT_NUMBER     VARCHAR2,
                                 P_DESTINATION_COUNTRY EXPORT_COUNTRY_CODE.EXPORT_COUNTRY_CODE%TYPE,
                                 P_PORT_OF_EXPORT      EXPORT_PORT_OF_EXPORT_CODE.EXPORT_PORT_OF_EXPORT_CODE%TYPE,
                                 P_STATUS              EXPORT_PERMIT_STATUS_CODE.EXPORT_PERMIT_STATUS_CODE%TYPE,
                                 P_TRANSPORT_REPORT    IN OUT REF_CUR_GENERAL);

  PROCEDURE TRANSPORT_REPORT_CSV(P_WHERE_CLAUSE IN VARCHAR,
                                 P_PARAMS_ARRAY IN CBR_VARCHAR2_ARRAY,
                                 P_NUM_PARAMS   IN NUMBER,
                                 P_FEES         IN OUT REF_CUR_GENERAL);

  PROCEDURE FEE_SUMMARY_RPT(P_FROM_DATE        VARCHAR2,
                            P_TO_DATE          VARCHAR2,
                            P_ORG_UNIT_NUMBER  VARCHAR2,
                            P_EXEMPTION_NUMBER EXPORT_EXEMPTION.EXEMPTION_NUMBER%TYPE,
                            P_EXEMPTION_TYPE   EXPORT_EXEMPTION.EXPORT_EXEMPTION_TYPE_CODE%TYPE,
                            P_EXEMPTION_REASON EXPORT_EXEMPTION_APPLICATION.EXPORT_EXEMPTION_REASON_CODE%TYPE,
                            P_GROWTH_TYPE      EXPORT_EXEMPTION_APPLICATION.EXPORT_GROWTH_TYPE_CODE%TYPE,
                            P_FEE_REPORT       IN OUT REF_CUR_GENERAL);

  PROCEDURE FEE_SUMMARY_RPT_CSV(P_WHERE_CLAUSE IN VARCHAR,
                                P_PARAMS_ARRAY IN CBR_VARCHAR2_ARRAY,
                                P_NUM_PARAMS   IN NUMBER,
                                P_FEES         IN OUT REF_CUR_GENERAL);

  PROCEDURE EXEMPTION_RPT(P_EXEMPTION_NUMBER VARCHAR2,
                          P_EXEMPTION        IN OUT REF_CUR_GENERAL);

  PROCEDURE PROVINCIAL_TEAC_REPORT(P_ORG_UNIT_NUMBER       IN VARCHAR2,
                                   P_LISTING_STARTING_DATE IN number,
                                   P_TEAC_REPORT           IN OUT REF_CUR_GENERAL);

  PROCEDURE FEDERAL_TEAC_REPORT(P_ORG_UNIT_NUMBER       IN VARCHAR2,
                                P_LISTING_STARTING_DATE IN number,
                                P_TEAC_REPORT           IN OUT REF_CUR_GENERAL);

  PROCEDURE TENURE_ANALYSIS(P_ORG_UNIT_NUMBER  IN VARCHAR2,
                            P_EXEMPTION_REASON IN EXPORT_EXEMPTION_REASON_CODE.EXPORT_EXEMPTION_REASON_CODE%TYPE,
                            P_EXEMPTION_TYPE   IN EXPORT_EXEMPTION_TYPE_CODE.EXPORT_EXEMPTION_TYPE_CODE%TYPE,
                            P_EXEMPTION_NUMBER IN EXPORT_EXEMPTION.EXEMPTION_NUMBER%TYPE,
                            P_CLIENT_NUMBER    IN VARCHAR2,
                            P_CLIENT_TYPE      IN VARCHAR2,
                            P_FROM_DATE        IN VARCHAR2,
                            P_TO_DATE          IN VARCHAR2,
                            P_TENURE_TYPE_1    IN PROV_FOREST_USE.FILE_TYPE_CODE%TYPE,
                            P_TENURE_TYPE_2    IN PROV_FOREST_USE.FILE_TYPE_CODE%TYPE,
                            P_TENURE_TYPE_3    IN PROV_FOREST_USE.FILE_TYPE_CODE%TYPE,
                            P_TENURE_TYPE_4    IN PROV_FOREST_USE.FILE_TYPE_CODE%TYPE,
                            P_TENURE_TYPE_5    IN PROV_FOREST_USE.FILE_TYPE_CODE%TYPE,
                            P_TENURE_TYPE_6    IN PROV_FOREST_USE.FILE_TYPE_CODE%TYPE,
                            P_TIMBER_MARK_1    IN HAULING_AUTHORITY.TIMBER_MARK%TYPE,
                            P_TIMBER_MARK_2    IN HAULING_AUTHORITY.TIMBER_MARK%TYPE,
                            P_TIMBER_MARK_3    IN HAULING_AUTHORITY.TIMBER_MARK%TYPE,
                            P_TIMBER_MARK_4    IN HAULING_AUTHORITY.TIMBER_MARK%TYPE,
                            P_TIMBER_MARK_5    IN HAULING_AUTHORITY.TIMBER_MARK%TYPE,
                            P_TIMBER_MARK_6    IN HAULING_AUTHORITY.TIMBER_MARK%TYPE,
                            P_FOREST_FILE_ID   IN HAULING_AUTHORITY.FOREST_FILE_ID%TYPE,
                            P_TENURE_ANALYSIS  IN OUT REF_CUR_GENERAL);

  PROCEDURE TENURE_ANALYSIS_CSV(P_WHERE_CLAUSE IN VARCHAR,
                                P_PARAMS_ARRAY IN CBR_VARCHAR2_ARRAY,
                                P_NUM_PARAMS   IN NUMBER,
                                P_FEES         IN OUT REF_CUR_GENERAL);

END LEXIS_REPORTING;
/
CREATE OR REPLACE EDITIONABLE PACKAGE BODY "THE"."LEXIS_REPORTING" IS

  -- AUTHOR  : BFALK
  -- CREATED : 03/15/2008 9:05:57 AM
  -- PURPOSE : HOLDS ALL REPORTING QUERIES FOR THE LEXIS APPLICATION

  /******************************************************************************
      FUNCTION:  RETRIEVE_SPECIES_ENDUSE
      PURPOSE: RETRIEVE THE SPECIES AND ENDUSE FOR AN APPLICATION.
  ******************************************************************************/
  FUNCTION RETRIEVE_SPECIES_ENDUSE(P_APPLICATION_NUMBER IN EXPORT_EXEMPTION_APPLICATION.APPLICATION_NUMBER%TYPE)
    RETURN VARCHAR2 IS

    V_SPECIES VARCHAR2(500);
    V_ENDUSE  VARCHAR2(50);

    CURSOR C1 IS

      SELECT ESC.EXPORT_SPECIES_CODE AS ESC_DESC,
             EEUS.DESCRIPTION        AS EUS_DESC
        FROM EXPORT_EXMPTN_APPL_SPCS_ENDUSE A
       INNER JOIN EXPORT_SPECIES_CODE ESC
          ON A.EXPORT_SPECIES_CODE = ESC.EXPORT_SPECIES_CODE
       INNER JOIN EXPORT_END_USE_CODE EEUS
          ON A.EXPORT_END_USE_CODE = EEUS.EXPORT_END_USE_CODE
       WHERE A.APPLICATION_NUMBER = P_APPLICATION_NUMBER;

  BEGIN

    FOR r_c1 IN C1 LOOP
      V_ENDUSE  := r_c1.EUS_DESC;
      V_SPECIES := V_SPECIES || r_c1.ESC_DESC || ' / ';
    END LOOP;

    RETURN V_SPECIES || V_ENDUSE;

  END RETRIEVE_SPECIES_ENDUSE;

  /******************************************************************************
      FUNCTION:  APPLICATION_HAS_OFFERS
      PURPOSE: Determines if an application has offers. Returns either Y or N.
  ******************************************************************************/
  FUNCTION APPLICATION_HAS_OFFERS(P_APPLICATION_NUMBER IN EXPORT_EXEMPTION_APPLICATION.APPLICATION_NUMBER%TYPE)
    RETURN VARCHAR2 IS

    V_HAS_OFFERS   VARCHAR2(1) := 'N';
    V_OFFERS_COUNT NUMBER;

    CURSOR C1 IS
      SELECT COUNT(EPO.EXPORT_PURCHASE_OFFER_NUMBER) AS PO_COUNT
        FROM EXPORT_PURCHASE_OFFER EPO
       INNER JOIN EXPORT_PACKAGE EP
          ON EPO.PACKAGE_NUMBER = EP.PACKAGE_NUMBER
       INNER JOIN EXPORT_EXEMPTION_APPLICATION EEA
          ON EP.APPLICATION_NUMBER = EEA.APPLICATION_NUMBER
       WHERE EEA.APPLICATION_NUMBER = P_APPLICATION_NUMBER;

  BEGIN

    OPEN C1;
    FETCH C1
      INTO V_OFFERS_COUNT;
    CLOSE C1;

    IF V_OFFERS_COUNT > 0 THEN
      V_HAS_OFFERS := 'Y';
    END IF;

    RETURN V_HAS_OFFERS;

  END APPLICATION_HAS_OFFERS;

  /******************************************************************************
      FUNCTION:  RETRIEVE_LOG_AMV_EFFECTIVE_DATE
      PURPOSE: RETRIEVES THE MAX EFFECTIVE DATE LESS THAN THE GIVEN DATE
  ******************************************************************************/
  FUNCTION RETRIEVE_LOG_AMV_EFFCTVE_DATE(P_DATE EXPORT_LOG_AMV.EFFECTIVE_DATE%TYPE)
    RETURN DATE IS

    V_EFFECTIVE_DATE EXPORT_LOG_AMV.EFFECTIVE_DATE%TYPE;

  BEGIN
    SELECT MAX(T.EFFECTIVE_DATE)
      INTO V_EFFECTIVE_DATE
      FROM EXPORT_LOG_AMV T
     WHERE T.EFFECTIVE_DATE <= P_DATE;

    RETURN V_EFFECTIVE_DATE;

  END RETRIEVE_LOG_AMV_EFFCTVE_DATE;

  /******************************************************************************
      PROCEDURE:  PERMIT_LEDGER_REPORT
      PURPOSE: USED BY LEXIS_PERMIT_LEDGER.RPT
  ******************************************************************************/
  PROCEDURE PERMIT_LEDGER_REPORT(P_FROM_DATE        VARCHAR2,
                                 P_TO_DATE          VARCHAR2,
                                 P_CLIENT_NUMBER    EXPORT_PERMIT_DETAIL.CLIENT_NUMBER%TYPE,
                                 P_ORG_UNIT_NUMBER  VARCHAR2,
                                 P_EXEMPTION_NUMBER EXPORT_EXEMPTION.EXEMPTION_NUMBER%TYPE,
                                 P_PERMIT_STATUS    EXPORT_PERMIT_DETAIL.export_permit_status_code%TYPE,
                                 P_EXEMPTION_TYPE   EXPORT_EXEMPTION.EXPORT_EXEMPTION_TYPE_CODE%TYPE,
                                 P_EXEMPTION_REASON EXPORT_EXEMPTION_APPLICATION.EXPORT_EXEMPTION_REASON_CODE%TYPE,
                                 P_GROWTH_TYPE      EXPORT_EXEMPTION_APPLICATION.EXPORT_GROWTH_TYPE_CODE%TYPE,
                                 P_TIMBER_MARK      EXPORT_SCALE_DETAIL.TIMBER_MARK%TYPE,
                                 P_DEST_COUNTRY     EXPORT_PERMIT_DETAIL.EXPORT_COUNTRY_CODE%TYPE,
                                 P_PERMIT_REPORT    IN OUT REF_CUR_GENERAL) AS

    V_DATE_FROM DATE := TO_DATE('0001-01-01', 'YYYY-MM-DD');
    V_DATE_TO   DATE := TO_DATE('9999-12-31', 'YYYY-MM-DD');

    V_CLIENT_NUMBER    VARCHAR2(10) := P_CLIENT_NUMBER;
    V_EXEMPTION_NUMBER VARCHAR2(12) := P_EXEMPTION_NUMBER;

    V_PERMIT_STATUS    VARCHAR2(5) := '%';
    V_EXEMPTION_TYPE   VARCHAR2(5) := '%';
    V_EXEMPTION_REASON VARCHAR2(5) := '%';
    V_GROWTH_TYPE      VARCHAR2(5) := '%';
    V_TIMBER_MARK      VARCHAR2(12) := '%';
    V_DEST_COUNTRY     VARCHAR2(5) := '%';

  BEGIN

    IF P_FROM_DATE IS NOT NULL THEN
      V_DATE_FROM := TO_DATE(P_FROM_DATE, 'YYYY-MM-DD');
    END IF;

    IF P_TO_DATE IS NOT NULL THEN
      V_DATE_TO := TO_DATE(P_TO_DATE, 'YYYY-MM-DD');
    END IF;

    IF V_CLIENT_NUMBER IS NULL THEN
      V_CLIENT_NUMBER := '%';
    END IF;

    IF V_EXEMPTION_NUMBER IS NULL THEN
      V_EXEMPTION_NUMBER := '%';
    END IF;

    IF P_PERMIT_STATUS IS NOT NULL THEN
      V_PERMIT_STATUS := P_PERMIT_STATUS;
    END IF;
    IF P_EXEMPTION_TYPE IS NOT NULL THEN
      V_EXEMPTION_TYPE := P_EXEMPTION_TYPE;
    END IF;
    IF P_EXEMPTION_REASON IS NOT NULL THEN
      V_EXEMPTION_REASON := P_EXEMPTION_REASON;
    END IF;
    IF P_GROWTH_TYPE IS NOT NULL THEN
      V_GROWTH_TYPE := P_GROWTH_TYPE;
    END IF;
    IF P_TIMBER_MARK IS NULL THEN
      V_TIMBER_MARK := '%';
    ELSE
      V_TIMBER_MARK := P_TIMBER_MARK;
    END IF;
    IF P_DEST_COUNTRY IS NOT NULL THEN
      V_DEST_COUNTRY := P_DEST_COUNTRY;
    END IF;

    OPEN P_PERMIT_REPORT FOR
      SELECT A.EXPORT_PERMIT_DETAIL_NUMBER,
             A.ORG_UNIT_NO,
             I.ORG_UNIT_NAME AS REGION,
             A.CLIENT_NUMBER,
             ECC.DESCRIPTION AS EXPORT_COUNTRY_CODE,
             case
               when FI.V_FI is not null then
                FI.V_FI
               else
                0
             end as V_FI,
             case
               when CE.V_CE is not null then
                CE.V_CE
               else
                0
             end as V_CE,
             case
               when SP.V_SP is not null then
                SP.V_SP
               else
                0
             end as V_SP,
             case
               when LO.V_LO is not null then
                LO.V_LO
               else
                0
             end as V_LO,
             case
               when HE.V_HE is not null then
                HE.V_HE
               else
                0
             end as V_HE,
             case
               when BA.V_BA is not null then
                BA.V_BA
               else
                0
             end as V_BA,
             case
               when CY.V_CY is not null then
                CY.V_CY
               else
                0
             end as V_CY,
             case
               when AL.V_AL is not null then
                AL.V_AL
               else
                0
             end as V_AL,
             case
               when CO.V_CO is not null then
                CO.V_CO
               else
                0
             end as V_CO,
             case
               when HD.V_HD is not null then
                HD.V_HD
               else
                0
             end as V_HD,
             case
               when OT.V_OT is not null then
                OT.V_OT
               else
                0
             end as V_OT,
             sm.v_SM,
             EJC.DESCRIPTION AS EXPORT_JURISDICTION_CODE,
             C.EXPORT_PRODUCT_TYPE_CODE,
             F.DESCRIPTION AS PRODUCT_TYPE,
             C.EXPORT_EXEMPTION_REASON_CODE,
             G.DESCRIPTION AS REASON,
             E.EXPORT_EXEMPTION_TYPE_CODE,
             H.DESCRIPTION AS EXEMPTION_TYPE,
             A.EXEMPTION_NUMBER
        FROM EXPORT_PERMIT_DETAIL A
       INNER JOIN EXPORT_SCALE_DETAIL D
          ON A.EXPORT_PERMIT_DETAIL_NUMBER = D.EXPORT_PERMIT_DETAIL_NUMBER
       INNER JOIN EXPORT_EXEMPTION E
          ON A.EXEMPTION_NUMBER = E.EXEMPTION_NUMBER
       INNER JOIN EXPORT_EXEMPTION_APPLICATION C ON E.Exemption_Number = C.Exemption_Number
       INNER JOIN EXPORT_PRODUCT_TYPE_CODE F
          ON C.EXPORT_PRODUCT_TYPE_CODE = F.EXPORT_PRODUCT_TYPE_CODE
       INNER JOIN EXPORT_EXEMPTION_REASON_CODE G
          ON C.EXPORT_EXEMPTION_REASON_CODE =
             G.EXPORT_EXEMPTION_REASON_CODE
       INNER JOIN EXPORT_EXEMPTION_TYPE_CODE H
          ON H.EXPORT_EXEMPTION_TYPE_CODE = E.EXPORT_EXEMPTION_TYPE_CODE
       INNER JOIN ORG_UNIT I
          ON A.ORG_UNIT_NO = I.ORG_UNIT_NO
       INNER JOIN EXPORT_JURISDICTION_CODE EJC
          ON C.EXPORT_JURISDICTION_CODE = EJC.EXPORT_JURISDICTION_CODE
       INNER JOIN EXPORT_COUNTRY_CODE ECC
          ON A.EXPORT_COUNTRY_CODE = ECC.EXPORT_COUNTRY_CODE
        LEFT OUTER JOIN (select esd_fi.export_permit_detail_number as permit_no,
                                sum(esd_fi.species_grade_volume) as V_FI
                           from export_scale_detail esd_fi
                          where esd_fi.export_species_code = 'FI'
                          group by esd_fi.export_permit_detail_number) FI
          on FI.permit_no = a.export_permit_detail_number
        LEFT OUTER JOIN (select esd_ce.export_permit_detail_number as permit_no,
                                sum(esd_ce.species_grade_volume) as V_CE
                           from export_scale_detail esd_ce
                          where esd_ce.export_species_code = 'CE'
                          group by esd_ce.export_permit_detail_number) CE
          on CE.permit_no = a.export_permit_detail_number
        LEFT OUTER JOIN (select esd_sp.export_permit_detail_number as permit_no,
                                sum(esd_sp.species_grade_volume) as V_SP
                           from export_scale_detail esd_sp
                          where esd_sp.export_species_code = 'SP'
                          group by esd_sp.export_permit_detail_number) SP
          on SP.permit_no = a.export_permit_detail_number
        LEFT OUTER JOIN (select esd_lo.export_permit_detail_number as permit_no,
                                sum(esd_lo.species_grade_volume) as V_LO
                           from export_scale_detail esd_lo
                          where esd_lo.export_species_code = 'LO'
                          group by esd_lo.export_permit_detail_number) LO
          on LO.permit_no = a.export_permit_detail_number
        LEFT OUTER JOIN (select esd_he.export_permit_detail_number as permit_no,
                                sum(esd_he.species_grade_volume) as V_HE
                           from export_scale_detail esd_he
                          where esd_he.export_species_code = 'HE'
                          group by esd_he.export_permit_detail_number) HE
          on HE.permit_no = a.export_permit_detail_number
        LEFT OUTER JOIN (select esd_ba.export_permit_detail_number as permit_no,
                                sum(esd_ba.species_grade_volume) as V_BA
                           from export_scale_detail esd_ba
                          where esd_ba.export_species_code = 'BA'
                          group by esd_ba.export_permit_detail_number) BA
          on BA.permit_no = a.export_permit_detail_number
        LEFT OUTER JOIN (select esd_cy.export_permit_detail_number as permit_no,
                                sum(esd_cy.species_grade_volume) as V_CY
                           from export_scale_detail esd_cy
                          where esd_cy.export_species_code = 'CY'
                          group by esd_cy.export_permit_detail_number) CY
          on CY.permit_no = a.export_permit_detail_number
        LEFT OUTER JOIN (select esd_al.export_permit_detail_number as permit_no,
                                sum(esd_al.species_grade_volume) as V_AL
                           from export_scale_detail esd_al
                          where esd_al.export_species_code = 'AL'
                          group by esd_al.export_permit_detail_number) AL
          on AL.permit_no = a.export_permit_detail_number
        LEFT OUTER JOIN (select esd_co.export_permit_detail_number as permit_no,
                                sum(esd_co.species_grade_volume) as V_CO
                           from export_scale_detail esd_co
                          where esd_co.export_species_code = 'CO'
                          group by esd_co.export_permit_detail_number) CO
          on CO.permit_no = a.export_permit_detail_number
        LEFT OUTER JOIN (select esd_HD.export_permit_detail_number as permit_no,
                                sum(esd_hd.species_grade_volume) as V_HD
                           from export_scale_detail esd_HD
                          where esd_HD.export_species_code in
                                ('AR', 'AS', 'BI', 'MA')
                          group by esd_HD.export_permit_detail_number) HD
          on HD.permit_no = a.export_permit_detail_number
        LEFT OUTER JOIN (select esd_SM.export_permit_detail_number as permit_no,
                                sum(esd_sm.species_grade_volume) as V_SM
                           from export_scale_detail esd_SM
                          group by esd_SM.export_permit_detail_number) SM
          on SM.permit_no = a.export_permit_detail_number
        LEFT OUTER JOIN (select esd_OT.export_permit_detail_number as permit_no,
                                sum(esd_OT.species_grade_volume) as V_OT
                           from export_scale_detail esd_OT
                          where esd_ot.export_species_code in
                                ('LA', 'WB', 'WH', 'YE', 'UU')
                          group by esd_OT.export_permit_detail_number) OT
          on OT.permit_no = a.export_permit_detail_number
       WHERE (A.EXPORT_PERMIT_ISSUE_DATE BETWEEN V_DATE_FROM AND V_DATE_TO OR
             A.EXPORT_PERMIT_ISSUE_DATE IS NULL)
         AND A.CLIENT_NUMBER LIKE V_CLIENT_NUMBER
         AND P_ORG_UNIT_NUMBER like '%' || A.ORG_UNIT_NO || '%'
         AND A.EXEMPTION_NUMBER LIKE V_EXEMPTION_NUMBER
         AND A.EXPORT_PERMIT_STATUS_CODE LIKE V_PERMIT_STATUS
         AND E.EXPORT_EXEMPTION_TYPE_CODE LIKE V_EXEMPTION_TYPE
         AND C.EXPORT_EXEMPTION_REASON_CODE LIKE V_EXEMPTION_REASON
         AND C.EXPORT_GROWTH_TYPE_CODE LIKE V_GROWTH_TYPE
         AND ((D.TIMBER_MARK LIKE V_TIMBER_MARK) OR (V_TIMBER_MARK = '%'))
         AND A.EXPORT_COUNTRY_CODE LIKE V_DEST_COUNTRY
       GROUP BY A.EXPORT_PERMIT_DETAIL_NUMBER,
                FI.V_FI,
                CE.V_CE,
                SP.V_SP,
                LO.V_LO,
                HE.V_HE,
                BA.V_BA,
                CY.V_CY,
                AL.V_AL,
                CO.V_CO,
                HD.V_HD,
                OT.V_OT,
                sM.V_SM,
                A.ORG_UNIT_NO,
                I.ORG_UNIT_NAME,
                A.CLIENT_NUMBER,
                ECC.DESCRIPTION,
                EJC.DESCRIPTION,
                C.EXPORT_PRODUCT_TYPE_CODE,
                F.DESCRIPTION,
                C.EXPORT_EXEMPTION_REASON_CODE,
                G.DESCRIPTION,
                E.EXPORT_EXEMPTION_TYPE_CODE,
                H.DESCRIPTION,
                A.EXEMPTION_NUMBER
       ORDER BY A.EXPORT_PERMIT_DETAIL_NUMBER ASC;
  END PERMIT_LEDGER_REPORT;

  /******************************************************************************
      PROCEDURE:  PERMIT_REPORT_CSV
      PURPOSE: RETRIEVE A FEE SUMMARY REPORT THAT WILL BE OUTPUTTED AS A CSV.
  ******************************************************************************/
  PROCEDURE PERMIT_REPORT_CSV(P_WHERE_CLAUSE IN VARCHAR,
                              P_PARAMS_ARRAY IN CBR_VARCHAR2_ARRAY,
                              P_NUM_PARAMS   IN NUMBER,
                              P_PERMITS      IN OUT REF_CUR_GENERAL) IS
    V_PERMIT_SELECT VARCHAR2(4000);
  BEGIN

    V_PERMIT_SELECT := 'SELECT A.EXPORT_PERMIT_DETAIL_NUMBER, ' ||
                       'A.ORG_UNIT_NO, ' || 'I.ORG_UNIT_NAME AS REGION, ' ||
                       'A.CLIENT_NUMBER, ' || 'A.EXPORT_COUNTRY_CODE, ' ||
                       'CASE ' ||
                       'WHEN D.EXPORT_SPECIES_CODE = ''FI'' THEN ' ||
                       'SUM(D.SPECIES_GRADE_VOLUME) ' || 'ELSE ' || '0.0 ' ||
                       'END AS V_FI, ' || 'CASE ' ||
                       'WHEN D.EXPORT_SPECIES_CODE = ''CE'' THEN ' ||
                       'SUM(D.SPECIES_GRADE_VOLUME) ' || 'ELSE ' || '0.0 ' ||
                       'END AS V_CE, ' || 'CASE ' ||
                       'WHEN D.EXPORT_SPECIES_CODE = ''SP'' THEN ' ||
                       'SUM(D.SPECIES_GRADE_VOLUME) ' || 'ELSE ' || '0.0 ' ||
                       'END AS V_SP, ' || 'CASE ' ||
                       'WHEN D.EXPORT_SPECIES_CODE = ''LO'' THEN ' ||
                       'SUM(D.SPECIES_GRADE_VOLUME) ' || 'ELSE ' || '0.0 ' ||
                       'END AS V_LO, ' || 'CASE ' ||
                       'WHEN D.EXPORT_SPECIES_CODE = ''HE'' THEN ' ||
                       'SUM(D.SPECIES_GRADE_VOLUME) ' || 'ELSE ' || '0.0 ' ||
                       'END AS V_HE, ' || 'CASE ' ||
                       'WHEN D.EXPORT_SPECIES_CODE = ''BA'' THEN ' ||
                       'SUM(D.SPECIES_GRADE_VOLUME) ' || 'ELSE ' || '0.0 ' ||
                       'END AS V_BA, ' || 'CASE ' ||
                       'WHEN D.EXPORT_SPECIES_CODE = ''CY'' THEN ' ||
                       'SUM(D.SPECIES_GRADE_VOLUME) ' || 'ELSE ' || '0.0 ' ||
                       'END AS V_CY, ' || 'CASE ' ||
                       'WHEN D.EXPORT_SPECIES_CODE = ''AL'' THEN ' ||
                       'SUM(D.SPECIES_GRADE_VOLUME) ' || 'ELSE ' || '0.0 ' ||
                       'END AS V_AL, ' || 'CASE ' ||
                       'WHEN D.EXPORT_SPECIES_CODE = ''CO'' THEN ' ||
                       'SUM(D.SPECIES_GRADE_VOLUME) ' || 'ELSE ' || '0.0 ' ||
                       'END AS V_CO, ' || 'CASE ' ||
                       'WHEN D.EXPORT_SPECIES_CODE = ''AR'' THEN ' ||
                       'SUM(D.SPECIES_GRADE_VOLUME) ' ||
                       'WHEN D.EXPORT_SPECIES_CODE = ''AS'' THEN ' ||
                       'SUM(D.SPECIES_GRADE_VOLUME) ' ||
                       'WHEN D.EXPORT_SPECIES_CODE = ''BI'' THEN ' ||
                       'SUM(D.SPECIES_GRADE_VOLUME) ' ||
                       'WHEN D.EXPORT_SPECIES_CODE = ''MA'' THEN ' ||
                       'SUM(D.SPECIES_GRADE_VOLUME) ' || 'ELSE ' || '0.0 ' ||
                       'END AS V_HD, ' || 'CASE ' ||
                       'WHEN D.EXPORT_SPECIES_CODE = ''LA'' THEN ' ||
                       'SUM(D.SPECIES_GRADE_VOLUME) ' ||
                       'WHEN D.EXPORT_SPECIES_CODE = ''WB'' THEN ' ||
                       'SUM(D.SPECIES_GRADE_VOLUME) ' ||
                       'WHEN D.EXPORT_SPECIES_CODE = ''WH'' THEN ' ||
                       'SUM(D.SPECIES_GRADE_VOLUME) ' ||
                       'WHEN D.EXPORT_SPECIES_CODE = ''YE'' THEN ' ||
                       'SUM(D.SPECIES_GRADE_VOLUME) ' ||
                       'WHEN D.EXPORT_SPECIES_CODE = ''UU'' THEN ' ||
                       'SUM(D.SPECIES_GRADE_VOLUME) ' || 'ELSE ' || '0.0 ' ||
                       'END AS V_OT, ' || 'D.SPECIES_GRADE_VOLUME, ' ||
                       'C.EXPORT_JURISDICTION_CODE, ' ||
                       'C.EXPORT_PRODUCT_TYPE_CODE, ' ||
                       'F.DESCRIPTION AS PRODUCT_TYPE, ' ||
                       'C.EXPORT_EXEMPTION_REASON_CODE, ' ||
                       'G.DESCRIPTION AS REASON, ' ||
                       'E.EXPORT_EXEMPTION_TYPE_CODE, ' ||
                       'H.DESCRIPTION AS EXEMPTION_TYPE, ' ||
                       'A.EXEMPTION_NUMBER, ' || 'D.TIMBER_MARK ' ||
                       'FROM EXPORT_PERMIT_DETAIL A ' ||
                       'INNER JOIN EXPORT_PACKAGE B ON A.EXPORT_PERMIT_DETAIL_NUMBER = ' ||
                       'B.EXPORT_PERMIT_DETAIL_NUMBER ' ||
                       'INNER JOIN EXPORT_EXEMPTION_APPLICATION C ON C.APPLICATION_NUMBER = ' ||
                       'B.APPLICATION_NUMBER ' ||
                       'INNER JOIN EXPORT_SCALE_DETAIL D ON B.PACKAGE_NUMBER = ' ||
                       'D.PACKAGE_NUMBER ' ||
                       'INNER JOIN EXPORT_EXEMPTION E ON C.EXEMPTION_NUMBER = ' ||
                       'E.EXEMPTION_NUMBER ' ||
                       'INNER JOIN EXPORT_PRODUCT_TYPE_CODE F ON C.EXPORT_PRODUCT_TYPE_CODE = ' ||
                       'F.EXPORT_PRODUCT_TYPE_CODE ' ||
                       'INNER JOIN EXPORT_EXEMPTION_REASON_CODE G ON C.EXPORT_EXEMPTION_REASON_CODE = ' ||
                       'G.EXPORT_EXEMPTION_REASON_CODE ' ||
                       'INNER JOIN EXPORT_EXEMPTION_TYPE_CODE H ON H.EXPORT_EXEMPTION_TYPE_CODE = ' ||
                       'E.EXPORT_EXEMPTION_TYPE_CODE ' ||
                       'INNER JOIN ORG_UNIT I ON A.ORG_UNIT_NO = I.ORG_UNIT_NO ';

    LEXIS.FIND_BY(V_PERMIT_SELECT || P_WHERE_CLAUSE ||
                  ' GROUP BY A.EXPORT_PERMIT_DETAIL_NUMBER,
                A.ORG_UNIT_NO,
                I.ORG_UNIT_NAME,
                A.CLIENT_NUMBER,
                A.EXPORT_COUNTRY_CODE,
                D.SPECIES_GRADE_VOLUME,
                D.EXPORT_SPECIES_CODE,
                C.EXPORT_JURISDICTION_CODE,
                C.EXPORT_PRODUCT_TYPE_CODE,
                F.DESCRIPTION,
                C.EXPORT_EXEMPTION_REASON_CODE,
                G.DESCRIPTION,
                E.EXPORT_EXEMPTION_TYPE_CODE,
                H.DESCRIPTION,
                A.EXEMPTION_NUMBER,
                D.TIMBER_MARK ',
                  P_PARAMS_ARRAY,
                  P_NUM_PARAMS,
                  P_PERMITS);
  END PERMIT_REPORT_CSV;

  /******************************************************************************
      PROCEDURE:  PERMIT_REPORT
      PURPOSE: USED BY LEXIS_PERMIT.RPT
  ******************************************************************************/
  PROCEDURE PERMIT_REPORT(P_EXPORT_PERMIT_NUMBER IN EXPORT_PERMIT_DETAIL.EXPORT_PERMIT_DETAIL_NUMBER%TYPE,
                          P_PERMIT_REPORT        IN OUT REF_CUR_GENERAL) AS

  BEGIN
    OPEN P_PERMIT_REPORT FOR
SELECT DISTINCT
            A.EXPORT_PERMIT_DETAIL_NUMBER,
            A.EXPORT_PERMIT_STATUS_CODE,
            H.DESCRIPTION                 AS PERMIT_STATUS,
            A.DESTINATION_COMPANY_NAME,
            ECC.DESCRIPTION AS EXPORT_COUNTRY_CODE,
            A.EXPORT_TRANSPORT_TYPE_CODE,
            F.DESCRIPTION                 AS TRANSPORT,
            A.TRANSPORT_NAME,
            A.ESTIMATED_SHIPPING_DATE,
            A.EXPORT_PORT_OF_EXPORT_CODE,
            CASE WHEN G.DESCRIPTION = 'Other' THEN A.OTHER_PORT_OF_EXPORT ELSE G.DESCRIPTION END AS PORT,
            A.APPLICATION_DATE,
            A.RECEIVED_DATE,
            A.EXPORT_PERMIT_ISSUE_DATE,
            A.EXPIRY_DATE,
            A.RECEIPT_NUMBER,
            C.EXPORT_APPLICANT_TYPE_CODE,

            A.PERMIT_VOLUME,
            A.NUMBER_OF_PIECES,
            A.EXPORT_SCALE_METHOD_CODE,
            A.EXEMPTION_NUMBER,
            A.CLIENT_NUMBER,
            A.CLIENT_LOCN_CODE,
            A.REMARKS,
            --C.EXPORT_GROWTH_TYPE_CODE,
            B.ORG_UNIT_NAME,
            CASE
              WHEN B.ORG_UNIT_NO IN (1909,1910) THEN
                'C'
              ELSE
                'I'
            END as COAST_INTERIOR_IND,
            AGT.CLIENT_NUMBER AS AGENT_CLIENT_NUMBER,

            NVL(D.ADDRESS_1, ' ') AS APP_ADDRESS_1,
            NVL(D.ADDRESS_2, ' ') AS APP_ADDRESS_2,
            NVL(D.ADDRESS_3, ' ') AS APP_ADDRESS_3,
            NVL(D.CITY, ' ') AS APP_CITY,
            NVL(D.PROVINCE, ' ') AS APP_PROVINCE,
            NVL(D.POSTAL_CODE, ' ') AS APP_POSTAL_CODE,

            NVL(OFC.CLIENT_NAME, ' ') AS OWNER_COMPANY_NAME,
            NVL(OL.ADDRESS_1, ' ') AS OWNER_ADDRESS_1,
            NVL(OL.ADDRESS_2, ' ') AS OWNER_ADDRESS_2,
            NVL(OL.ADDRESS_3, ' ') AS OWNER_ADDRESS_3,
            NVL(OL.CITY, ' ') AS OWNER_CITY,
            NVL(OL.PROVINCE, ' ') AS OWNER_PROVINCE,
            NVL(OL.POSTAL_CODE, ' ') AS OWNER_POSTAL_CODE,
            NVL(AFC.CLIENT_NAME, ' ') AS AGT_COMPANY_NAME,
            NVL(AGT.ADDRESS_1, ' ') AS AGT_ADDRESS_1,
            NVL(AGT.ADDRESS_2, ' ') AS AGT_ADDRESS_2,
            NVL(AGT.ADDRESS_3, ' ') AS AGT_ADDRESS_3,
            NVL(AGT.CITY, ' ') AS AGT_CITY,
            NVL(AGT.PROVINCE, ' ') AS AGT_PROVINCE,
            NVL(AGT.POSTAL_CODE, ' ') AS AGT_POSTAL_CODE,

            D.CLIENT_LOCN_NAME,
            D.BUSINESS_PHONE,
            NVL(APP_USER.UPDATE_USERID, ' ') AS ISSUER,
            APP_USER.UPDATE_TIMESTAMP AS COMP_DATETIME

          FROM EXPORT_PERMIT_DETAIL A
          JOIN ORG_UNIT B
            ON B.ORG_UNIT_NO = A.ORG_UNIT_NO
         INNER JOIN CLIENT_LOCATION D
            ON A.CLIENT_NUMBER = D.CLIENT_NUMBER
           AND A.CLIENT_LOCN_CODE = D.CLIENT_LOCN_CODE
         INNER JOIN EXPORT_COUNTRY_CODE ECC
            ON ECC.EXPORT_COUNTRY_CODE = A.EXPORT_COUNTRY_CODE
         INNER JOIN
           (SELECT
                SUM(PIECES_COUNT) AS SUM_PKG_PIECES,
                SUM(SPECIES_GRADE_VOLUME) AS SUM_PKG_VOLUME,
                PACKAGE_NUMBER,
                Export_Permit_Detail_Number
              FROM EXPORT_SCALE_DETAIL
              WHERE Export_Permit_Detail_Number = P_EXPORT_PERMIT_NUMBER
              GROUP BY PACKAGE_NUMBER, Export_Permit_Detail_Number
                ) ESD ON ESD.Export_Permit_Detail_Number = A.EXPORT_PERMIT_DETAIL_NUMBER
          INNER JOIN EXPORT_PACKAGE EP ON EP.Package_Number = ESD.Package_Number
          INNER JOIN EXPORT_EXEMPTION_APPLICATION C ON C.APPLICATION_NUMBER = EP.APPLICATION_NUMBER
         LEFT OUTER JOIN CLIENT_LOCATION OL
            ON C.OWNER_CLIENT_NUMBER = OL.CLIENT_NUMBER
           AND C.OWNER_CLIENT_LOCATION_CODE = OL.CLIENT_LOCN_CODE
         LEFT OUTER JOIN FOREST_CLIENT OFC
            ON OFC.CLIENT_NUMBER = C.OWNER_CLIENT_NUMBER
         LEFT OUTER JOIN CLIENT_LOCATION AGT
            ON (C.AGENT_CLIENT_NUMBER = AGT.CLIENT_NUMBER
           AND C.AGENT_CLIENT_LOCATION_CODE = AGT.CLIENT_LOCN_CODE
           AND C.EXPORT_APPLICANT_TYPE_CODE = 'A'
           AND A.AGENT_NUMBER IS NULL)
           OR (A.AGENT_NUMBER = AGT.CLIENT_NUMBER AND A.AGENT_LOCN_CODE = AGT.CLIENT_LOCN_CODE)
         LEFT OUTER JOIN FOREST_CLIENT AFC
            ON A.AGENT_NUMBER = AFC.CLIENT_NUMBER
         INNER JOIN EXPORT_TRANSPORT_TYPE_CODE F
            ON A.EXPORT_TRANSPORT_TYPE_CODE = F.EXPORT_TRANSPORT_TYPE_CODE
         INNER JOIN EXPORT_PORT_OF_EXPORT_CODE G
            ON A.EXPORT_PORT_OF_EXPORT_CODE = G.EXPORT_PORT_OF_EXPORT_CODE
         INNER JOIN EXPORT_PERMIT_STATUS_CODE H
            ON H.EXPORT_PERMIT_STATUS_CODE = A.EXPORT_PERMIT_STATUS_CODE
         LEFT JOIN (
            SELECT PC.UPDATE_TIMESTAMP, PC.UPDATE_USERID, EXPORT_PERMIT_DETAIL_NUMBER, ROW_NUMBER() OVER( ORDER BY PC.UPDATE_TIMESTAMP ASC) AS ROW_NMBR
            FROM (
                 SELECT UPDATE_TIMESTAMP, UPDATE_USERID, EXPORT_PERMIT_DETAIL_NUMBER
                 FROM EXPORT_PERMIT_DETAIL
                 WHERE EXPORT_PERMIT_DETAIL_NUMBER = P_EXPORT_PERMIT_NUMBER AND EXPORT_PERMIT_STATUS_CODE = 'COM'
                 UNION ALL
                 SELECT UPDATE_TIMESTAMP, UPDATE_USERID, EXPORT_PERMIT_DETAIL_NUMBER
                 FROM EXPORT_PERMIT_AUDIT
                 WHERE EXPORT_PERMIT_DETAIL_NUMBER = P_EXPORT_PERMIT_NUMBER AND EXPORT_PERMIT_STATUS_CODE = 'COM'
                 ) PC
            INNER JOIN
                  (
                  SELECT MAX(UPDATE_TIMESTAMP)AS UPDATE_TIMESTAMP
                  FROM EXPORT_PERMIT_AUDIT
                  WHERE EXPORT_PERMIT_DETAIL_NUMBER = P_EXPORT_PERMIT_NUMBER AND EXPORT_PERMIT_STATUS_CODE = 'ACT'
                  ) PA ON PC.UPDATE_TIMESTAMP > PA.UPDATE_TIMESTAMP
            )APP_USER ON APP_USER.ROW_NMBR = 1 AND APP_USER.EXPORT_PERMIT_DETAIL_NUMBER = A.EXPORT_PERMIT_DETAIL_NUMBER
         WHERE A.EXPORT_PERMIT_DETAIL_NUMBER = P_EXPORT_PERMIT_NUMBER

     GROUP BY
                A.EXPORT_PERMIT_DETAIL_NUMBER,
                --C.APPLICATION_NUMBER,
                A.EXPORT_PERMIT_STATUS_CODE,
                H.DESCRIPTION,
                A.DESTINATION_COMPANY_NAME,
                ECC.DESCRIPTION,
                A.EXPORT_TRANSPORT_TYPE_CODE,
                F.DESCRIPTION,
                A.TRANSPORT_NAME,
                A.ESTIMATED_SHIPPING_DATE,
                A.EXPORT_PORT_OF_EXPORT_CODE,
                CASE WHEN G.DESCRIPTION = 'Other' THEN A.OTHER_PORT_OF_EXPORT ELSE G.DESCRIPTION END,
                A.APPLICATION_DATE,
                A.RECEIVED_DATE,
                A.EXPORT_PERMIT_ISSUE_DATE,
                A.EXPIRY_DATE,
                A.RECEIPT_NUMBER,
                A.EXPORT_SCALE_METHOD_CODE,
                A.EXEMPTION_NUMBER,
                A.CLIENT_NUMBER,
                A.CLIENT_LOCN_CODE,
                A.REMARKS,
                --C.EXPORT_GROWTH_TYPE_CODE,
                B.ORG_UNIT_NAME,
                CASE
                  WHEN B.ORG_UNIT_NO IN (1909,1910) THEN
                    'C'
                  ELSE
                    'I'
                END,
                D.ADDRESS_1,
                D.ADDRESS_2,
                D.ADDRESS_3,
                D.CITY,
                D.PROVINCE,
                D.POSTAL_CODE,

                AGT.CLIENT_NUMBER,
                OFC.CLIENT_NAME,
                OL.ADDRESS_1,
                OL.ADDRESS_2 ,
                OL.ADDRESS_3 ,
                OL.CITY ,
                OL.PROVINCE ,
                OL.POSTAL_CODE ,
                AFC.CLIENT_NAME,
                AGT.ADDRESS_1 ,
                AGT.ADDRESS_2 ,
                AGT.ADDRESS_3 ,
                AGT.CITY ,
                AGT.PROVINCE ,
                AGT.POSTAL_CODE ,

                D.CLIENT_LOCN_NAME,
                D.BUSINESS_PHONE,
                C.EXPORT_APPLICANT_TYPE_CODE,
                A.PERMIT_VOLUME,
                A.NUMBER_OF_PIECES,
                APP_USER.UPDATE_USERID,
                APP_USER.UPDATE_TIMESTAMP;

  END PERMIT_REPORT;

 /******************************************************************************
      PROCEDURE:  PERMIT_REPORT_SUB
      PURPOSE: USED BY LEXIS_PERMIT.RPT
  ******************************************************************************/
  PROCEDURE PERMIT_REPORT_SUB(P_EXPORT_PERMIT_NUMBER IN EXPORT_PERMIT.EXPORT_PERMIT_NMBR%TYPE,
                              P_PERMIT_REPORT_SUB IN OUT REF_CUR_GENERAL) AS
  BEGIN

    OPEN P_PERMIT_REPORT_SUB FOR

    SELECT DISTINCT
      A.EXEMPTION_NUMBER,
      CASE
        WHEN EE.EXPORT_EXEMPTION_TYPE_CODE != 'B' THEN
          A.APPLICATION_NUMBER
        ELSE
          NULL
      END AS APPLICATION_NUMBER,
      CASE
        WHEN EE.EXPORT_EXEMPTION_TYPE_CODE != 'B' THEN
          ESD_1.SPECIES_END_USE
        ELSE
          ESD_2.SPECIES_END_USE
      END AS SPECIES_END_USE,
      EP.PACKAGE_NUMBER,
      PKG_SD.PACKAGE_SPECIES_CODE,
      PTM.PACKAGE_TIMBER_MARK,
      ESD.SUM_PIECES,
      ESD.SUM_VOLUME,
      TMH.TIMBER_MARK_HOLDER,
      CASE
        WHEN EE.EXPORT_EXEMPTION_TYPE_CODE != 'B' THEN
             EGTC.DESCRIPTION
        ELSE
          EGTC2.DESCRIPTION
      END AS AGE_CLASS

    FROM EXPORT_EXEMPTION_APPLICATION A
    INNER JOIN EXPORT_PACKAGE EP ON A.APPLICATION_NUMBER = EP.APPLICATION_NUMBER
    INNER JOIN EXPORT_EXEMPTION EE ON EE.EXEMPTION_NUMBER = A.EXEMPTION_NUMBER
    INNER JOIN
        (
           SELECT DISTINCT
                  PS.PACKAGE_NUMBER,
                  LISTAGG(PS.PACKAGE_SPECIES_CODE, ',,') WITHIN GROUP (ORDER BY PS.PACKAGE_NUMBER)
                  OVER (PARTITION BY PS.PACKAGE_NUMBER) AS PACKAGE_SPECIES_CODE
           FROM (
              SELECT DISTINCT
                S.PACKAGE_NUMBER,
               S.EXPORT_SPECIES_CODE || ' - ' || LISTAGG(S.EXPORT_GRADE_CODE, ',') WITHIN GROUP(ORDER BY S.EXPORT_SPECIES_CODE, S.PACKAGE_NUMBER)
                OVER (PARTITION BY S.EXPORT_SPECIES_CODE, S.PACKAGE_NUMBER) AS PACKAGE_SPECIES_CODE
              FROM (
              SELECT DISTINCT
                     ESC.DESCRIPTION AS EXPORT_SPECIES_CODE,
                     ESD.EXPORT_GRADE_CODE,
                     ESD.PACKAGE_NUMBER
              FROM EXPORT_SCALE_DETAIL ESD
              INNER JOIN EXPORT_SPECIES_CODE ESC ON ESC.EXPORT_SPECIES_CODE = ESD.EXPORT_SPECIES_CODE
                   AND ESD.Export_Permit_Detail_Number = P_EXPORT_PERMIT_NUMBER
                   ORDER BY ESD.PACKAGE_NUMBER, ESC.DESCRIPTION, ESD.EXPORT_GRADE_CODE
                   ) S
               )PS
           )PKG_SD ON PKG_SD.PACKAGE_NUMBER = EP.PACKAGE_NUMBER
    LEFT OUTER JOIN
        (SELECT
           LISTAGG(ESD.TIMBER_MARK, ', ')
                 WITHIN GROUP (ORDER BY ESD.TIMBER_MARK)
                 OVER (PARTITION BY ESD.PACKAGE_NUMBER) AS PACKAGE_TIMBER_MARK,
           ESD.PACKAGE_NUMBER
         FROM EXPORT_SCALE_DETAIL ESD
         WHERE ESD.Export_Permit_Detail_Number = P_EXPORT_PERMIT_NUMBER)  PTM ON PTM.PACKAGE_NUMBER = EP.PACKAGE_NUMBER
    LEFT OUTER JOIN
        (SELECT SUM(PIECES_COUNT) AS SUM_PIECES, SUM(SPECIES_GRADE_VOLUME) AS SUM_VOLUME,  PACKAGE_NUMBER
        FROM EXPORT_SCALE_DETAIL ESD
        WHERE ESD.Export_Permit_Detail_Number = P_EXPORT_PERMIT_NUMBER
        GROUP BY PACKAGE_NUMBER) ESD ON ESD.PACKAGE_NUMBER = EP.PACKAGE_NUMBER
    LEFT OUTER JOIN
         (
          SELECT DISTINCT
            LISTAGG( TT.TIMBER_MARK, ',, ') WITHIN GROUP (ORDER BY TT.PACKAGE_NUMBER)
            OVER (PARTITION BY TT.PACKAGE_NUMBER) AS TIMBER_MARK_HOLDER,
            TT.PACKAGE_NUMBER,
            TT.APPLICATION_NUMBER
          FROM (
                SELECT DISTINCT
                      FC.CLIENT_NAME,
                      ESD.PACKAGE_NUMBER,
                     NVL(FC.CLIENT_NAME, 'Unknown') || ' :- ' || LISTAGG( ESD.TIMBER_MARK, ', ') WITHIN GROUP (ORDER BY FC.CLIENT_NAME, ESD.TIMBER_MARK)
                     OVER (PARTITION BY FC.CLIENT_NAME, ESD.PACKAGE_NUMBER, IEP.APPLICATION_NUMBER) AS TIMBER_MARK,
                    IEP.APPLICATION_NUMBER
                FROM EXPORT_SCALE_DETAIL ESD

                INNER JOIN EXPORT_PACKAGE IEP
                      ON IEP.PACKAGE_NUMBER = ESD.PACKAGE_NUMBER AND ESD.Export_Permit_Detail_Number = P_EXPORT_PERMIT_NUMBER

                INNER JOIN HAULING_AUTHORITY HA
                      ON ESD.TIMBER_MARK = HA.TIMBER_MARK

                LEFT JOIN HARVESTING_HAULING_XREF X
                      ON HA.TIMBER_MARK = X.TIMBER_MARK

                LEFT JOIN HARVESTING_AUTHORITY HVA
                      ON X.HVA_SKEY = HVA.HVA_SKEY

                LEFT JOIN FOR_CLIENT_LINK FCL
                      ON HVA.FOREST_FILE_ID = FCL.FOREST_FILE_ID
                      AND (FCL.FILE_CLIENT_TYPE = 'A' OR
                      FCL.FILE_CLIENT_TYPE = 'S' AND
                      HVA.CUTTING_PERMIT_ID = FCL.CUTTING_PERMIT_ID)

                LEFT JOIN FOREST_CLIENT FC
                      ON FCL.CLIENT_NUMBER = fc.CLIENT_NUMBER

                 ORDER BY APPLICATION_NUMBER, PACKAGE_NUMBER
                ) TT
          ) TMH ON TMH.PACKAGE_NUMBER = EP.PACKAGE_NUMBER AND A.APPLICATION_NUMBER = TMH.APPLICATION_NUMBER
          LEFT OUTER JOIN EXPORT_GROWTH_TYPE_CODE EGTC ON EGTC.EXPORT_GROWTH_TYPE_CODE = A.EXPORT_GROWTH_TYPE_CODE
          LEFT OUTER JOIN EXPORT_GROWTH_TYPE_CODE EGTC2 ON EGTC2.EXPORT_GROWTH_TYPE_CODE = EP.EXPORT_GROWTH_TYPE_CODE
          LEFT OUTER JOIN
            (SELECT
               LISTAGG(ESC.EXPORT_SPECIES_CODE, '/')
                     WITHIN GROUP (ORDER BY ESC.EXPORT_SPECIES_CODE)
                     OVER (PARTITION BY EEASPE.APPLICATION_NUMBER) || '/' || EEUS.DESCRIPTION AS SPECIES_END_USE,
                     EEASPE.APPLICATION_NUMBER
             FROM EXPORT_EXMPTN_APPL_SPCS_ENDUSE EEASPE
               INNER JOIN EXPORT_SPECIES_CODE ESC
                  ON EEASPE.EXPORT_SPECIES_CODE = ESC.EXPORT_SPECIES_CODE
               INNER JOIN EXPORT_END_USE_CODE EEUS
                  ON EEASPE.EXPORT_END_USE_CODE = EEUS.EXPORT_END_USE_CODE
                  WHERE EEASPE.PACKAGE_NUMBER IS NULL
             ) ESD_1 ON ESD_1.APPLICATION_NUMBER = A.APPLICATION_NUMBER
          LEFT OUTER JOIN
            (SELECT
               LISTAGG(ESC.EXPORT_SPECIES_CODE, '/')
                     WITHIN GROUP (ORDER BY ESC.EXPORT_SPECIES_CODE)
                     OVER (PARTITION BY EEASPE.PACKAGE_NUMBER) || '/' || EEUS.DESCRIPTION AS SPECIES_END_USE,
                     EEASPE.PACKAGE_NUMBER
             FROM EXPORT_EXMPTN_APPL_SPCS_ENDUSE EEASPE
               INNER JOIN EXPORT_SPECIES_CODE ESC
                  ON EEASPE.EXPORT_SPECIES_CODE = ESC.EXPORT_SPECIES_CODE
               INNER JOIN EXPORT_END_USE_CODE EEUS
                  ON EEASPE.EXPORT_END_USE_CODE = EEUS.EXPORT_END_USE_CODE
                  WHERE EEASPE.APPLICATION_NUMBER IS NULL
             ) ESD_2 ON ESD_2.PACKAGE_NUMBER = EP.PACKAGE_NUMBER

ORDER BY APPLICATION_NUMBER, PACKAGE_NUMBER;

  END PERMIT_REPORT_SUB;

  /******************************************************************************
      PROCEDURE:  BIWEEKLY_RPT
      PURPOSE: USED BY LEXIS_BIWEEKLY.RPT
  ******************************************************************************/
  PROCEDURE BIWEEKLY_RPT(P_ORG_UNIT     VARCHAR2,
                         P_JURISDICTION VARCHAR2,
                         P_FROM_DATE    VARCHAR2,
                         P_TO_DATE      VARCHAR2,
                         P_BIWEEKLY     IN OUT REF_CUR_GENERAL) AS

    V_FROM_DATE     DATE := TO_DATE('0001-01-01', 'YYYY-MM-DD');
    V_TO_DATE       DATE := TO_DATE('9999-01-01', 'YYYY-MM-DD');
    V_FED_JURIS     VARCHAR2(1) := 'F';
    V_PROV_JURIS    VARCHAR2(1) := 'P';

  BEGIN

    IF P_FROM_DATE IS NOT NULL THEN
      V_FROM_DATE := TO_DATE(P_FROM_DATE, 'YYYY-MM-DD');
    END IF;

    IF P_TO_DATE IS NOT NULL THEN
      V_TO_DATE := TO_DATE(P_TO_DATE, 'YYYY-MM-DD');
    END IF;

    IF P_JURISDICTION = 'P' THEN
      V_FED_JURIS := '%';
    ELSIF P_JURISDICTION = 'F' THEN
      V_PROV_JURIS := '%';
    END IF;

    OPEN P_BIWEEKLY FOR
      SELECT A.EXPORT_JURISDICTION_CODE,
             EJC.DESCRIPTION AS JURISDICTION_CODE,
             A.APPLICATION_NUMBER,
             A.FED_APPLICATION_NUMBER,
             A.EXPORT_APPLICATION_STATUS_CODE,
             F.DESCRIPTION AS APPLICATION_STATUS,
             A.AGENT_CLIENT_NUMBER,
             A.AGENT_CLIENT_LOCATION_CODE,
             A.AGENT_CONTACT_NAME,
             A.OWNER_CLIENT_NUMBER,
             A.OWNER_CLIENT_LOCATION_CODE,
             A.OWNER_CONTACT_NAME,
             RETRIEVE_SPECIES_ENDUSE(A.APPLICATION_NUMBER) AS SPECIES_ENDUSE,
             A.PRODUCT_LOCATION,
             A.EXPORT_PRODUCT_TYPE_CODE,
             G.DESCRIPTION AS PRODUCT_TYPE,
             A.EXEMPTION_APPLICATION_VOLUME,
             A.AVERAGE_LOG_VOLUME,
             A.ORG_UNIT_NO,
             H.ORG_UNIT_NAME AS ORG_UNIT,
             E.ADVERTISING_DATE,
             B.CLIENT_NAME,
             B.CLIENT_TYPE_CODE,
             C.ADDRESS_1,
             C.ADDRESS_2,
             C.ADDRESS_3,
             C.CITY,
             C.PROVINCE,
             C.POSTAL_CODE,
             C.BUSINESS_PHONE,
             C.HOME_PHONE,
             C.FAX_NUMBER,
             C.EMAIL_ADDRESS,
             FCA.CLIENT_NAME as AGENT_CLIENT_NAME,
             CLA.ADDRESS_1 as AGENT_ADDRESS_1,
             CLA.ADDRESS_2 as AGENT_ADDRESS_2,
             CLA.ADDRESS_3 as AGENT_ADDRESS_3,
             CLA.CITY as AGENT_CITY,
             CLA.PROVINCE as AGENT_PROV,
             CLA.POSTAL_CODE as AGENT_POSTAL,
             CLA.BUSINESS_PHONE as AGENT_BUS_PHONE,
             CLA.HOME_PHONE as AGENT_HOME_PHONE,
             CLA.FAX_NUMBER as AGENT_FAX,
             CLA.EMAIL_ADDRESS as AGENT_EMAIL
        FROM EXPORT_EXEMPTION_APPLICATION A
       INNER JOIN FOREST_CLIENT B
          ON A.OWNER_CLIENT_NUMBER = B.CLIENT_NUMBER
       INNER JOIN CLIENT_LOCATION C
          ON A.OWNER_CLIENT_NUMBER = C.CLIENT_NUMBER
         AND A.OWNER_CLIENT_LOCATION_CODE = C.CLIENT_LOCN_CODE
        LEFT JOIN FOREST_CLIENT FCA
          ON A.AGENT_CLIENT_NUMBER = FCA.CLIENT_NUMBER
        LEFT JOIN CLIENT_LOCATION CLA
          ON A.AGENT_CLIENT_NUMBER = CLA.CLIENT_NUMBER
         AND A.AGENT_CLIENT_LOCATION_CODE = CLA.CLIENT_LOCN_CODE
       INNER JOIN EXPORT_SCHEDULE E
          ON E.EXPORT_SCHEDULE_ID = A.EXPORT_SCHEDULE_ID
       INNER JOIN EXPORT_APPLICATION_STATUS_CODE F
          ON F.EXPORT_APPLICATION_STATUS_CODE =
             A.EXPORT_APPLICATION_STATUS_CODE
       INNER JOIN EXPORT_PRODUCT_TYPE_CODE G
          ON G.EXPORT_PRODUCT_TYPE_CODE = A.EXPORT_PRODUCT_TYPE_CODE
       INNER JOIN ORG_UNIT H
          ON H.ORG_UNIT_NO = A.ORG_UNIT_NO
       INNER JOIN EXPORT_JURISDICTION_CODE EJC
          ON A.EXPORT_JURISDICTION_CODE = EJC.EXPORT_JURISDICTION_CODE
       WHERE E.ADVERTISING_DATE BETWEEN V_FROM_DATE AND V_TO_DATE
         AND A.EXPORT_JURISDICTION_CODE IN (V_FED_JURIS, V_PROV_JURIS)
         AND (A.EXPORT_APPLICATION_STATUS_CODE = 'APP')
         AND (P_ORG_UNIT like '%' || A.ORG_UNIT_NO || '%' OR P_ORG_UNIT IS NULL)
         AND NOT A.EXPORT_PRODUCT_TYPE_CODE = 'T'
       ORDER BY A.ORG_UNIT_NO,
                A.EXPORT_JURISDICTION_CODE,
                B.CLIENT_NAME,
                A.AGENT_CONTACT_NAME,
                A.APPLICATION_NUMBER;
  END BIWEEKLY_RPT;

  /******************************************************************************
      PROCEDURE:  BIWEEKLY_SUBREPORT_RPT
      PURPOSE: USED BY LEXIS_BIWEEKLY.RPT'S SUB REPORT
  ******************************************************************************/
  PROCEDURE BIWEEKLY_SUBREPORT_RPT(P_APPLICATION_NUMBER EXPORT_EXEMPTION_APPLICATION.APPLICATION_NUMBER%TYPE,
                                   P_JURISDICTION       EXPORT_EXEMPTION_APPLICATION.EXPORT_JURISDICTION_CODE%TYPE,
                                   P_BIWEEKLY_PACKAGE   IN OUT REF_CUR_GENERAL) AS
  BEGIN
    OPEN P_BIWEEKLY_PACKAGE FOR
      SELECT A.PACKAGE_NUMBER,
             A.PACKAGE_VOLUME,
             B.EXPORT_GROWTH_TYPE_CODE,
             GTC.DESCRIPTION,
             A.AVERAGE_LENGTH,
             A.AVERAGE_DIAMETER
        FROM EXPORT_PACKAGE A
       INNER JOIN EXPORT_EXEMPTION_APPLICATION B
          ON A.APPLICATION_NUMBER = B.APPLICATION_NUMBER
       INNER JOIN EXPORT_GROWTH_TYPE_CODE GTC
          ON B.EXPORT_GROWTH_TYPE_CODE = GTC.EXPORT_GROWTH_TYPE_CODE
       WHERE B.EXPORT_JURISDICTION_CODE = P_JURISDICTION
         AND B.APPLICATION_NUMBER = P_APPLICATION_NUMBER;

  END BIWEEKLY_SUBREPORT_RPT;

  /******************************************************************************
      PROCEDURE:  BIWEEKLY_REPORT_CSV
      PURPOSE: RETRIEVE A BIWEEKLY REPORT THAT WILL BE OUTPUTTED AS A CSV.
  ******************************************************************************/
  PROCEDURE BIWEEKLY_REPORT_CSV(P_WHERE_CLAUSE IN VARCHAR,
                                P_PARAMS_ARRAY IN CBR_VARCHAR2_ARRAY,
                                P_NUM_PARAMS   IN NUMBER,
                                P_BIWEEKLY     IN OUT REF_CUR_GENERAL) IS
    V_BIWEEKLY_SELECT VARCHAR2(4000);
  BEGIN

    V_BIWEEKLY_SELECT := 'SELECT ES.ADVERTISING_DATE as "Notification Date", ' ||
                         'OU.ORG_UNIT_NAME as "Region", ' ||
                         'FC.CLIENT_NAME as "Client Name", ' ||
                         'CL.ADDRESS_1 as "Client Address 1", ' ||
                         'CL.ADDRESS_2 as "Client Address 2", ' ||
                         'CL.ADDRESS_3 as "Client Address 3", ' ||
                         'CL.CITY as "Client City", ' ||
                         'CL.PROVINCE as "Client Province", ' ||
                         'CL.POSTAL_CODE as "Client Postal Code", ' ||
                         'EEA.Owner_Contact_Name as "Client Contact Name", ' ||
                         'CL.BUSINESS_PHONE as "Client Contact Phone", ' ||
                         'EEA.EXPORT_JURISDICTION_CODE as "Jursidiction Code", ' ||
                         'NVL(EEA.FED_APPLICATION_NUMBER, EEA.APPLICATION_NUMBER) as "Application Number", ' ||
                         'LEXIS_REPORTING.RETRIEVE_SPECIES_ENDUSE(EEA.APPLICATION_NUMBER) AS "Species Enduse", ' ||
                         'EPTC.DESCRIPTION AS "Product Type", ' ||
                         'EEA.PRODUCT_LOCATION as "Product Location", ' ||
                         'EEA.EXEMPTION_APPLICATION_VOLUME as "Exemption Application Volume", ' ||
                         'EEA.AVERAGE_LOG_VOLUME as "Average Log Volume", ' ||
                         'AFC.CLIENT_NAME as "Agent Name", ' ||
                         'ACL.Business_Phone as "Agent Phone", ' ||
                         'EEA.Agent_Contact_Name as "Agent Contact Name", ' ||
                         'EP.PACKAGE_NUMBER as "Package Number", ' ||
                         'EP.PACKAGE_VOLUME as "Package Volume", ' ||
                         'EEA.Export_Growth_Type_Code as "Age Class", ' ||
                         'EP.AVERAGE_LENGTH as "Average Length", ' ||
                         'EP.AVERAGE_DIAMETER as "Average Diameter" ' ||
                         'FROM EXPORT_EXEMPTION_APPLICATION EEA ' ||
                         'LEFT OUTER JOIN EXPORT_PACKAGE EP ' ||
                         '    ON EEA.APPLICATION_NUMBER = EP.APPLICATION_NUMBER ' ||
                         ' INNER JOIN FOREST_CLIENT FC ' ||
                         '    ON EEA.OWNER_CLIENT_NUMBER = FC.CLIENT_NUMBER ' ||
                         'INNER JOIN CLIENT_LOCATION CL ' ||
                         '    ON EEA.OWNER_CLIENT_NUMBER = CL.CLIENT_NUMBER ' ||
                         '   AND EEA.OWNER_CLIENT_LOCATION_CODE = CL.CLIENT_LOCN_CODE ' ||
                         '  LEFT JOIN FOREST_CLIENT AFC ' ||
                         '    ON EEA.AGENT_CLIENT_NUMBER = AFC.CLIENT_NUMBER ' ||
                         '  LEFT JOIN CLIENT_LOCATION ACL ' ||
                         '    ON EEA.AGENT_CLIENT_NUMBER = ACL.CLIENT_NUMBER ' ||
                         '   AND EEA.AGENT_CLIENT_LOCATION_CODE = ACL.CLIENT_LOCN_CODE ' ||
                         ' INNER JOIN EXPORT_SCHEDULE ES ' ||
                         '    ON ES.EXPORT_SCHEDULE_ID = EEA.EXPORT_SCHEDULE_ID ' ||
                         ' INNER JOIN EXPORT_PRODUCT_TYPE_CODE EPTC ' ||
                         '    ON EPTC.EXPORT_PRODUCT_TYPE_CODE = EEA.EXPORT_PRODUCT_TYPE_CODE ' ||
                         ' INNER JOIN ORG_UNIT OU ' ||
                         '    ON OU.ORG_UNIT_NO = EEA.ORG_UNIT_NO ';
    LEXIS.FIND_BY(V_BIWEEKLY_SELECT || P_WHERE_CLAUSE,
                  P_PARAMS_ARRAY,
                  P_NUM_PARAMS,
                  P_BIWEEKLY);
  END BIWEEKLY_REPORT_CSV;

  /******************************************************************************
      PROCEDURE:  APPLICATION_RPT
      PURPOSE: USED BY LEXIS_APPLICATION.RPT
  ******************************************************************************/
  PROCEDURE APPLICATION_RPT(P_ORG_UNIT         ORG_UNIT.ORG_UNIT_NO%TYPE,
                            P_JURISDICTION     VARCHAR2,
                            P_EXEMPTION_REASON VARCHAR2,
                            P_RECEIVED_FROM    VARCHAR2,
                            P_RECEIVED_TO      VARCHAR2,
                            P_CLIENT_NUMBER    VARCHAR2,
                            P_GROWTH_TYPE      VARCHAR2,
                            P_APPLICATION      IN OUT REF_CUR_GENERAL) AS

    V_DATE_RECEIVED_FROM DATE := TO_DATE('0001-01-01', 'YYYY-MM-DD');
    V_DATE_RECEIVED_TO   DATE := TO_DATE('9999-12-31', 'YYYY-MM-DD');
    V_ORG_UNIT_FROM      NUMBER := P_ORG_UNIT;
    V_ORG_UNIT_TO        NUMBER := P_ORG_UNIT;

    V_EXEMPTION_REASON VARCHAR2(2) := '%';
    V_CLIENT_NUMBER    VARCHAR2(8) := '%';
    V_GROWTH_TYPE      VARCHAR2(2) := '%';
    V_FED_JURIS        VARCHAR2(1) := 'F';
    V_PROV_JURIS       VARCHAR2(1) := 'P';

  BEGIN

    IF P_ORG_UNIT = 0 THEN
      V_ORG_UNIT_FROM := 1903;
      V_ORG_UNIT_TO   := 1910;
    END IF;

    IF P_RECEIVED_FROM IS NOT NULL THEN
      V_DATE_RECEIVED_FROM := TO_DATE(P_RECEIVED_FROM, 'YYYY-MM-DD');
    END IF;

    IF P_RECEIVED_TO IS NOT NULL THEN
      V_DATE_RECEIVED_TO := TO_DATE(P_RECEIVED_TO, 'YYYY-MM-DD');
    END IF;

    IF P_JURISDICTION = 'P' THEN
      V_FED_JURIS := '%';
    ELSIF P_JURISDICTION = 'F' THEN
      V_PROV_JURIS := '%';
    END IF;

    IF P_EXEMPTION_REASON IS NOT NULL THEN
      V_EXEMPTION_REASON := P_EXEMPTION_REASON;
    END IF;

    IF P_CLIENT_NUMBER IS NOT NULL THEN
      V_CLIENT_NUMBER := P_CLIENT_NUMBER;
    END IF;

    IF P_GROWTH_TYPE IS NOT NULL THEN
      V_GROWTH_TYPE := P_GROWTH_TYPE;
    END IF;

    OPEN P_APPLICATION FOR
      SELECT EEA.ORG_UNIT_NO,
             EEA.EXPORT_EXEMPTION_REASON_CODE,
             EEA.EXPORT_JURISDICTION_CODE,
             EEA.APPLICATION_NUMBER,
             EEA.FED_APPLICATION_NUMBER,
             EEA.EXPORT_APPLICATION_STATUS_CODE,
             EEA.OWNER_CLIENT_NUMBER,
             FC.CLIENT_NAME,
             FC.LEGAL_FIRST_NAME,
             FC.LEGAL_MIDDLE_NAME,
             FC.CLIENT_TYPE_CODE,
             LEXIS_REPORTING.RETRIEVE_SPECIES_ENDUSE(EEA.APPLICATION_NUMBER),
             EEA.EXEMPTION_APPLICATION_VOLUME,
             ES.ADVERTISING_DATE,
             EE.EXPORT_EXEMPTION_TYPE_CODE,
             EE.APPROVED_VOLUME,
             EEA.EXEMPTION_NUMBER,
             EEA.EXPORT_GROWTH_TYPE_CODE,
             (SELECT COUNT(EEAR.EXPORT_EXMPTN_APPL_REMARK_NMBR)
                FROM EXPORT_EXEMPTION_APP_REMARKS EEAR
               WHERE EEAR.APPLICATION_NUMBER = EEA.APPLICATION_NUMBER) AS REMARK_COUNT,
             LEXIS_REPORTING.APPLICATION_HAS_OFFERS(EEA.APPLICATION_NUMBER) AS HAS_OFFERS
        FROM EXPORT_EXEMPTION_APPLICATION EEA
       INNER JOIN FOREST_CLIENT FC
          ON EEA.OWNER_CLIENT_NUMBER = FC.CLIENT_NUMBER
        LEFT JOIN EXPORT_EXEMPTION EE
          ON EEA.EXEMPTION_NUMBER = EE.EXEMPTION_NUMBER
       INNER JOIN EXPORT_SCHEDULE ES
          ON ES.EXPORT_SCHEDULE_ID = EEA.EXPORT_SCHEDULE_ID
       WHERE EEA.Application_Date BETWEEN V_DATE_RECEIVED_FROM AND
             V_DATE_RECEIVED_TO
         AND EEA.ORG_UNIT_NO BETWEEN V_ORG_UNIT_FROM AND V_ORG_UNIT_TO
         AND EEA.OWNER_CLIENT_NUMBER LIKE V_CLIENT_NUMBER
         AND EEA.EXPORT_JURISDICTION_CODE IN (V_FED_JURIS, V_PROV_JURIS)
         AND NOT EEA.EXPORT_JURISDICTION_CODE = 'I'
         AND EEA.EXPORT_EXEMPTION_REASON_CODE LIKE V_EXEMPTION_REASON
         AND EEA.EXPORT_GROWTH_TYPE_CODE LIKE V_GROWTH_TYPE
         order by eea.application_number asc;
  END APPLICATION_RPT;

  /******************************************************************************
      PROCEDURE:  APP_SUBREPORT_RPT
      PURPOSE: USED BY LEXIS_APPLICATION_LEDGER.RPT'S SUB REPORT
  ******************************************************************************/
  PROCEDURE APP_SUBREPORT_RPT(P_APPLICATION_NUMBER EXPORT_EXEMPTION_APPLICATION.APPLICATION_NUMBER%TYPE,
                              P_REMARKS            IN OUT REF_CUR_GENERAL) AS
  BEGIN
    OPEN P_REMARKS FOR
      SELECT EEAR.REMARK
        FROM EXPORT_EXEMPTION_APP_REMARKS EEAR
       WHERE EEAR.APPLICATION_NUMBER = P_APPLICATION_NUMBER;

  END APP_SUBREPORT_RPT;

  /******************************************************************************
      FUNCTION:  GET_APP_REMARK_AT_ROW
      PURPOSE: Gets a remark from a specifc row in the table
  ******************************************************************************/
  FUNCTION GET_APP_REMARK_AT_ROW(P_APPLICATION_NUMBER IN EXPORT_EXEMPTION_APPLICATION.APPLICATION_NUMBER%TYPE,
                                 P_ROWNUMBER          IN NUMBER)
    RETURN VARCHAR2 IS

    C_REMARK VARCHAR(256);

    CURSOR C1 IS

      SELECT REMARK
        FROM (SELECT /*+ FIRST_ROWS(1) */
               A.*, ROWNUM RNUM
                FROM (SELECT EEAR.REMARK
                        FROM EXPORT_EXEMPTION_APP_REMARKS EEAR
                       WHERE EEAR.APPLICATION_NUMBER = P_APPLICATION_NUMBER) A
               WHERE ROWNUM <= P_ROWNUMBER)
       WHERE RNUM >= P_ROWNUMBER;

  BEGIN

    OPEN C1;
    FETCH C1
      INTO C_REMARK;
    CLOSE C1;

    RETURN C_REMARK;

  END GET_APP_REMARK_AT_ROW;

  /******************************************************************************
      PROCEDURE:  APP_REPORT_CSV
      PURPOSE: RETRIEVE A BIWEEKLY REPORT THAT WILL BE OUTPUTTED AS A CSV.
  ******************************************************************************/
  PROCEDURE APP_REPORT_CSV(P_WHERE_CLAUSE IN VARCHAR,
                           P_PARAMS_ARRAY IN CBR_VARCHAR2_ARRAY,
                           P_NUM_PARAMS   IN NUMBER,
                           P_BIWEEKLY     IN OUT REF_CUR_GENERAL) IS
    V_APP_SELECT VARCHAR2(4000);
  BEGIN

    V_APP_SELECT := 'SELECT EEA.ORG_UNIT_NO, ' ||
                    'EEA.EXPORT_EXEMPTION_REASON_CODE, ' ||
                    'EEA.EXPORT_JURISDICTION_CODE, ' ||
                    'NVL(EEA.FED_APPLICATION_NUMBER, EEA.APPLICATION_NUMBER) AS APPLICATION_NUMBER, ' ||
                    'EEA.EXPORT_APPLICATION_STATUS_CODE, ' ||
                    'EEA.OWNER_CLIENT_NUMBER, ' || 'FC.CLIENT_NAME, ' ||
                    'FC.LEGAL_FIRST_NAME, ' || 'FC.LEGAL_MIDDLE_NAME, ' ||
                    'FC.CLIENT_TYPE_CODE, ' ||
                    'LEXIS_REPORTING.RETRIEVE_SPECIES_ENDUSE(EEA.APPLICATION_NUMBER) AS SPECIES_ENDUSE, ' ||
                    'EEA.EXEMPTION_APPLICATION_VOLUME, ' ||
                    'ES.ADVERTISING_DATE, ' ||
                    'EE.EXPORT_EXEMPTION_TYPE_CODE, ' ||
                    'EE.APPROVED_VOLUME, ' || 'EEA.EXEMPTION_NUMBER, ' ||
                    'EEA.EXPORT_GROWTH_TYPE_CODE, ' ||
                    '(SELECT COUNT(EEAR.EXPORT_EXMPTN_APPL_REMARK_NMBR) ' ||
                    'FROM EXPORT_EXEMPTION_APP_REMARKS EEAR ' ||
                    'WHERE EEAR.APPLICATION_NUMBER = EEA.APPLICATION_NUMBER) AS REMARK_COUNT, ' ||
                    'LEXIS_REPORTING.GET_APP_REMARK_AT_ROW(EEA.APPLICATION_NUMBER, 1) AS REMARK_1, ' ||
                    'LEXIS_REPORTING.GET_APP_REMARK_AT_ROW(EEA.APPLICATION_NUMBER, 2) AS REMARK_2, ' ||
                    'LEXIS_REPORTING.GET_APP_REMARK_AT_ROW(EEA.APPLICATION_NUMBER, 3) AS REMARK_3, ' ||
                    'LEXIS_REPORTING.GET_APP_REMARK_AT_ROW(EEA.APPLICATION_NUMBER, 4) AS REMARK_4, ' ||
                    'LEXIS_REPORTING.GET_APP_REMARK_AT_ROW(EEA.APPLICATION_NUMBER, 5) AS REMARK_5, ' ||
                    'LEXIS_REPORTING.GET_APP_REMARK_AT_ROW(EEA.APPLICATION_NUMBER, 6) AS REMARK_6, ' ||
                    'LEXIS_REPORTING.GET_APP_REMARK_AT_ROW(EEA.APPLICATION_NUMBER, 7) AS REMARK_7, ' ||
                    'LEXIS_REPORTING.GET_APP_REMARK_AT_ROW(EEA.APPLICATION_NUMBER, 8) AS REMARK_8, ' ||
                    'LEXIS_REPORTING.GET_APP_REMARK_AT_ROW(EEA.APPLICATION_NUMBER, 9) AS REMARK_9, ' ||
                    'LEXIS_REPORTING.GET_APP_REMARK_AT_ROW(EEA.APPLICATION_NUMBER, 10) AS REMARK_10, ' ||
                    'LEXIS_REPORTING.GET_APP_REMARK_AT_ROW(EEA.APPLICATION_NUMBER, 11) AS REMARK_11, ' ||
                    'LEXIS_REPORTING.GET_APP_REMARK_AT_ROW(EEA.APPLICATION_NUMBER, 12) AS REMARK_12, ' ||
                    'LEXIS_REPORTING.GET_APP_REMARK_AT_ROW(EEA.APPLICATION_NUMBER, 13) AS REMARK_13, ' ||
                    'LEXIS_REPORTING.GET_APP_REMARK_AT_ROW(EEA.APPLICATION_NUMBER, 14) AS REMARK_14, ' ||
                    'LEXIS_REPORTING.GET_APP_REMARK_AT_ROW(EEA.APPLICATION_NUMBER, 15) AS REMARK_15 ' ||
                    'FROM EXPORT_EXEMPTION_APPLICATION EEA ' ||
                    'INNER JOIN FOREST_CLIENT FC ON EEA.OWNER_CLIENT_NUMBER = FC.CLIENT_NUMBER ' ||
                    'LEFT JOIN EXPORT_EXEMPTION EE ON EEA.EXEMPTION_NUMBER = EE.EXEMPTION_NUMBER ' ||
                    'INNER JOIN EXPORT_SCHEDULE ES ON ES.EXPORT_SCHEDULE_ID = EEA.EXPORT_SCHEDULE_ID ';

    LEXIS.FIND_BY(V_APP_SELECT || P_WHERE_CLAUSE,
                  P_PARAMS_ARRAY,
                  P_NUM_PARAMS,
                  P_BIWEEKLY);
  END APP_REPORT_CSV;

  /******************************************************************************
      PROCEDURE:  EXEMPTION_RPT
      PURPOSE: USED BY LEXIS_EXEMPTION.RPT
  ******************************************************************************/
  PROCEDURE EXEMPTION_RPT(P_EXEMPTION_NUMBER VARCHAR2,
                          P_EXEMPTION        IN OUT REF_CUR_GENERAL) IS

  BEGIN
    OPEN P_EXEMPTION FOR
      SELECT DISTINCT A.EXEMPTION_NUMBER,
                      RETRIEVE_SPECIES_ENDUSE(A.APPLICATION_NUMBER) AS SPECIES_ENDUSE,
                      A.EXEMPTION_APPLICATION_VOLUME,
                      A.APPLICATION_NUMBER,
                      D.EXPIRY_DATE,
                      C.CLIENT_NUMBER,
                      C.CLIENT_LOCN_NAME,
                      A.ORG_UNIT_NO,
                      E.ORG_UNIT_NAME,
                      C.ADDRESS_1 || ' ' || C.ADDRESS_2 || ' ' ||
                      C.ADDRESS_3 AS ADDRESS,
                      C.CITY,
                      C.PROVINCE,
                      C.POSTAL_CODE
        FROM EXPORT_EXEMPTION_APPLICATION A
       INNER JOIN EXPORT_EXMPTN_APPL_SPCS_ENDUSE B
          ON A.APPLICATION_NUMBER = B.APPLICATION_NUMBER
       INNER JOIN CLIENT_LOCATION C
          ON C.CLIENT_NUMBER = A.OWNER_CLIENT_NUMBER
         AND C.CLIENT_LOCN_CODE = A.OWNER_CLIENT_LOCATION_CODE
       INNER JOIN EXPORT_EXEMPTION D
          ON A.EXEMPTION_NUMBER = D.EXEMPTION_NUMBER
       INNER JOIN ORG_UNIT E
          ON E.ORG_UNIT_NO = A.ORG_UNIT_NO
       WHERE D.EXEMPTION_NUMBER = P_EXEMPTION_NUMBER
         AND D.EXPORT_EXEMPTION_STATUS_CODE = 'ACT';
  END EXEMPTION_RPT;

  /******************************************************************************
      PROCEDURE:  OFFERS_LEDGER_RPT
      PURPOSE: USED BY LEXIS_OFFERS_LEDGER.RPT
  ******************************************************************************/
  PROCEDURE OFFERS_LEDGER_RPT(P_APPLICATION_DATE_FROM VARCHAR2,
                              P_APPLICATION_DATE_TO   VARCHAR2,
                              P_ORG_UNIT              VARCHAR2,
                              P_CLIENT_NUMBER         EXPORT_EXEMPTION_APPLICATION.OWNER_CLIENT_NUMBER%TYPE,
                              P_WITHDRAWN_DATE_FROM   VARCHAR2,
                              P_WITHDRAWN_DATE_TO     VARCHAR2,
                              P_JURISDICTION          EXPORT_EXEMPTION_APPLICATION.EXPORT_JURISDICTION_CODE%TYPE,
                              P_OFFERS                IN OUT REF_CUR_GENERAL) IS

    V_APPLICATION_FROM_DATE DATE := TO_DATE('0001-01-01', 'YYYY-MM-DD');
    V_APPLICATION_TO_DATE   DATE := TO_DATE('9999-12-31', 'YYYY-MM-DD');

    V_JURISDICTION VARCHAR2(2) := '%';

    V_CLIENT_NUMBER VARCHAR2(10) := P_CLIENT_NUMBER;
  BEGIN

    IF P_APPLICATION_DATE_FROM IS NOT NULL THEN
      V_APPLICATION_FROM_DATE := TO_DATE(P_APPLICATION_DATE_FROM,
                                         'YYYY-MM-DD');
    END IF;

    IF P_APPLICATION_DATE_TO IS NOT NULL THEN
      V_APPLICATION_TO_DATE := TO_DATE(P_APPLICATION_DATE_TO, 'YYYY-MM-DD');
    END IF;

    IF V_CLIENT_NUMBER IS NULL THEN
      V_CLIENT_NUMBER := '%';
    END IF;

    IF P_JURISDICTION IS NOT NULL THEN
      V_JURISDICTION := P_JURISDICTION;
    END IF;
  IF (P_WITHDRAWN_DATE_FROM IS NOT NULL AND P_WITHDRAWN_DATE_TO IS NOT NULL ) THEN
    OPEN P_OFFERS FOR
      SELECT O.ORG_UNIT_CODE,
             O.ORG_UNIT_NAME,
             EEA.EXPORT_JURISDICTION_CODE || ' - ' || C.DESCRIPTION AS EXPORT_JURISDICTION_DESC,
             EEA.APPLICATION_NUMBER,
             EEA.FED_APPLICATION_NUMBER,
             EEA.EXPORT_JURISDICTION_CODE,
             EEA.EXPORT_APPLICATION_STATUS_CODE,
             G.DESCRIPTION AS APPLICATION_STATUS,
             S.ADVERTISING_DATE,
             PO.EXPORT_PURCHASE_OFFER_NUMBER,
             PO.COMPANY_NAME,
             PO.OFFER_WITHDRAWAL_DATE,
             PO.TEAC_REVIEW_DATE,
             PO.FAIR_OFFER_INDICATOR,
             PO.VALID_OFFER_INDICATOR,
             PO.OFFER_REMARK,
             F.CLIENT_TYPE_CODE,
             F.CLIENT_NAME,
             F.LEGAL_FIRST_NAME,
             F.LEGAL_MIDDLE_NAME,
             PO.PURCHASE_OFFER_AMOUNT,
             EEA.PRODUCT_LOCATION,
             EEA.Owner_Client_Number,
             RETRIEVE_SPECIES_ENDUSE(EEA.APPLICATION_NUMBER) AS SPECIES_GRADE,
             PO.PICKUP_LOCATION
        FROM EXPORT_PURCHASE_OFFER PO
       INNER JOIN EXPORT_EXEMPTION_APPLICATION EEA
          ON EEA.APPLICATION_NUMBER = PO.APPLICATION_NUMBER
       LEFT JOIN EXPORT_PACKAGE EP
          ON EP.PACKAGE_NUMBER = PO.PACKAGE_NUMBER
       INNER JOIN EXPORT_SCHEDULE S
          ON EEA.EXPORT_SCHEDULE_ID = S.EXPORT_SCHEDULE_ID
       INNER JOIN ORG_UNIT O
          ON EEA.ORG_UNIT_NO = O.ORG_UNIT_NO
       INNER JOIN EXPORT_JURISDICTION_CODE C
          ON EEA.EXPORT_JURISDICTION_CODE = C.EXPORT_JURISDICTION_CODE
       INNER JOIN FOREST_CLIENT F
          ON EEA.OWNER_CLIENT_NUMBER = F.CLIENT_NUMBER
       INNER JOIN EXPORT_APPLICATION_STATUS_CODE G
          ON G.EXPORT_APPLICATION_STATUS_CODE =
             EEA.EXPORT_APPLICATION_STATUS_CODE
       WHERE EEA.APPLICATION_DATE BETWEEN V_APPLICATION_FROM_DATE AND
             V_APPLICATION_TO_DATE
         AND PO.OFFER_WITHDRAWAL_DATE BETWEEN  TO_DATE(P_WITHDRAWN_DATE_FROM, 'YYYY-MM-DD') AND TO_DATE(P_WITHDRAWN_DATE_TO, 'YYYY-MM-DD')
         AND P_ORG_UNIT LIKE '%' || EEA.ORG_UNIT_NO || '%'
         AND EEA.OWNER_CLIENT_NUMBER LIKE V_CLIENT_NUMBER
         AND EEA.EXPORT_JURISDICTION_CODE LIKE V_JURISDICTION
       ORDER BY EEA.ORG_UNIT_NO,
                EEA.EXPORT_JURISDICTION_CODE,
                EEA.APPLICATION_NUMBER,
                S.ADVERTISING_DATE,
                PO.EXPORT_PURCHASE_OFFER_NUMBER;
      ELSE
            OPEN P_OFFERS FOR
      SELECT O.ORG_UNIT_CODE,
             O.ORG_UNIT_NAME,
             EEA.EXPORT_JURISDICTION_CODE || ' - ' || C.DESCRIPTION AS EXPORT_JURISDICTION_DESC,
             EEA.APPLICATION_NUMBER,
             EEA.FED_APPLICATION_NUMBER,
             EEA.EXPORT_JURISDICTION_CODE,
             EEA.EXPORT_APPLICATION_STATUS_CODE,
             G.DESCRIPTION AS APPLICATION_STATUS,
             S.ADVERTISING_DATE,
             PO.EXPORT_PURCHASE_OFFER_NUMBER,
             PO.COMPANY_NAME,
             PO.OFFER_WITHDRAWAL_DATE,
             PO.TEAC_REVIEW_DATE,
             PO.FAIR_OFFER_INDICATOR,
             PO.VALID_OFFER_INDICATOR,
             PO.OFFER_REMARK,
             F.CLIENT_TYPE_CODE,
             F.CLIENT_NAME,
             F.LEGAL_FIRST_NAME,
             F.LEGAL_MIDDLE_NAME,
             PO.PURCHASE_OFFER_AMOUNT,
             EEA.PRODUCT_LOCATION,
             EEA.Owner_Client_Number,
             RETRIEVE_SPECIES_ENDUSE(EEA.APPLICATION_NUMBER) AS SPECIES_GRADE,
             PO.PICKUP_LOCATION
        FROM EXPORT_PURCHASE_OFFER PO
       INNER JOIN EXPORT_EXEMPTION_APPLICATION EEA
          ON EEA.APPLICATION_NUMBER = PO.APPLICATION_NUMBER
       LEFT JOIN EXPORT_PACKAGE EP
          ON EP.PACKAGE_NUMBER = PO.PACKAGE_NUMBER
       INNER JOIN EXPORT_SCHEDULE S
          ON EEA.EXPORT_SCHEDULE_ID = S.EXPORT_SCHEDULE_ID
       INNER JOIN ORG_UNIT O
          ON EEA.ORG_UNIT_NO = O.ORG_UNIT_NO
       INNER JOIN EXPORT_JURISDICTION_CODE C
          ON EEA.EXPORT_JURISDICTION_CODE = C.EXPORT_JURISDICTION_CODE
       INNER JOIN FOREST_CLIENT F
          ON EEA.OWNER_CLIENT_NUMBER = F.CLIENT_NUMBER
       INNER JOIN EXPORT_APPLICATION_STATUS_CODE G
          ON G.EXPORT_APPLICATION_STATUS_CODE =
             EEA.EXPORT_APPLICATION_STATUS_CODE
       WHERE EEA.APPLICATION_DATE BETWEEN V_APPLICATION_FROM_DATE AND
             V_APPLICATION_TO_DATE
         AND P_ORG_UNIT LIKE '%' || EEA.ORG_UNIT_NO || '%'
         AND EEA.OWNER_CLIENT_NUMBER LIKE V_CLIENT_NUMBER
         AND EEA.EXPORT_JURISDICTION_CODE LIKE V_JURISDICTION
       ORDER BY EEA.ORG_UNIT_NO,
                EEA.EXPORT_JURISDICTION_CODE,
                EEA.APPLICATION_NUMBER,
                S.ADVERTISING_DATE,
                PO.EXPORT_PURCHASE_OFFER_NUMBER;

      END IF;
  END OFFERS_LEDGER_RPT;

  /******************************************************************************
      PROCEDURE:  OFFERS_REPORT_CSV
      PURPOSE: RETRIEVE A BIWEEKLY REPORT THAT WILL BE OUTPUTTED AS A CSV.
  ******************************************************************************/
  PROCEDURE OFFERS_REPORT_CSV(P_WHERE_CLAUSE IN VARCHAR,
                              P_PARAMS_ARRAY IN CBR_VARCHAR2_ARRAY,
                              P_NUM_PARAMS   IN NUMBER,
                              P_OFFERS       IN OUT REF_CUR_GENERAL) IS
    V_OFFERS_SELECT VARCHAR2(4000);
  BEGIN

    V_OFFERS_SELECT := 'SELECT O.ORG_UNIT_CODE, ' || 'O.ORG_UNIT_NAME, ' ||
                       'EEA.EXPORT_JURISDICTION_CODE || '' - '' || C.DESCRIPTION AS EXPORT_JURISDICTION_DESC, ' ||
                       'EEA.EXPORT_JURISDICTION_CODE || NVL(EEA.FED_APPLICATION_NUMBER, EEA.APPLICATION_NUMBER) as APPLICATION_NUMBER,  ' ||
                       'EEA.EXPORT_APPLICATION_STATUS_CODE, ' ||
                       'G.DESCRIPTION AS APPLICATION_STATUS, ' ||
                       'S.ADVERTISING_DATE, ' ||
                       'PO.EXPORT_PURCHASE_OFFER_NUMBER, ' ||
                       'PO.COMPANY_NAME, ' || 'PO.OFFER_WITHDRAWAL_DATE, ' ||
                       'PO.TEAC_REVIEW_DATE, ' ||
                       'PO.FAIR_OFFER_INDICATOR, ' ||
                       'PO.VALID_OFFER_INDICATOR, ' || 'PO.OFFER_REMARK, ' ||
                       'F.CLIENT_TYPE_CODE, ' || 'F.CLIENT_NAME, ' ||
                       'F.LEGAL_FIRST_NAME, ' || 'F.LEGAL_MIDDLE_NAME, ' ||
                       'PO.PURCHASE_OFFER_AMOUNT, ' ||
                       'EEA.PRODUCT_LOCATION, ' ||
                       'EEA.Owner_Client_Number, ' ||
                       'LEXIS_REPORTING.RETRIEVE_SPECIES_ENDUSE(EEA.APPLICATION_NUMBER) AS SPECIES_GRADE, ' ||
                       'PO.PICKUP_LOCATION ' ||
                       'FROM EXPORT_PURCHASE_OFFER PO ' ||
                       'INNER JOIN EXPORT_EXEMPTION_APPLICATION EEA ON EEA.APPLICATION_NUMBER = PO.APPLICATION_NUMBER ' ||
                       'LEFT JOIN EXPORT_PACKAGE EP ON EP.PACKAGE_NUMBER = PO.PACKAGE_NUMBER ' ||
                       'INNER JOIN EXPORT_SCHEDULE S ON EEA.EXPORT_SCHEDULE_ID = S.EXPORT_SCHEDULE_ID ' ||
                       'INNER JOIN ORG_UNIT O ON EEA.ORG_UNIT_NO = O.ORG_UNIT_NO ' ||
                       'INNER JOIN EXPORT_JURISDICTION_CODE C ON EEA.EXPORT_JURISDICTION_CODE = C.EXPORT_JURISDICTION_CODE ' ||
                       'INNER JOIN FOREST_CLIENT F ON EEA.OWNER_CLIENT_NUMBER = F.CLIENT_NUMBER ' ||
                       'INNER JOIN EXPORT_APPLICATION_STATUS_CODE G ON G.EXPORT_APPLICATION_STATUS_CODE = EEA.EXPORT_APPLICATION_STATUS_CODE ';

    LEXIS.FIND_BY(V_OFFERS_SELECT || P_WHERE_CLAUSE,
                  P_PARAMS_ARRAY,
                  P_NUM_PARAMS,
                  P_OFFERS);
  END OFFERS_REPORT_CSV;

  /******************************************************************************
      PROCEDURE:  SPECIES_GRADE_RPT
      PURPOSE: USED BY LEXIS_SPECIES_GRADE.RPT
      Note: I know this is a crazy query, but it's actually intentional since it helps with
      formatting the data for both the crystal reports report and the csv report.
  ******************************************************************************/
  PROCEDURE SPECIES_GRADE_RPT(P_DATE_FROM        VARCHAR2,
                              P_DATE_TO          VARCHAR2,
                              P_ORG_UNIT         VARCHAR2,
                              P_EXEMPTION_NUMBER EXPORT_EXEMPTION_APPLICATION.OWNER_CLIENT_NUMBER%TYPE,
                              P_EXEMPTION_TYPE   EXPORT_EXEMPTION.EXPORT_EXEMPTION_TYPE_CODE%TYPE,
                              P_EXEMPTION_REASON EXPORT_EXEMPTION_APPLICATION.EXPORT_EXEMPTION_REASON_CODE%TYPE,
                              P_GROWTH_TYPE      EXPORT_EXEMPTION_APPLICATION.EXPORT_GROWTH_TYPE_CODE%TYPE,
                              P_TIMBER_MARK      EXPORT_SCALE_DETAIL.TIMBER_MARK%TYPE,
                              P_FOREST_FILE_ID   HAULING_AUTHORITY.FOREST_FILE_ID%TYPE,
                              P_PERMIT_STATUS    EXPORT_PERMIT_DETAIL.EXPORT_PERMIT_STATUS_CODE%TYPE,
                              P_SPECIES_GRADE    IN OUT REF_CUR_GENERAL) AS

    -- variables used in the initial data selection
    V_DATE_FROM     DATE := TO_DATE('0001-01-01', 'YYYY-MM-DD');
    V_DATE_TO       DATE := TO_DATE('9999-12-31', 'YYYY-MM-DD');

    V_EXEMPTION_NUMBER VARCHAR2(12) := P_EXEMPTION_NUMBER;
    V_EXEMPTION_TYPE   VARCHAR2(5) := '%';
    V_EXEMPTION_REASON VARCHAR2(5) := '%';
    V_GROWTH_TYPE      VARCHAR2(5) := '%';
    V_TIMBER_MARK      VARCHAR2(12) := '%';
    V_PERMIT_STATUS    VARCHAR2(5) := '%';

  BEGIN

    IF P_EXEMPTION_NUMBER IS NULL THEN
      V_EXEMPTION_NUMBER := '%';
    END IF;

    IF P_DATE_FROM IS NOT NULL THEN
      V_DATE_FROM := TO_DATE(P_DATE_FROM, 'YYYY-MM-DD');
    END IF;

    IF P_DATE_TO IS NOT NULL THEN
      V_DATE_TO := TO_DATE(P_DATE_TO, 'YYYY-MM-DD');
    END IF;

    IF P_EXEMPTION_TYPE IS NOT NULL THEN
      V_EXEMPTION_TYPE := P_EXEMPTION_TYPE;
    END IF;

    IF P_EXEMPTION_REASON IS NOT NULL THEN
      V_EXEMPTION_REASON := P_EXEMPTION_REASON;
    END IF;

    IF P_GROWTH_TYPE IS NOT NULL THEN
      V_GROWTH_TYPE := P_GROWTH_TYPE;
    END IF;

    IF P_TIMBER_MARK IS NOT NULL THEN
      V_TIMBER_MARK := P_TIMBER_MARK;
    END IF;

    IF P_PERMIT_STATUS IS NOT NULL THEN
      V_PERMIT_STATUS := P_PERMIT_STATUS;
    END IF;

    open p_species_grade for
SELECT C.EXPORT_PRODUCT_TYPE_CODE,
       F.DESCRIPTION AS PRODUCT_TYPE,
       i.org_unit_name AS region,
       d.export_grade_code,
       sum(case
             when FI.SUM_FI is null then
              0
             else
              fi.sum_fi
           end) as SUM_FI,
       sum(case
             when ce.SUM_ce is null then
              0
             else
              CE.SUM_CE
           end) as SUM_CE,
       sum(case
             when sp.SUM_sp is null then
              0
             else
              SP.SUM_SP
           end) as SUM_SP,
       sum(case
             when lo.SUM_lo is null then
              0
             else
              LO.SUM_LO
           end) as SUM_LO,
       sum(case
             when he.SUM_he is null then
              0
             else
              HE.SUM_HE
           end) as SUM_HE,
       sum(case
             when ba.SUM_ba is null then
              0
             else
              BA.SUM_BA
           end) as SUM_BA,
       sum(case
             when cy.SUM_cy is null then
              0
             else
              CY.SUM_CY
           end) as SUM_CY,
       sum(case
             when al.SUM_al is null then
              0
             else
              AL.SUM_AL
           end) as SUM_AL,
       sum(case
             when co.SUM_co is null then
              0
             else
              CO.SUM_CO
           end) as SUM_CO,
       sum(case
             when hd.SUM_hd is null then
              0
             else
              HD.SUM_HD
           end) as SUM_HD,
       sum(case
             when ot.SUM_ot is null then
              0
             else
              OT.SUM_OT
           end) as SUM_OT,
       sum(case
             when sm.SUM_sm is null then
              0
             else
              sm.SUM_SM
           end),
       EJC.DESCRIPTION AS EXPORT_JURISDICTION_CODE,
       case -- we want to sort by letter grades followed by number grades followed by the blank grade
         when export_grade_code = regexp_substr(export_grade_code, '[1-9]') then
          regexp_substr(export_grade_code, '[1-9]') + 1
         when export_grade_code = ' ' then
          10
         else
          1
       end as sorted_column
  FROM EXPORT_PERMIT_DETAIL A
 INNER JOIN EXPORT_SCALE_DETAIL D
    ON A.EXPORT_PERMIT_DETAIL_NUMBER = D.EXPORT_PERMIT_DETAIL_NUMBER
 INNER JOIN EXPORT_EXEMPTION E
    ON A.EXEMPTION_NUMBER = E.EXEMPTION_NUMBER
 INNER JOIN Export_Exemption_Application C
    on C.Exemption_Number = E.Exemption_Number
 INNER JOIN EXPORT_PRODUCT_TYPE_CODE F
    ON C.EXPORT_PRODUCT_TYPE_CODE = F.EXPORT_PRODUCT_TYPE_CODE
 INNER JOIN EXPORT_EXEMPTION_REASON_CODE G
    ON C.EXPORT_EXEMPTION_REASON_CODE = G.EXPORT_EXEMPTION_REASON_CODE
 INNER JOIN EXPORT_EXEMPTION_TYPE_CODE H
    ON H.EXPORT_EXEMPTION_TYPE_CODE = E.EXPORT_EXEMPTION_TYPE_CODE
 INNER JOIN ORG_UNIT I
    ON A.ORG_UNIT_NO = I.ORG_UNIT_NO
 INNER JOIN EXPORT_JURISDICTION_CODE EJC
    ON C.EXPORT_JURISDICTION_CODE = EJC.EXPORT_JURISDICTION_CODE
 INNER JOIN EXPORT_COUNTRY_CODE ECC
    ON A.EXPORT_COUNTRY_CODE = ECC.EXPORT_COUNTRY_CODE
  LEFT OUTER JOIN (select esd_fi.export_grade_code as grade_code,
                          esd_fi.export_scale_detail_id as scale_id,
                          sum(esd_fi.species_grade_volume) as SUM_FI
                     from export_scale_detail esd_fi
                    inner join export_package fi_pack
                       on esd_fi.package_number = fi_pack.package_number
                    inner join export_permit_detail epd
                       on esd_fi.export_permit_detail_number =
                          epd.export_permit_detail_number
                    where esd_fi.export_species_code = 'FI' and epd.EXPORT_PERMIT_ISSUE_DATE BETWEEN V_DATE_FROM AND V_DATE_TO
                    group by esd_fi.export_scale_detail_id,
                             esd_fi.export_grade_code) FI
    on FI.scale_id = d.export_scale_detail_id
   and FI.grade_code = d.export_grade_code
  LEFT OUTER JOIN (select esd_ce.export_grade_code as grade_code,
                          esd_ce.export_scale_detail_id as scale_id,
                          sum(esd_ce.species_grade_volume) as SUM_ce
                     from export_scale_detail esd_ce
                    inner join export_package ce_pack
                       on esd_ce.package_number = ce_pack.package_number
                    inner join export_permit_detail epd on esd_ce.export_permit_detail_number = epd.export_permit_detail_number
                    where esd_ce.export_species_code = 'CE'  and epd.EXPORT_PERMIT_ISSUE_DATE BETWEEN V_DATE_FROM AND V_DATE_TO
                    group by esd_CE.export_scale_detail_id,
                             esd_CE.export_grade_code) CE
    on CE.scale_id = d.export_scale_detail_id
   and CE.grade_code = d.export_grade_code
  LEFT OUTER JOIN (select esd_SP.export_grade_code as grade_code,
                          esd_SP.export_scale_detail_id as scale_id,
                          sum(esd_SP.species_grade_volume) as SUM_SP
                     from export_scale_detail esd_SP
                    inner join export_package SP_pack
                       on esd_SP.package_number = SP_pack.package_number
                       inner join export_permit_detail epd on esd_sp.export_permit_detail_number = epd.export_permit_detail_number
                    where esd_SP.export_species_code = 'SP'  and epd.EXPORT_PERMIT_ISSUE_DATE BETWEEN V_DATE_FROM AND V_DATE_TO
                    group by esd_SP.export_scale_detail_id,
                             esd_SP.export_grade_code) SP
    on SP.scale_id = d.export_scale_detail_id
   and SP.grade_code = d.export_grade_code
  LEFT OUTER JOIN (select esd_LO.export_grade_code as grade_code,
                          esd_LO.export_scale_detail_id as scale_id,
                          sum(esd_LO.species_grade_volume) as SUM_LO
                     from export_scale_detail esd_LO
                    inner join export_package LO_pack
                       on esd_LO.package_number = LO_pack.package_number
                       inner join export_permit_detail epd on esd_lo.export_permit_detail_number = epd.export_permit_detail_number
                    where esd_LO.export_species_code = 'LO'  and epd.EXPORT_PERMIT_ISSUE_DATE BETWEEN V_DATE_FROM AND V_DATE_TO
                    group by esd_LO.export_scale_detail_id,
                             esd_LO.export_grade_code) LO
    on LO.scale_id = d.export_scale_detail_id
   and LO.grade_code = d.export_grade_code
  LEFT OUTER JOIN (select esd_He.export_grade_code as grade_code,
                          esd_HE.export_scale_detail_id as scale_id,
                          sum(esd_HE.species_grade_volume) as SUM_HE
                     from export_scale_detail esd_HE
                    inner join export_package HE_pack
                       on esd_HE.package_number = HE_pack.package_number
                       inner join export_permit_detail epd on esd_he.export_permit_detail_number = epd.export_permit_detail_number
                    where esd_HE.export_species_code = 'HE'  and epd.EXPORT_PERMIT_ISSUE_DATE BETWEEN V_DATE_FROM AND V_DATE_TO
                    group by esd_HE.export_scale_detail_id,
                             esd_HE.export_grade_code) HE
    on HE.scale_id = d.export_scale_detail_id
   and HE.grade_code = d.export_grade_code
  LEFT OUTER JOIN (select esd_BA.export_grade_code as grade_code,
                          esd_BA.export_scale_detail_id as scale_id,
                          sum(esd_BA.species_grade_volume) as SUM_BA
                     from export_scale_detail esd_BA
                    inner join export_package BA_pack
                       on esd_BA.package_number = BA_pack.package_number
                       inner join export_permit_detail epd on esd_ba.export_permit_detail_number = epd.export_permit_detail_number
                    where esd_BA.export_species_code = 'BA'  and epd.EXPORT_PERMIT_ISSUE_DATE BETWEEN V_DATE_FROM AND V_DATE_TO
                    group by esd_BA.export_scale_detail_id,
                             esd_BA.export_grade_code) BA
    on BA.scale_id = d.export_scale_detail_id
   and BA.grade_code = d.export_grade_code
  LEFT OUTER JOIN (select esd_CY.export_grade_code as grade_code,
                          esd_CY.export_scale_detail_id as scale_id,
                          sum(esd_CY.species_grade_volume) as SUM_CY
                     from export_scale_detail esd_CY
                    inner join export_package CY_pack
                       on esd_CY.package_number = CY_pack.package_number
                       inner join export_permit_detail epd on esd_cy.export_permit_detail_number = epd.export_permit_detail_number
                    where esd_CY.export_species_code = 'CY'  and epd.EXPORT_PERMIT_ISSUE_DATE BETWEEN V_DATE_FROM AND V_DATE_TO
                    group by esd_CY.export_scale_detail_id,
                             esd_CY.export_grade_code) CY
    on CY.scale_id = d.export_scale_detail_id
   and CY.grade_code = d.export_grade_code
  LEFT OUTER JOIN (select esd_LA.export_grade_code as grade_code,
                          esd_LA.export_scale_detail_id as scale_id,
                          sum(esd_LA.species_grade_volume) as SUM_LA
                     from export_scale_detail esd_LA
                    inner join export_package LA_pack
                       on esd_LA.package_number = la_pack.package_number
                       inner join export_permit_detail epd on esd_la.export_permit_detail_number = epd.export_permit_detail_number
                    where esd_LA.export_species_code = 'LA'  and epd.EXPORT_PERMIT_ISSUE_DATE BETWEEN V_DATE_FROM AND V_DATE_TO
                    group by esd_LA.export_scale_detail_id,
                             esd_LA.export_grade_code) LA
    on LA.scale_id = d.export_scale_detail_id
   and LA.grade_code = d.export_grade_code
  LEFT OUTER JOIN (select esd_al.export_grade_code as grade_code,
                          esd_al.export_scale_detail_id as scale_id,
                          sum(esd_al.species_grade_volume) as SUM_al
                     from export_scale_detail esd_al
                    inner join export_package al_pack
                       on esd_al.package_number = al_pack.package_number
                       inner join export_permit_detail epd on esd_al.export_permit_detail_number = epd.export_permit_detail_number
                    where esd_AL.export_species_code = 'AL'  and epd.EXPORT_PERMIT_ISSUE_DATE BETWEEN V_DATE_FROM AND V_DATE_TO
                    group by esd_AL.export_scale_detail_id,
                             esd_AL.export_grade_code) AL
    on AL.scale_id = d.export_scale_detail_id
   and AL.grade_code = d.export_grade_code
  LEFT OUTER JOIN (select esd_CO.export_grade_code as grade_code,
                          esd_CO.export_scale_detail_id as scale_id,
                          sum(esd_CO.species_grade_volume) as SUM_CO
                     from export_scale_detail esd_CO
                    inner join export_package CO_pack
                       on esd_CO.package_number = CO_pack.package_number
                       inner join export_permit_detail epd on esd_co.export_permit_detail_number = epd.export_permit_detail_number
                    where esd_CO.export_species_code = 'CO'  and epd.EXPORT_PERMIT_ISSUE_DATE BETWEEN V_DATE_FROM AND V_DATE_TO
                    group by esd_CO.export_scale_detail_id,
                             esd_CO.export_grade_code) CO
    on CO.scale_id = d.export_scale_detail_id
   and CO.grade_code = d.export_grade_code
  LEFT OUTER JOIN (select esd_HD.export_grade_code as grade_code,
                          esd_HD.export_scale_detail_id as scale_id,
                          sum(esd_HD.species_grade_volume) as SUM_HD
                     from export_scale_detail esd_HD
                    inner join export_package HD_pack
                       on esd_HD.package_number = HD_pack.package_number
                       inner join export_permit_detail epd on esd_hd.export_permit_detail_number = epd.export_permit_detail_number
                    where esd_HD.export_species_code IN
                          ('AR', 'AS', 'BI', 'MA')  and epd.EXPORT_PERMIT_ISSUE_DATE BETWEEN V_DATE_FROM AND V_DATE_TO
                    group by esd_HD.export_scale_detail_id,
                             esd_HD.export_grade_code) HD
    on HD.scale_id = d.export_scale_detail_id
   and HD.grade_code = d.export_grade_code
  LEFT OUTER JOIN (select esd_SM.export_grade_code as grade_code,
                          esd_SM.export_scale_detail_id as scale_id,
                          sum(esd_SM.species_grade_volume) as SUM_SM
                     from export_scale_detail esd_SM
                    inner join export_package SM_pack
                       on esd_SM.package_number = SM_pack.package_number
                       inner join export_permit_detail epd on esd_sm.export_permit_detail_number = epd.export_permit_detail_number
                    where esd_SM.export_species_code = 'SM'  and epd.EXPORT_PERMIT_ISSUE_DATE BETWEEN V_DATE_FROM AND V_DATE_TO
                    group by esd_SM.export_scale_detail_id,
                             esd_SM.export_grade_code) SM
    on SM.scale_id = d.export_scale_detail_id
   and SM.grade_code = d.export_grade_code
  LEFT OUTER JOIN (select esd_OT.export_grade_code as grade_code,
                          esd_OT.export_scale_detail_id as scale_id,
                          sum(esd_OT.species_grade_volume) as SUM_OT
                     from export_scale_detail esd_OT
                    inner join export_package OT_pack
                       on esd_OT.package_number = OT_pack.package_number
                       inner join export_permit_detail epd on esd_ot.export_permit_detail_number = epd.export_permit_detail_number
                    where esd_OT.export_species_code in
                          ('LA', 'WB', 'WH', 'YE', 'UU')  and epd.EXPORT_PERMIT_ISSUE_DATE BETWEEN V_DATE_FROM AND V_DATE_TO
                    group by esd_OT.export_scale_detail_id,
                             esd_OT.export_grade_code) OT
    on OT.scale_id = d.export_scale_detail_id
   and OT.grade_code = d.export_grade_code

 WHERE A.EXPORT_PERMIT_ISSUE_DATE BETWEEN
       V_DATE_FROM AND
       V_DATE_TO
   and c.export_jurisdiction_code = 'P'
   AND A.EXPORT_PERMIT_STATUS_CODE LIKE V_PERMIT_STATUS
   AND P_ORG_UNIT like '%' || A.ORG_UNIT_NO || '%'
   AND E.EXEMPTION_NUMBER LIKE V_EXEMPTION_NUMBER
   AND E.EXPORT_EXEMPTION_TYPE_CODE LIKE V_EXEMPTION_TYPE
   AND C.EXPORT_EXEMPTION_REASON_CODE LIKE V_EXEMPTION_REASON
   AND C.EXPORT_GROWTH_TYPE_CODE LIKE V_GROWTH_TYPE
   AND (P_TIMBER_MARK IS NULL OR D.TIMBER_MARK LIKE V_TIMBER_MARK)

 GROUP BY C.EXPORT_PRODUCT_TYPE_CODE,
          F.DESCRIPTION,
          i.org_unit_name,
          d.export_grade_code,
          EJC.DESCRIPTION
 ORDER BY i.org_unit_name,
          c.export_product_type_code asc,
          SORTED_COLUMN,
          d.export_grade_code;


  END SPECIES_GRADE_RPT;


  /******************************************************************************
      PROCEDURE:  SPECIES_GRADE_CSV
      PURPOSE: RETRIEVE A SPECIES GRADE REPORT THAT WILL BE OUTPUTTED AS A CSV.
      TODO: We're not using this as designed as this query can't be executed in the typical way.
      Typically, CSV reports make use of dynamic queries (hence the procedure signature).
      However, this doesn't work in all cases (i.e. inner selects with where clauses).
      The best option would be to get rid of the CSV procedures and use the existing
      report procedures to generate CSV reports.  This would reduce the amound of code, and any
      changes required to the report would only need to be done in one place instead of two.
      Until then, I'm just using the P_PARAMS_ARRAY to determine how to populate the where clauses.
      This is annoying given that the array indexes need to be determined via the java code, but
      we this fix is time sensitive.
  ******************************************************************************/
  PROCEDURE SPECIES_GRADE_REPORT_CSV(P_DATE_FROM          IN EXPORT_PERMIT_DETAIL.EXPORT_PERMIT_ISSUE_DATE%TYPE,
                                     P_DATE_TO            IN EXPORT_PERMIT_DETAIL.EXPORT_PERMIT_ISSUE_DATE%TYPE,
                                     P_ORG_UNIT_NO        IN VARCHAR2,
                                     P_PERMIT_STATUS      IN EXPORT_PERMIT_DETAIL.EXPORT_PERMIT_STATUS_CODE%TYPE,
                                     P_EXEMPTION_NUMBER   IN EXPORT_PERMIT_DETAIL.Exemption_Number%TYPE,
                                     P_EXEMPTION_TYPE     IN EXPORT_EXEMPTION.Export_Exemption_Type_Code%TYPE,
                                     P_EXEMPTION_REASON   IN EXPORT_EXEMPTION_REASON_CODE.EXPORT_EXEMPTION_REASON_CODE%TYPE,
                                     P_GROWTH_TYPE        IN EXPORT_GROWTH_TYPE_CODE.EXPORT_GROWTH_TYPE_CODE%TYPE,
                                     P_TIMBER_MARK        IN HAULING_AUTHORITY.Timber_Mark%TYPE,
                                     P_FOREST_FILE_ID     IN HAULING_AUTHORITY.FOREST_FILE_ID%TYPE,
                                     P_SPECIES_GRADE      IN OUT REF_CUR_GENERAL) IS
    V_DATE_FROM     DATE := TO_DATE('01/01/0001', 'MM/DD/YYYY');
    V_DATE_TO     DATE := TO_DATE('12/31/9999', 'MM/DD/YYYY');


    -- variables used in the initial data selection
    V_PERMIT_STATUS    VARCHAR2(5) := '%';
    V_EXEMPTION_NUMBER VARCHAR2(12) := '%';
    V_EXEMPTION_TYPE   VARCHAR2(5) := '%';
    V_EXEMPTION_REASON VARCHAR2(5) := '%';
    V_GROWTH_TYPE      VARCHAR2(5) := '%';
    V_TIMBER_MARK      VARCHAR2(12) := '%';
    V_FOREST_FILE_ID   VARCHAR2(12) := '%';


  BEGIN
      IF P_DATE_FROM IS NOT NULL THEN
      V_DATE_FROM := P_DATE_FROM;
    END IF;

    IF P_DATE_FROM IS NOT NULL THEN
      V_DATE_TO := P_DATE_TO;
    END IF;

    IF P_PERMIT_STATUS IS NOT NULL THEN
      V_PERMIT_STATUS := P_PERMIT_STATUS;
    END IF;

    IF P_EXEMPTION_NUMBER IS NOT NULL THEN
      V_EXEMPTION_NUMBER := P_EXEMPTION_NUMBER;
    END IF;

    IF P_EXEMPTION_REASON IS NOT NULL THEN
      V_EXEMPTION_REASON := P_EXEMPTION_REASON;
    END IF;

    IF P_GROWTH_TYPE IS NOT NULL THEN
      V_GROWTH_TYPE := P_GROWTH_TYPE;
    END IF;

    IF P_TIMBER_MARK IS NOT NULL THEN
      V_TIMBER_MARK := P_TIMBER_MARK;
    END IF;

    IF P_FOREST_FILE_ID IS NOT NULL THEN
      V_FOREST_FILE_ID := P_FOREST_FILE_ID;
    END IF;

    OPEN P_SPECIES_GRADE FOR
SELECT C.EXPORT_PRODUCT_TYPE_CODE,
       F.DESCRIPTION AS PRODUCT_TYPE,
       i.org_unit_name AS region,
       d.export_grade_code,
       sum(case
             when FI.SUM_FI is null then
              0
             else
              fi.sum_fi
           end) as SUM_FI,
       sum(case
             when ce.SUM_ce is null then
              0
             else
              CE.SUM_CE
           end) as SUM_CE,
       sum(case
             when sp.SUM_sp is null then
              0
             else
              SP.SUM_SP
           end) as SUM_SP,
       sum(case
             when lo.SUM_lo is null then
              0
             else
              LO.SUM_LO
           end) as SUM_LO,
       sum(case
             when he.SUM_he is null then
              0
             else
              HE.SUM_HE
           end) as SUM_HE,
       sum(case
             when ba.SUM_ba is null then
              0
             else
              BA.SUM_BA
           end) as SUM_BA,
       sum(case
             when cy.SUM_cy is null then
              0
             else
              CY.SUM_CY
           end) as SUM_CY,
       sum(case
             when al.SUM_al is null then
              0
             else
              AL.SUM_AL
           end) as SUM_AL,
       sum(case
             when co.SUM_co is null then
              0
             else
              CO.SUM_CO
           end) as SUM_CO,
       sum(case
             when hd.SUM_hd is null then
              0
             else
              HD.SUM_HD
           end) as SUM_HD,
       sum(case
             when ot.SUM_ot is null then
              0
             else
              OT.SUM_OT
           end) as SUM_OT,
       EJC.DESCRIPTION AS EXPORT_JURISDICTION_CODE,
       case -- we want to sort by letter grades followed by number grades followed by the blank grade
         when export_grade_code = regexp_substr(export_grade_code, '[1-9]') then
          regexp_substr(export_grade_code, '[1-9]') + 1
         when export_grade_code = ' ' then
          10
         else
          1
       end as sorted_column
  FROM EXPORT_PERMIT_DETAIL A
 INNER JOIN EXPORT_SCALE_DETAIL D
    ON A.EXPORT_PERMIT_DETAIL_NUMBER = D.EXPORT_PERMIT_DETAIL_NUMBER
 INNER JOIN EXPORT_EXEMPTION E
    ON A.EXEMPTION_NUMBER = E.EXEMPTION_NUMBER
 INNER JOIN Export_Exemption_Application C
    on C.Exemption_Number = E.Exemption_Number
 INNER JOIN EXPORT_PRODUCT_TYPE_CODE F
    ON C.EXPORT_PRODUCT_TYPE_CODE = F.EXPORT_PRODUCT_TYPE_CODE
 INNER JOIN EXPORT_EXEMPTION_REASON_CODE G
    ON C.EXPORT_EXEMPTION_REASON_CODE = G.EXPORT_EXEMPTION_REASON_CODE
 INNER JOIN EXPORT_EXEMPTION_TYPE_CODE H
    ON H.EXPORT_EXEMPTION_TYPE_CODE = E.EXPORT_EXEMPTION_TYPE_CODE
 INNER JOIN ORG_UNIT I
    ON A.ORG_UNIT_NO = I.ORG_UNIT_NO
 INNER JOIN EXPORT_JURISDICTION_CODE EJC
    ON C.EXPORT_JURISDICTION_CODE = EJC.EXPORT_JURISDICTION_CODE
 INNER JOIN EXPORT_COUNTRY_CODE ECC
    ON A.EXPORT_COUNTRY_CODE = ECC.EXPORT_COUNTRY_CODE
  LEFT OUTER JOIN (select esd_fi.export_grade_code as grade_code,
                          esd_fi.export_scale_detail_id as scale_id,
                          sum(esd_fi.species_grade_volume) as SUM_FI
                     from export_scale_detail esd_fi
                    inner join export_package fi_pack
                       on esd_fi.package_number = fi_pack.package_number
                    inner join export_permit_detail epd
                       on esd_fi.export_permit_detail_number =
                          epd.export_permit_detail_number
                    where esd_fi.export_species_code = 'FI'
                      and epd.EXPORT_PERMIT_ISSUE_DATE BETWEEN
                          V_DATE_FROM AND
                          V_DATE_TO
                    group by esd_fi.export_scale_detail_id,
                             esd_fi.export_grade_code) FI
    on FI.scale_id = d.export_scale_detail_id
   and FI.grade_code = d.export_grade_code
  LEFT OUTER JOIN (select esd_ce.export_grade_code as grade_code,
                          esd_ce.export_scale_detail_id as scale_id,
                          sum(esd_ce.species_grade_volume) as SUM_ce
                     from export_scale_detail esd_ce
                    inner join export_package ce_pack
                       on esd_ce.package_number = ce_pack.package_number
                    inner join export_permit_detail epd on esd_ce.export_permit_detail_number = epd.export_permit_detail_number
                    where esd_ce.export_species_code = 'CE'  and epd.EXPORT_PERMIT_ISSUE_DATE BETWEEN V_DATE_FROM AND V_DATE_TO
                    group by esd_CE.export_scale_detail_id,
                             esd_CE.export_grade_code) CE
    on CE.scale_id = d.export_scale_detail_id
   and CE.grade_code = d.export_grade_code
  LEFT OUTER JOIN (select esd_SP.export_grade_code as grade_code,
                          esd_SP.export_scale_detail_id as scale_id,
                          sum(esd_SP.species_grade_volume) as SUM_SP
                     from export_scale_detail esd_SP
                    inner join export_package SP_pack
                       on esd_SP.package_number = SP_pack.package_number
                       inner join export_permit_detail epd on esd_sp.export_permit_detail_number = epd.export_permit_detail_number
                    where esd_SP.export_species_code = 'SP'  and epd.EXPORT_PERMIT_ISSUE_DATE BETWEEN V_DATE_FROM AND V_DATE_TO
                    group by esd_SP.export_scale_detail_id,
                             esd_SP.export_grade_code) SP
    on SP.scale_id = d.export_scale_detail_id
   and SP.grade_code = d.export_grade_code
  LEFT OUTER JOIN (select esd_LO.export_grade_code as grade_code,
                          esd_LO.export_scale_detail_id as scale_id,
                          sum(esd_LO.species_grade_volume) as SUM_LO
                     from export_scale_detail esd_LO
                    inner join export_package LO_pack
                       on esd_LO.package_number = LO_pack.package_number
                       inner join export_permit_detail epd on esd_lo.export_permit_detail_number = epd.export_permit_detail_number
                    where esd_LO.export_species_code = 'LO'  and epd.EXPORT_PERMIT_ISSUE_DATE BETWEEN V_DATE_FROM AND V_DATE_TO
                    group by esd_LO.export_scale_detail_id,
                             esd_LO.export_grade_code) LO
    on LO.scale_id = d.export_scale_detail_id
   and LO.grade_code = d.export_grade_code
  LEFT OUTER JOIN (select esd_He.export_grade_code as grade_code,
                          esd_HE.export_scale_detail_id as scale_id,
                          sum(esd_HE.species_grade_volume) as SUM_HE
                     from export_scale_detail esd_HE
                    inner join export_package HE_pack
                       on esd_HE.package_number = HE_pack.package_number
                       inner join export_permit_detail epd on esd_he.export_permit_detail_number = epd.export_permit_detail_number
                    where esd_HE.export_species_code = 'HE'  and epd.EXPORT_PERMIT_ISSUE_DATE BETWEEN V_DATE_FROM AND V_DATE_TO
                    group by esd_HE.export_scale_detail_id,
                             esd_HE.export_grade_code) HE
    on HE.scale_id = d.export_scale_detail_id
   and HE.grade_code = d.export_grade_code
  LEFT OUTER JOIN (select esd_BA.export_grade_code as grade_code,
                          esd_BA.export_scale_detail_id as scale_id,
                          sum(esd_BA.species_grade_volume) as SUM_BA
                     from export_scale_detail esd_BA
                    inner join export_package BA_pack
                       on esd_BA.package_number = BA_pack.package_number
                       inner join export_permit_detail epd on esd_ba.export_permit_detail_number = epd.export_permit_detail_number
                    where esd_BA.export_species_code = 'BA'  and epd.EXPORT_PERMIT_ISSUE_DATE BETWEEN V_DATE_FROM AND V_DATE_TO
                    group by esd_BA.export_scale_detail_id,
                             esd_BA.export_grade_code) BA
    on BA.scale_id = d.export_scale_detail_id
   and BA.grade_code = d.export_grade_code
  LEFT OUTER JOIN (select esd_CY.export_grade_code as grade_code,
                          esd_CY.export_scale_detail_id as scale_id,
                          sum(esd_CY.species_grade_volume) as SUM_CY
                     from export_scale_detail esd_CY
                    inner join export_package CY_pack
                       on esd_CY.package_number = CY_pack.package_number
                       inner join export_permit_detail epd on esd_cy.export_permit_detail_number = epd.export_permit_detail_number
                    where esd_CY.export_species_code = 'CY'  and epd.EXPORT_PERMIT_ISSUE_DATE BETWEEN V_DATE_FROM AND V_DATE_TO
                    group by esd_CY.export_scale_detail_id,
                             esd_CY.export_grade_code) CY
    on CY.scale_id = d.export_scale_detail_id
   and CY.grade_code = d.export_grade_code
  LEFT OUTER JOIN (select esd_LA.export_grade_code as grade_code,
                          esd_LA.export_scale_detail_id as scale_id,
                          sum(esd_LA.species_grade_volume) as SUM_LA
                     from export_scale_detail esd_LA
                    inner join export_package LA_pack
                       on esd_LA.package_number = la_pack.package_number
                       inner join export_permit_detail epd on esd_la.export_permit_detail_number = epd.export_permit_detail_number
                    where esd_LA.export_species_code = 'LA'  and epd.EXPORT_PERMIT_ISSUE_DATE BETWEEN V_DATE_FROM AND V_DATE_TO
                    group by esd_LA.export_scale_detail_id,
                             esd_LA.export_grade_code) LA
    on LA.scale_id = d.export_scale_detail_id
   and LA.grade_code = d.export_grade_code
  LEFT OUTER JOIN (select esd_al.export_grade_code as grade_code,
                          esd_al.export_scale_detail_id as scale_id,
                          sum(esd_al.species_grade_volume) as SUM_al
                     from export_scale_detail esd_al
                    inner join export_package al_pack
                       on esd_al.package_number = al_pack.package_number
                       inner join export_permit_detail epd on esd_al.export_permit_detail_number = epd.export_permit_detail_number
                    where esd_AL.export_species_code = 'AL'  and epd.EXPORT_PERMIT_ISSUE_DATE BETWEEN V_DATE_FROM AND V_DATE_TO
                    group by esd_AL.export_scale_detail_id,
                             esd_AL.export_grade_code) AL
    on AL.scale_id = d.export_scale_detail_id
   and AL.grade_code = d.export_grade_code
  LEFT OUTER JOIN (select esd_CO.export_grade_code as grade_code,
                          esd_CO.export_scale_detail_id as scale_id,
                          sum(esd_CO.species_grade_volume) as SUM_CO
                     from export_scale_detail esd_CO
                    inner join export_package CO_pack
                       on esd_CO.package_number = CO_pack.package_number
                       inner join export_permit_detail epd on esd_co.export_permit_detail_number = epd.export_permit_detail_number
                    where esd_CO.export_species_code = 'CO'  and epd.EXPORT_PERMIT_ISSUE_DATE BETWEEN V_DATE_FROM AND V_DATE_TO
                    group by esd_CO.export_scale_detail_id,
                             esd_CO.export_grade_code) CO
    on CO.scale_id = d.export_scale_detail_id
   and CO.grade_code = d.export_grade_code
  LEFT OUTER JOIN (select esd_HD.export_grade_code as grade_code,
                          esd_HD.export_scale_detail_id as scale_id,
                          sum(esd_HD.species_grade_volume) as SUM_HD
                     from export_scale_detail esd_HD
                    inner join export_package HD_pack
                       on esd_HD.package_number = HD_pack.package_number
                       inner join export_permit_detail epd on esd_hd.export_permit_detail_number = epd.export_permit_detail_number
                    where esd_HD.export_species_code IN
                          ('AR', 'AS', 'BI', 'MA')  and epd.EXPORT_PERMIT_ISSUE_DATE BETWEEN V_DATE_FROM AND V_DATE_TO
                    group by esd_HD.export_scale_detail_id,
                             esd_HD.export_grade_code) HD
    on HD.scale_id = d.export_scale_detail_id
   and HD.grade_code = d.export_grade_code
  LEFT OUTER JOIN (select esd_OT.export_grade_code as grade_code,
                          esd_OT.export_scale_detail_id as scale_id,
                          sum(esd_OT.species_grade_volume) as SUM_OT
                     from export_scale_detail esd_OT
                    inner join export_package OT_pack
                       on esd_OT.package_number = OT_pack.package_number
                       inner join export_permit_detail epd on esd_ot.export_permit_detail_number = epd.export_permit_detail_number
                    where esd_OT.export_species_code in
                          ('LA', 'WB', 'WH', 'YE', 'UU')  and epd.EXPORT_PERMIT_ISSUE_DATE BETWEEN V_DATE_FROM AND V_DATE_TO
                    group by esd_OT.export_scale_detail_id,
                             esd_OT.export_grade_code) OT
    on OT.scale_id = d.export_scale_detail_id
   and OT.grade_code = d.export_grade_code

 WHERE A.EXPORT_PERMIT_ISSUE_DATE BETWEEN
       V_DATE_FROM AND
       V_DATE_TO
   and c.export_jurisdiction_code = 'P'
   AND A.EXPORT_PERMIT_STATUS_CODE LIKE V_PERMIT_STATUS
   AND P_ORG_UNIT_NO like '%' || A.ORG_UNIT_NO || '%'
   AND E.EXEMPTION_NUMBER LIKE V_EXEMPTION_NUMBER
   AND E.EXPORT_EXEMPTION_TYPE_CODE LIKE V_EXEMPTION_TYPE
   AND C.EXPORT_EXEMPTION_REASON_CODE LIKE V_EXEMPTION_REASON
   AND C.EXPORT_GROWTH_TYPE_CODE LIKE V_GROWTH_TYPE
   AND (P_TIMBER_MARK IS NULL OR D.TIMBER_MARK LIKE V_TIMBER_MARK)
 GROUP BY C.EXPORT_PRODUCT_TYPE_CODE,
          F.DESCRIPTION,
          i.org_unit_name,
          d.export_grade_code,
          EJC.DESCRIPTION
 ORDER BY i.org_unit_name,
          c.export_product_type_code asc,
          SORTED_COLUMN,
          d.export_grade_code;

  END SPECIES_GRADE_REPORT_CSV;

  /******************************************************************************
      PROCEDURE:  SPECIES_GRADE_REGION_SUBRPT
      PURPOSE: USED BY LEXIS_SPECIES_GRADE.RPT's subreport
  ******************************************************************************/
  PROCEDURE SPECIES_GRADE_REGION_SUBRPT(P_DATE_FROM        VARCHAR2,
                                        P_DATE_TO          VARCHAR2,
                                        P_ORG_UNIT         VARCHAR2,
                                        P_EXEMPTION_NUMBER EXPORT_EXEMPTION_APPLICATION.OWNER_CLIENT_NUMBER%TYPE,
                                        P_EXEMPTION_TYPE   EXPORT_EXEMPTION.EXPORT_EXEMPTION_TYPE_CODE%TYPE,
                                        P_EXEMPTION_REASON EXPORT_EXEMPTION_APPLICATION.EXPORT_EXEMPTION_REASON_CODE%TYPE,
                                        P_GROWTH_TYPE      EXPORT_EXEMPTION_APPLICATION.EXPORT_GROWTH_TYPE_CODE%TYPE,
                                        P_TIMBER_MARK      EXPORT_SCALE_DETAIL.TIMBER_MARK%TYPE,
                                        P_FOREST_FILE_ID   HAULING_AUTHORITY.FOREST_FILE_ID%TYPE,
                                        P_PERMIT_STATUS    EXPORT_PERMIT_DETAIL.EXPORT_PERMIT_STATUS_CODE%TYPE,
                                        P_SPECIES_GRADE    IN OUT REF_CUR_GENERAL) AS

    -- variables used in the initial data selection
    V_DATE_FROM     DATE := TO_DATE('0001-01-01', 'YYYY-MM-DD');
    V_DATE_TO       DATE := TO_DATE('9999-12-31', 'YYYY-MM-DD');

    V_EXEMPTION_NUMBER VARCHAR2(12) := P_EXEMPTION_NUMBER;
    V_EXEMPTION_TYPE   VARCHAR2(5) := '%';
    V_EXEMPTION_REASON VARCHAR2(5) := '%';
    V_GROWTH_TYPE      VARCHAR2(5) := '%';
    V_TIMBER_MARK      VARCHAR2(12) := '%';
    V_PERMIT_STATUS    VARCHAR2(5) := '%';

  BEGIN

    IF P_EXEMPTION_NUMBER IS NULL THEN
      V_EXEMPTION_NUMBER := '%';
    END IF;

    IF P_DATE_FROM IS NOT NULL THEN
      V_DATE_FROM := TO_DATE(P_DATE_FROM, 'YYYY-MM-DD');
    END IF;

    IF P_DATE_TO IS NOT NULL THEN
      V_DATE_TO := TO_DATE(P_DATE_TO, 'YYYY-MM-DD');
    END IF;

    IF P_EXEMPTION_TYPE IS NOT NULL THEN
      V_EXEMPTION_TYPE := P_EXEMPTION_TYPE;
    END IF;

    IF P_EXEMPTION_REASON IS NOT NULL THEN
      V_EXEMPTION_REASON := P_EXEMPTION_REASON;
    END IF;

    IF P_GROWTH_TYPE IS NOT NULL THEN
      V_GROWTH_TYPE := P_GROWTH_TYPE;
    END IF;

    IF P_TIMBER_MARK IS NOT NULL THEN
      V_TIMBER_MARK := P_TIMBER_MARK;
    END IF;

    IF P_PERMIT_STATUS IS NOT NULL THEN
      V_PERMIT_STATUS := P_PERMIT_STATUS;
    END IF;

    OPEN P_SPECIES_GRADE FOR
      select inner_select.org_unit_name,
             inner_select.export_growth_type_code,
             inner_select.export_grade_code,
             sum(inner_select.sum_fi),
             sum(inner_select.sum_ce),
             sum(inner_select.sum_sp),
             sum(inner_select.sum_lo),
             sum(inner_select.sum_he),
             sum(inner_select.sum_ba),
             sum(inner_select.sum_cy),
             sum(inner_select.sum_al),
             sum(inner_select.sum_co),
             sum(inner_select.sum_hd),
             sum(inner_select.sum_ot),
             case -- we want to sort by letter grades followed by number grades followed by the blank grade
               when inner_select.export_grade_code =
                    regexp_substr(inner_select.export_grade_code, '[1-9]') then
                regexp_substr(inner_select.export_grade_code, '[1-9]') + 1
               when inner_select.export_grade_code = ' ' then
                10
               else
                1
             end as sorted_column
        from (SELECT I.Org_Unit_Name,
                     a.export_growth_type_code,
                     D.EXPORT_SPECIES_CODE,
                     D.EXPORT_GRADE_CODE,
                     CASE
                       WHEN D.EXPORT_SPECIES_CODE = 'FI' THEN
                        SUM(D.SPECIES_GRADE_VOLUME)
                       ELSE
                        0.0
                     END AS SUM_FI,
                     CASE
                       WHEN D.EXPORT_SPECIES_CODE = 'CE' THEN
                        SUM(D.SPECIES_GRADE_VOLUME)
                       ELSE
                        0.0
                     END AS SUM_CE,
                     CASE
                       WHEN D.EXPORT_SPECIES_CODE = 'SP' THEN
                        SUM(D.SPECIES_GRADE_VOLUME)
                       ELSE
                        0.0
                     END AS SUM_SP,
                     CASE
                       WHEN D.EXPORT_SPECIES_CODE = 'LO' THEN
                        SUM(D.SPECIES_GRADE_VOLUME)
                       ELSE
                        0.0
                     END AS SUM_LO,
                     CASE
                       WHEN D.EXPORT_SPECIES_CODE = 'HE' THEN
                        SUM(D.SPECIES_GRADE_VOLUME)
                       ELSE
                        0.0
                     END AS SUM_HE,
                     CASE
                       WHEN D.EXPORT_SPECIES_CODE = 'BA' THEN
                        SUM(D.SPECIES_GRADE_VOLUME)
                       ELSE
                        0.0
                     END AS SUM_BA,
                     CASE
                       WHEN D.EXPORT_SPECIES_CODE = 'CY' THEN
                        SUM(D.SPECIES_GRADE_VOLUME)
                       ELSE
                        0.0
                     END AS SUM_CY,
                     CASE
                       WHEN D.EXPORT_SPECIES_CODE = 'AL' THEN
                        SUM(D.SPECIES_GRADE_VOLUME)
                       ELSE
                        0.0
                     END AS SUM_AL,
                     CASE
                       WHEN D.EXPORT_SPECIES_CODE = 'CO' THEN
                        SUM(D.SPECIES_GRADE_VOLUME)
                       ELSE
                        0.0
                     END AS SUM_CO,
                     CASE
                       WHEN D.EXPORT_SPECIES_CODE = 'AR' THEN
                        SUM(D.SPECIES_GRADE_VOLUME)
                       WHEN D.EXPORT_SPECIES_CODE = 'AS' THEN
                        SUM(D.SPECIES_GRADE_VOLUME)
                       WHEN D.EXPORT_SPECIES_CODE = 'BI' THEN
                        SUM(D.SPECIES_GRADE_VOLUME)
                       WHEN D.EXPORT_SPECIES_CODE = 'MA' THEN
                        SUM(D.SPECIES_GRADE_VOLUME)
                       ELSE
                        0.0
                     END AS SUM_HD,
                     CASE
                       WHEN D.EXPORT_SPECIES_CODE = 'LA' THEN
                        SUM(D.SPECIES_GRADE_VOLUME)
                       WHEN D.EXPORT_SPECIES_CODE = 'WB' THEN
                        SUM(D.SPECIES_GRADE_VOLUME)
                       WHEN D.EXPORT_SPECIES_CODE = 'WH' THEN
                        SUM(D.SPECIES_GRADE_VOLUME)
                       WHEN D.EXPORT_SPECIES_CODE = 'YE' THEN
                        SUM(D.SPECIES_GRADE_VOLUME)
                       WHEN D.EXPORT_SPECIES_CODE = 'UU' THEN
                        SUM(D.SPECIES_GRADE_VOLUME)
                       ELSE
                        0.0
                     END AS SUM_OT
                FROM EXPORT_PERMIT_DETAIL A
               INNER JOIN EXPORT_SCALE_DETAIL D
                  ON A.EXPORT_PERMIT_DETAIL_NUMBER = D.EXPORT_PERMIT_DETAIL_NUMBER
               INNER JOIN HAULING_AUTHORITY HA
                  ON D.TIMBER_MARK = HA.TIMBER_MARK
               INNER JOIN EXPORT_EXEMPTION E
                  ON A.EXEMPTION_NUMBER = E.EXEMPTION_NUMBER
               INNER JOIN EXPORT_EXEMPTION_APPLICATION C on C.Exemption_Number = A.Exemption_Number
               INNER JOIN EXPORT_EXEMPTION_REASON_CODE F
                  ON F.EXPORT_EXEMPTION_REASON_CODE =
                     C.EXPORT_EXEMPTION_REASON_CODE
               INNER JOIN EXPORT_PERMIT_STATUS_CODE G
                  ON G.EXPORT_PERMIT_STATUS_CODE =
                     A.EXPORT_PERMIT_STATUS_CODE
               INNER JOIN EXPORT_PRODUCT_TYPE_CODE H
                  ON C.EXPORT_PRODUCT_TYPE_CODE = H.EXPORT_PRODUCT_TYPE_CODE
               INNER JOIN ORG_UNIT I
                  ON I.ORG_UNIT_NO = A.ORG_UNIT_NO
               WHERE (A.EXPORT_PERMIT_ISSUE_DATE BETWEEN V_DATE_FROM AND
                     V_DATE_TO OR A.EXPORT_PERMIT_ISSUE_DATE IS NULL)
                 AND A.EXPORT_PERMIT_STATUS_CODE LIKE V_PERMIT_STATUS
                 AND C.EXPORT_JURISDICTION_CODE = 'P'
                 AND P_ORG_UNIT like '%' || A.ORG_UNIT_NO || '%'
                 AND E.EXEMPTION_NUMBER LIKE V_EXEMPTION_NUMBER
                 AND E.EXPORT_EXEMPTION_TYPE_CODE LIKE V_EXEMPTION_TYPE
                 AND C.EXPORT_EXEMPTION_REASON_CODE LIKE V_EXEMPTION_REASON
                 AND C.EXPORT_GROWTH_TYPE_CODE LIKE V_GROWTH_TYPE
                 AND D.TIMBER_MARK LIKE V_TIMBER_MARK
               GROUP BY D.EXPORT_SPECIES_CODE,
                        D.EXPORT_GRADE_CODE,
                        I.Org_Unit_Name,
                        a.export_growth_type_code) inner_select
        join export_grade_code egc
          on egc.export_grade_code = inner_select.export_grade_code
       group by org_unit_name,
                inner_select.export_growth_type_code,
                inner_select.export_grade_code
       ORDER BY org_unit_name,
                sorted_column,
                inner_select.EXPORT_GRADE_CODE;

  END SPECIES_GRADE_REGION_SUBRPT;

  /******************************************************************************
      PROCEDURE:  EXEMPTION_LEDGER_RPT
      PURPOSE: USED BY LEXIS_EXEMPTIONS_LEDGER.RPT
  ******************************************************************************/
  PROCEDURE EXEMPTION_LEDGER_RPT(P_FROM_DATE        VARCHAR2,
                                 P_TO_DATE          VARCHAR2,
                                 P_LISTING_FROM_DATE VARCHAR2,
                                 P_LISTING_TO_DATE  VARCHAR2,
                                 P_ORG_UNIT         VARCHAR2,
                                 P_EXEMPTION_REASON EXPORT_EXEMPTION_REASON_CODE.EXPORT_EXEMPTION_REASON_CODE%TYPE,
                                 P_EXEMPTION_TYPE   EXPORT_EXEMPTION_TYPE_CODE.EXPORT_EXEMPTION_TYPE_CODE%TYPE,
                                 P_CLIENT           CLIENT_LOCATION.CLIENT_NUMBER%TYPE,
                                 P_GROWTH_TYPE      EXPORT_EXEMPTION_APPLICATION.EXPORT_GROWTH_TYPE_CODE%TYPE,
                                 P_EXEMPTION_NUMBER EXPORT_EXEMPTION_APPLICATION.EXEMPTION_NUMBER%TYPE,
                                 P_EXEMPTION_STATUS EXPORT_EXEMPTION.EXPORT_EXEMPTION_STATUS_CODE%TYPE,
                                 P_EXEMPTIONS       IN OUT REF_CUR_GENERAL) IS

    V_DATE_FROM        DATE := TO_DATE('0001-01-01', 'YYYY-MM-DD');
    V_DATE_TO          DATE := TO_DATE('9999-12-31', 'YYYY-MM-DD');
    V_DATE_LISTING_FROM DATE := TO_DATE('0001-01-01', 'YYYY-MM-DD');
    V_DATE_LISTING_TO   DATE := TO_DATE('9999-12-31', 'YYYY-MM-DD');
    V_CLIENT           VARCHAR2(10) := P_CLIENT;
    V_GROWTH_TYPE      VARCHAR2(5) := '%';
    V_EXEMPTION_REASON VARCHAR2(5) := '%';
    V_EXEMPTION_TYPE   VARCHAR2(5) := '%';
    V_EXEMPTION_NUMBER VARCHAR2(8) := P_EXEMPTION_NUMBER;
    V_EXEMPTION_STATUS VARCHAR2(5) := '%';

  BEGIN

    IF P_CLIENT IS NULL THEN
      V_CLIENT := '%';
    END IF;

    IF P_FROM_DATE IS NOT NULL THEN
      V_DATE_FROM := TO_DATE(P_FROM_DATE, 'YYYY-MM-DD');
    END IF;

    IF P_TO_DATE IS NOT NULL THEN
      V_DATE_TO := TO_DATE(P_TO_DATE, 'YYYY-MM-DD');
    END IF;

    IF P_LISTING_FROM_DATE IS NOT NULL THEN
      V_DATE_LISTING_FROM := TO_DATE(P_LISTING_FROM_DATE, 'YYYY-MM-DD');
    END IF;

    IF P_LISTING_TO_DATE IS NOT NULL THEN
      V_DATE_LISTING_TO := TO_DATE(P_LISTING_TO_DATE, 'YYYY-MM-DD');
    END IF;

    IF P_GROWTH_TYPE IS NOT NULL THEN
      V_GROWTH_TYPE := P_GROWTH_TYPE;
    END IF;

    IF P_EXEMPTION_REASON IS NOT NULL THEN
      V_EXEMPTION_REASON := P_EXEMPTION_REASON;
    END IF;

    IF P_EXEMPTION_TYPE IS NOT NULL THEN
      V_EXEMPTION_TYPE := P_EXEMPTION_TYPE;
    END IF;

    IF P_EXEMPTION_NUMBER IS NULL THEN
      V_EXEMPTION_NUMBER := '%';
    END IF;

    IF P_EXEMPTION_STATUS IS NOT NULL THEN
      V_EXEMPTION_STATUS := P_EXEMPTION_STATUS;
    END IF;

    OPEN P_EXEMPTIONS FOR
      SELECT O.ORG_UNIT_CODE,
             O.ORG_UNIT_NAME,
             A.EXPORT_EXEMPTION_REASON_CODE || ' - ' || R.DESCRIPTION AS EXPORT_EXEMPTION_REASON_DESC,
             T.EXPORT_EXEMPTION_TYPE_CODE || ' - ' || T.DESCRIPTION AS EXPORT_EXEMPTION_TYPE_DESC,
             A.EXEMPTION_NUMBER AS EXEMPTION_NUMBER,
             F.CLIENT_NAME,
             F.LEGAL_FIRST_NAME,
             F.LEGAL_MIDDLE_NAME,
             F.CLIENT_TYPE_CODE,
             E.EXPIRY_DATE,
             E.APPROVED_VOLUME,
             A.EXPORT_JURISDICTION_CODE,
             A.APPLICATION_NUMBER,
             A.FED_APPLICATION_NUMBER,
             A.EXEMPTION_APPLICATION_VOLUME
        FROM EXPORT_EXEMPTION_APPLICATION A
       INNER JOIN EXPORT_EXEMPTION E
          ON A.EXEMPTION_NUMBER = E.EXEMPTION_NUMBER
       INNER JOIN FOREST_CLIENT F
          ON A.OWNER_CLIENT_NUMBER = F.CLIENT_NUMBER
       INNER JOIN ORG_UNIT O
          ON A.ORG_UNIT_NO = O.ORG_UNIT_NO
       INNER JOIN EXPORT_EXEMPTION_REASON_CODE R
          ON A.EXPORT_EXEMPTION_REASON_CODE =
             R.EXPORT_EXEMPTION_REASON_CODE
       INNER JOIN EXPORT_EXEMPTION_TYPE_CODE T
          ON E.EXPORT_EXEMPTION_TYPE_CODE = T.EXPORT_EXEMPTION_TYPE_CODE
       LEFT JOIN EXPORT_SCHEDULE ES
          ON ES.EXPORT_SCHEDULE_ID = A.EXPORT_SCHEDULE_ID
       WHERE (E.APPROVAL_DATE BETWEEN V_DATE_FROM AND V_DATE_TO OR E.APPROVAL_DATE IS NULL)
         AND ((V_DATE_LISTING_FROM = TO_DATE('0001-01-01', 'YYYY-MM-DD') AND V_DATE_LISTING_TO = TO_DATE('9999-12-31', 'YYYY-MM-DD'))
             OR
             (ES.ADVERTISING_DATE BETWEEN V_DATE_LISTING_FROM AND V_DATE_LISTING_TO))
         AND P_ORG_UNIT like '%' || A.ORG_UNIT_NO || '%'
         AND A.EXPORT_EXEMPTION_REASON_CODE LIKE V_EXEMPTION_REASON
         AND E.EXPORT_EXEMPTION_TYPE_CODE LIKE V_EXEMPTION_TYPE
         AND A.OWNER_CLIENT_NUMBER LIKE '%' || V_CLIENT || '%'
         AND A.EXPORT_GROWTH_TYPE_CODE LIKE V_GROWTH_TYPE
         AND E.EXPORT_EXEMPTION_STATUS_CODE LIKE V_EXEMPTION_STATUS
         AND A.EXEMPTION_NUMBER LIKE '%' || V_EXEMPTION_NUMBER || '%'
       ORDER BY O.Org_Unit_No, A.EXEMPTION_NUMBER, A.APPLICATION_NUMBER;
  END EXEMPTION_LEDGER_RPT;

  /******************************************************************************
      PROCEDURE:  EXEMPTION_LEDGER_RPT_CSV
      PURPOSE: RETRIEVE A FEE SUMMARY REPORT THAT WILL BE OUTPUTTED AS A CSV.
  ******************************************************************************/
  PROCEDURE EXEMPTION_LEDGER_RPT_CSV(P_WHERE_CLAUSE IN VARCHAR,
                                     P_PARAMS_ARRAY IN CBR_VARCHAR2_ARRAY,
                                     P_NUM_PARAMS   IN NUMBER,
                                     P_EXEMPTION    IN OUT REF_CUR_GENERAL) IS
    V_EXEMPTION_SELECT VARCHAR2(4000);
  BEGIN

    V_EXEMPTION_SELECT := 'SELECT O.ORG_UNIT_CODE,
             O.ORG_UNIT_NAME,
             A.EXPORT_EXEMPTION_REASON_CODE || '' - '' || R.DESCRIPTION AS EXPORT_EXEMPTION_REASON_DESC,
             T.EXPORT_EXEMPTION_TYPE_CODE || '' - '' || T.DESCRIPTION AS EXPORT_EXEMPTION_TYPE_DESC,
             A.EXEMPTION_NUMBER AS EXEMPTION_NUMBER,
             F.CLIENT_NAME,
             F.LEGAL_FIRST_NAME,
             F.LEGAL_MIDDLE_NAME,
             F.CLIENT_TYPE_CODE,
             E.EXPIRY_DATE,
             E.APPROVED_VOLUME,
             E.OTHER_CONDITIONS,
             NVL(A.FED_APPLICATION_NUMBER, A.APPLICATION_NUMBER) AS APPLICATION_NUMBER,
             A.EXEMPTION_APPLICATION_VOLUME
        FROM EXPORT_EXEMPTION_APPLICATION A
       INNER JOIN EXPORT_EXEMPTION E ON A.EXEMPTION_NUMBER =
                                        E.EXEMPTION_NUMBER
       INNER JOIN FOREST_CLIENT F ON A.OWNER_CLIENT_NUMBER =
                                     F.CLIENT_NUMBER
       INNER JOIN ORG_UNIT O ON A.ORG_UNIT_NO = O.ORG_UNIT_NO
       INNER JOIN EXPORT_EXEMPTION_REASON_CODE R ON A.EXPORT_EXEMPTION_REASON_CODE =
                                                    R.EXPORT_EXEMPTION_REASON_CODE
       INNER JOIN EXPORT_EXEMPTION_TYPE_CODE T ON E.EXPORT_EXEMPTION_TYPE_CODE =
                                                  T.EXPORT_EXEMPTION_TYPE_CODE
       LEFT JOIN EXPORT_SCHEDULE ES ON ES.EXPORT_SCHEDULE_ID = A.EXPORT_SCHEDULE_ID';

    LEXIS.FIND_BY(V_EXEMPTION_SELECT || P_WHERE_CLAUSE ||
                  ' ORDER BY A.EXEMPTION_NUMBER, A.APPLICATION_NUMBER',
                  P_PARAMS_ARRAY,
                  P_NUM_PARAMS,
                  P_EXEMPTION);
  END EXEMPTION_LEDGER_RPT_CSV;

  /******************************************************************************
      PROCEDURE:  TRANSPORT_LEDGER_RPT
      PURPOSE: USED BY LEXIS_TRASPORT_LEDGER.RPT
  ******************************************************************************/
  PROCEDURE TRANSPORT_LEDGER_RPT(P_FROM_DATE           VARCHAR2,
                                 P_TO_DATE             VARCHAR2,
                                 P_JURISDICTION        EXPORT_JURISDICTION_CODE.EXPORT_JURISDICTION_CODE%TYPE,
                                 P_ORG_UNIT_NUMBER     VARCHAR2,
                                 P_DESTINATION_COUNTRY EXPORT_COUNTRY_CODE.EXPORT_COUNTRY_CODE%TYPE,
                                 P_PORT_OF_EXPORT      EXPORT_PORT_OF_EXPORT_CODE.EXPORT_PORT_OF_EXPORT_CODE%TYPE,
                                 P_STATUS              EXPORT_PERMIT_STATUS_CODE.EXPORT_PERMIT_STATUS_CODE%TYPE,
                                 P_TRANSPORT_REPORT    IN OUT REF_CUR_GENERAL) IS

    V_FROM_DATE           DATE := TO_DATE('0001-01-01', 'YYYY-MM-DD');
    V_TO_DATE             DATE := TO_DATE('9999-12-31', 'YYYY-MM-DD');
    V_JURISDICTION        VARCHAR2(5) := '%';
    V_DESTINATION_COUNTRY VARCHAR2(5) := '%';
    V_PORT_OF_EXPORT      VARCHAR2(5) := '%';
    V_STATUS              VARCHAR2(5) := '%';

  BEGIN

    IF P_FROM_DATE IS NOT NULL THEN
      V_FROM_DATE := TO_DATE(P_FROM_DATE, 'YYYY-MM-DD');
    END IF;

    IF P_TO_DATE IS NOT NULL THEN
      V_TO_DATE := TO_DATE(P_TO_DATE, 'YYYY-MM-DD');
    END IF;

    IF P_JURISDICTION IS NOT NULL THEN
      V_JURISDICTION := P_JURISDICTION;
    END IF;

    IF P_DESTINATION_COUNTRY IS NOT NULL THEN
      V_DESTINATION_COUNTRY := P_DESTINATION_COUNTRY;
    END IF;

    IF P_PORT_OF_EXPORT IS NOT NULL THEN
      V_PORT_OF_EXPORT := P_PORT_OF_EXPORT;
    END IF;

    IF P_STATUS IS NOT NULL THEN
      V_STATUS := P_STATUS;
    END IF;

    OPEN P_TRANSPORT_REPORT FOR
      SELECT DISTINCT A.ORG_UNIT_NO,
                      F.ORG_UNIT_NAME               AS REGION,
                      A.EXPORT_TRANSPORT_TYPE_CODE,
                      E.DESCRIPTION                 AS TRANSPORT,
                      A.EXPORT_PORT_OF_EXPORT_CODE,
                      D.DESCRIPTION                 AS PORT,
                      A.TRANSPORT_NAME,
                      A.ESTIMATED_SHIPPING_DATE,
                      A.EXPORT_PERMIT_DETAIL_NUMBER,
                      A.EXPORT_PERMIT_STATUS_CODE,
                      PSC.DESCRIPTION               AS STATUS,
                      A.CLIENT_NUMBER,
                      B.CLIENT_NAME,
                      B.LEGAL_FIRST_NAME,
                      B.LEGAL_MIDDLE_NAME,
                      B.CLIENT_TYPE_CODE,
                      A.PERMIT_VOLUME,
                      A.EXPORT_COUNTRY_CODE,
                      ECC.DESCRIPTION               AS COUNTRY,
                      A.EXEMPTION_NUMBER
        FROM EXPORT_PERMIT_VIEW A
       INNER JOIN FOREST_CLIENT B
          ON A.CLIENT_NUMBER = B.CLIENT_NUMBER
       INNER JOIN EXPORT_PORT_OF_EXPORT_CODE D
          ON A.EXPORT_PORT_OF_EXPORT_CODE = D.EXPORT_PORT_OF_EXPORT_CODE
       INNER JOIN EXPORT_TRANSPORT_TYPE_CODE E
          ON A.EXPORT_TRANSPORT_TYPE_CODE = E.EXPORT_TRANSPORT_TYPE_CODE
       INNER JOIN EXPORT_PERMIT_STATUS_CODE PSC
          ON A.EXPORT_PERMIT_STATUS_CODE = PSC.EXPORT_PERMIT_STATUS_CODE
       INNER JOIN EXPORT_COUNTRY_CODE ECC
          ON A.EXPORT_COUNTRY_CODE = ECC.EXPORT_COUNTRY_CODE
       INNER JOIN ORG_UNIT F
          ON F.ORG_UNIT_NO = A.ORG_UNIT_NO
       WHERE (A.EXPORT_PERMIT_ISSUE_DATE BETWEEN V_FROM_DATE AND V_TO_DATE OR
             A.EXPORT_PERMIT_ISSUE_DATE IS NULL)
         AND A.RECEIVED_DATE BETWEEN V_FROM_DATE AND V_TO_DATE
         AND A.EXPORT_PERMIT_STATUS_CODE LIKE V_STATUS
         AND P_ORG_UNIT_NUMBER like '%' || A.ORG_UNIT_NO || '%'
         AND A.EXPORT_PORT_OF_EXPORT_CODE LIKE V_PORT_OF_EXPORT
         AND A.EXPORT_COUNTRY_CODE LIKE V_DESTINATION_COUNTRY
         AND A.JURISDICTION LIKE V_JURISDICTION
       ORDER BY D.DESCRIPTION,
                A.EXEMPTION_NUMBER,
                A.EXPORT_PERMIT_DETAIL_NUMBER;

  END TRANSPORT_LEDGER_RPT;

  /******************************************************************************
      PROCEDURE:  TRANSPORT_REPORT_CSV
      PURPOSE: RETRIEVE A FEE SUMMARY REPORT THAT WILL BE OUTPUTTED AS A CSV.
  ******************************************************************************/
  PROCEDURE TRANSPORT_REPORT_CSV(P_WHERE_CLAUSE IN VARCHAR,
                                 P_PARAMS_ARRAY IN CBR_VARCHAR2_ARRAY,
                                 P_NUM_PARAMS   IN NUMBER,
                                 P_FEES         IN OUT REF_CUR_GENERAL) IS
    V_TRANSPORT_SELECT VARCHAR2(4000);
  BEGIN

    V_TRANSPORT_SELECT := 'SELECT DISTINCT A.ORG_UNIT_NO,
                      F.ORG_UNIT_NAME AS REGION,
                      A.EXPORT_TRANSPORT_TYPE_CODE,
                      E.DESCRIPTION AS TRANSPORT,
                      A.EXPORT_PORT_OF_EXPORT_CODE,
                      D.DESCRIPTION AS PORT,
                      A.TRANSPORT_NAME,
                      A.ESTIMATED_SHIPPING_DATE,
                      A.EXPORT_PERMIT_DETAIL_NUMBER,
                      A.EXPORT_PERMIT_STATUS_CODE,
                      PSC.DESCRIPTION AS STATUS,
                      A.CLIENT_NUMBER,
                      B.CLIENT_NAME,
                      B.LEGAL_FIRST_NAME,
                      B.LEGAL_MIDDLE_NAME,
                      B.CLIENT_TYPE_CODE,
                      A.PERMIT_VOLUME,
                      A.EXPORT_COUNTRY_CODE,
                      ECC.DESCRIPTION AS COUNTRY,
                      A.EXEMPTION_NUMBER
        FROM EXPORT_PERMIT_VIEW A
       INNER JOIN FOREST_CLIENT B ON A.CLIENT_NUMBER = B.CLIENT_NUMBER
       INNER JOIN EXPORT_PORT_OF_EXPORT_CODE D ON A.EXPORT_PORT_OF_EXPORT_CODE =
                                                  D.EXPORT_PORT_OF_EXPORT_CODE
       INNER JOIN EXPORT_TRANSPORT_TYPE_CODE E ON A.EXPORT_TRANSPORT_TYPE_CODE =
                                                  E.EXPORT_TRANSPORT_TYPE_CODE
       INNER JOIN EXPORT_PERMIT_STATUS_CODE PSC ON A.EXPORT_PERMIT_STATUS_CODE =
                                                   PSC.EXPORT_PERMIT_STATUS_CODE
       INNER JOIN EXPORT_COUNTRY_CODE ECC ON A.EXPORT_COUNTRY_CODE =
                                             ECC.EXPORT_COUNTRY_CODE
       INNER JOIN ORG_UNIT F ON F.ORG_UNIT_NO = A.ORG_UNIT_NO ';

    LEXIS.FIND_BY(V_TRANSPORT_SELECT || P_WHERE_CLAUSE ||
                  ' ORDER BY D.DESCRIPTION, A.EXEMPTION_NUMBER, A.EXPORT_PERMIT_DETAIL_NUMBER',
                  P_PARAMS_ARRAY,
                  P_NUM_PARAMS,
                  P_FEES);
  END TRANSPORT_REPORT_CSV;

  /******************************************************************************
      PROCEDURE:  FEE_SUMMARY_RPT
      PURPOSE: USED BY LEXIS_FEE_SUMMARY.RPT
  ******************************************************************************/
  PROCEDURE FEE_SUMMARY_RPT(P_FROM_DATE        VARCHAR2,
                            P_TO_DATE          VARCHAR2,
                            P_ORG_UNIT_NUMBER  VARCHAR2,
                            P_EXEMPTION_NUMBER EXPORT_EXEMPTION.EXEMPTION_NUMBER%TYPE,
                            P_EXEMPTION_TYPE   EXPORT_EXEMPTION.EXPORT_EXEMPTION_TYPE_CODE%TYPE,
                            P_EXEMPTION_REASON EXPORT_EXEMPTION_APPLICATION.EXPORT_EXEMPTION_REASON_CODE%TYPE,
                            P_GROWTH_TYPE      EXPORT_EXEMPTION_APPLICATION.EXPORT_GROWTH_TYPE_CODE%TYPE,
                            P_FEE_REPORT       IN OUT REF_CUR_GENERAL) IS

    V_FROM_DATE        DATE := TO_DATE('0001-01-01', 'YYYY-MM-DD');
    V_TO_DATE          DATE := TO_DATE('9999-01-01', 'YYYY-MM-DD');

    V_EXEMPTION_NUMBER VARCHAR2(12) := P_EXEMPTION_NUMBER;
    V_EXEMPTION_TYPE   VARCHAR2(5) := '%';
    V_EXEMPTION_REASON VARCHAR2(5) := '%';
    V_GROWTH_TYPE      VARCHAR2(5) := '%';

  BEGIN

    IF P_EXEMPTION_NUMBER IS NULL THEN
      V_EXEMPTION_NUMBER := '%';
    END IF;

    IF P_FROM_DATE IS NOT NULL THEN
      V_FROM_DATE := TO_DATE(P_FROM_DATE, 'YYYY-MM-DD');
    END IF;

    IF P_TO_DATE IS NOT NULL THEN
      V_TO_DATE := TO_DATE(P_TO_DATE, 'YYYY-MM-DD');
    END IF;

    IF P_EXEMPTION_TYPE IS NOT NULL THEN
      V_EXEMPTION_TYPE := P_EXEMPTION_TYPE;
    END IF;

    IF P_EXEMPTION_REASON IS NOT NULL THEN
      V_EXEMPTION_REASON := P_EXEMPTION_REASON;
    END IF;

    IF P_GROWTH_TYPE IS NOT NULL THEN
      V_GROWTH_TYPE := P_GROWTH_TYPE;
    END IF;

    OPEN P_FEE_REPORT FOR
            SELECT ORG_UNIT_NO,
                   ORG_UNIT,
                   EXPORT_EXEMPTION_REASON_CODE,
                   EXPORT_EXEMPTION_TYPE_CODE,
                   EXEMPTION_NUMBER,
                   EXPORT_PERMIT_DETAIL_NUMBER,
                   PERMIT_VOLUME,
                   CASE WHEN DOMESTIC_VALUE IS NULL THEN
                        LEXIS_POLICY.GET_PERMIT_DV(EXPORT_PERMIT_DETAIL_NUMBER, APPLICATION_DATE)
                        ELSE
                          DOMESTIC_VALUE END AS DOMESTIC_VALUE,
                   LEXIS.GET_PERMIT_EXPORT_VALUE(EXPORT_PERMIT_DETAIL_NUMBER) AS EXPORT_VALUE,
                   CASE WHEN OVERRIDE_FEE IS NULL OR OVERRIDE_FEE <= 0 THEN
                     CASE WHEN INVOICE_TOTAL IS NULL THEN
                       LEXIS_POLICY.GET_PERMIT_FEE(EXPORT_PERMIT_DETAIL_NUMBER, APPLICATION_DATE)
                     ELSE
                      INVOICE_TOTAL END
                   ELSE
                    OVERRIDE_FEE
                   END AS TOTAL_FEE
              FROM (
            SELECT  DISTINCT EPD.ORG_UNIT_NO,
                      OU.ORG_UNIT_NAME AS ORG_UNIT,
                      EEA.EXPORT_EXEMPTION_REASON_CODE,
                      EE.EXPORT_EXEMPTION_TYPE_CODE,
                      EPD.EXEMPTION_NUMBER,
                      EPD.EXPORT_PERMIT_DETAIL_NUMBER,
                      EPD.PERMIT_VOLUME,
                      EPD.APPLICATION_DATE,
                      EPIT.INVOICE_TOTAL,
                      EPIT.DOMESTIC_VALUE,
                      EPD.OVERRIDE_FEE
        FROM EXPORT_PERMIT_DETAIL EPD
       INNER JOIN EXPORT_EXEMPTION EE
          ON EPD.EXEMPTION_NUMBER = EE.EXEMPTION_NUMBER
       INNER JOIN EXPORT_EXEMPTION_APPLICATION EEA
          ON EE.EXEMPTION_NUMBER = EEA.EXEMPTION_NUMBER
       INNER JOIN ORG_UNIT OU
          ON OU.ORG_UNIT_NO = EPD.ORG_UNIT_NO
       LEFT JOIN (
              SELECT
                    EPI.EXPORT_PERMIT_DETAIL_NUMBER,
                    EPI.PERMIT_INVOICE_NUMBER,
                    EPI.SUBMIT_TIMESTAMP,
                    RANK() OVER (PARTITION BY EXPORT_PERMIT_DETAIL_NUMBER ORDER BY EPI.SUBMIT_TIMESTAMP DESC, EPI.PERMIT_INVOICE_NUMBER DESC) AS RNK,
                    EPI.INVOICE_TOTAL,
                    EPID.DOMESTIC_VALUE
              FROM EXPORT_PERMIT_INVOICE EPI
              INNER JOIN
              (SELECT SUM(ROUND(AMV_RATE * VOLUME, 2)) AS DOMESTIC_VALUE, PERMIT_INVOICE_NUMBER
              FROM EXPORT_PERMIT_INVOICE_DETAIL
              GROUP BY PERMIT_INVOICE_NUMBER) EPID ON EPID.PERMIT_INVOICE_NUMBER = EPI.PERMIT_INVOICE_NUMBER
              WHERE CANCEL_TIMESTAMP IS NULL
            ) EPIT ON EPIT.EXPORT_PERMIT_DETAIL_NUMBER = EPD.EXPORT_PERMIT_DETAIL_NUMBER AND EPIT.RNK = 1
       WHERE P_ORG_UNIT_NUMBER LIKE '%' || EPD.ORG_UNIT_NO || '%'
         AND EPD.EXPORT_PERMIT_STATUS_CODE = 'COM'
         AND EPD.EXEMPTION_NUMBER LIKE V_EXEMPTION_NUMBER
         AND EPD.EXPORT_PERMIT_ISSUE_DATE BETWEEN V_FROM_DATE AND V_TO_DATE
         AND EE.EXPORT_EXEMPTION_TYPE_CODE LIKE V_EXEMPTION_TYPE
         AND EEA.EXPORT_EXEMPTION_REASON_CODE LIKE V_EXEMPTION_REASON
         AND EEA.EXPORT_GROWTH_TYPE_CODE LIKE V_GROWTH_TYPE
    ORDER BY EPD.ORG_UNIT_NO,
             EE.EXPORT_EXEMPTION_TYPE_CODE,
             EPD.EXEMPTION_NUMBER,
             EPD.EXPORT_PERMIT_DETAIL_NUMBER);

  END FEE_SUMMARY_RPT;

  /******************************************************************************
      PROCEDURE:  FEE_SUMMARY_RPT_CSV
      PURPOSE: RETRIEVE A FEE SUMMARY REPORT THAT WILL BE OUTPUTTED AS A CSV.
  ******************************************************************************/
  PROCEDURE FEE_SUMMARY_RPT_CSV(P_WHERE_CLAUSE IN VARCHAR,
                                P_PARAMS_ARRAY IN CBR_VARCHAR2_ARRAY,
                                P_NUM_PARAMS   IN NUMBER,
                                P_FEES         IN OUT REF_CUR_GENERAL) IS
    V_FEE_SELECT   VARCHAR2(2000);
    V_FEE_ORDER_BY VARCHAR2(1000);

  BEGIN
    V_FEE_SELECT := ' SELECT ORG_UNIT_NO,' ||
                    ' ORG_UNIT,' ||
                    ' EXPORT_EXEMPTION_REASON_CODE,' ||
                    ' Export_Exemption_Type_Code,' ||
                    ' EXEMPTION_NUMBER,' ||
                    ' EPDN AS EXPORT_PERMIT_DETAIL_NUMBER,' ||
                    ' PERMIT_VOLUME,' ||
                    ' CASE' ||
                    ' WHEN DV IS NULL THEN' ||
                    ' LEXIS_POLICY.GET_PERMIT_DV(EPDN,' ||
                    ' APPLICATION_DATE)' ||
                    ' ELSE' ||
                    ' DV' ||
                    ' END AS DOMESTIC_VALUE,' ||
                    ' LEXIS.GET_PERMIT_EXPORT_VALUE(EPDN) AS EXPORT_VALUE,' ||
                    ' CASE' ||
                    ' WHEN INVOICE_TOTAL IS NULL THEN' ||
                    ' LEXIS_POLICY.GET_PERMIT_FEE(EPDN,' ||
                    ' APPLICATION_DATE)' ||
                    ' ELSE' ||
                    ' INVOICE_TOTAL' ||
                    ' END AS TOTAL_FEE' ||
                    ' FROM (SELECT DISTINCT EPD.ORG_UNIT_NO,' ||
                    ' OU.ORG_UNIT_NAME AS ORG_UNIT,' ||
                    ' EEA.export_exemption_reason_code,' ||
                    ' EE.Export_Exemption_Type_Code,' ||
                    ' EPD.EXEMPTION_NUMBER,' ||
                    ' EPD.export_permit_detail_number AS EPDN,' ||
                    ' EPD.PERMIT_VOLUME,' ||
                    ' EPD.APPLICATION_DATE,' ||
                    ' EPIT.INVOICE_TOTAL,' ||
                    ' EPIT.DV' ||
                    ' FROM EXPORT_PERMIT_DETAIL EPD' ||
                    ' INNER JOIN EXPORT_EXEMPTION EE' ||
                    ' ON EPD.EXEMPTION_NUMBER = EE.EXEMPTION_NUMBER' ||
                    ' INNER JOIN EXPORT_EXEMPTION_APPLICATION EEA' ||
                    ' ON EE.EXEMPTION_NUMBER = EEA.EXEMPTION_NUMBER' ||
                    ' INNER JOIN ORG_UNIT OU' ||
                    ' ON OU.ORG_UNIT_NO = EPD.ORG_UNIT_NO' ||
                    ' LEFT JOIN (SELECT EPI.EXPORT_PERMIT_DETAIL_NUMBER AS PDN,' ||
                    ' EPI.PERMIT_INVOICE_NUMBER,' ||
                    ' EPI.SUBMIT_TIMESTAMP,' ||
                    ' RANK() OVER(PARTITION BY EXPORT_PERMIT_DETAIL_NUMBER ORDER BY EPI.SUBMIT_TIMESTAMP DESC, EPI.PERMIT_INVOICE_NUMBER DESC) AS RNK,' ||
                    ' EPI.INVOICE_TOTAL,' ||
                    ' EPID.DV' ||
                    ' FROM EXPORT_PERMIT_INVOICE EPI' ||
                    ' INNER JOIN (SELECT SUM(ROUND(AMV_RATE * VOLUME, 2)) AS DV,' ||
                    ' PERMIT_INVOICE_NUMBER' ||
                    ' FROM EXPORT_PERMIT_INVOICE_DETAIL' ||
                    ' GROUP BY PERMIT_INVOICE_NUMBER) EPID' ||
                    ' ON EPID.PERMIT_INVOICE_NUMBER =' ||
                    ' EPI.PERMIT_INVOICE_NUMBER' ||
                    ' WHERE CANCEL_TIMESTAMP IS NULL) EPIT' ||
                    ' ON EPIT.PDN =' ||
                    ' EPD.EXPORT_PERMIT_DETAIL_NUMBER' ||
                    ' AND EPIT.RNK = 1';

    V_FEE_ORDER_BY := ' ORDER BY EPD.ORG_UNIT_NO, EE.Export_Exemption_Type_Code, EPD.EXEMPTION_NUMBER, EPD.EXPORT_PERMIT_DETAIL_NUMBER)';

    LEXIS.FIND_BY(V_FEE_SELECT || P_WHERE_CLAUSE || V_FEE_ORDER_BY,
                  P_PARAMS_ARRAY,
                  P_NUM_PARAMS,
                  P_FEES);
  END FEE_SUMMARY_RPT_CSV;

  /******************************************************************************
      PROCEDURE:  PROVINCIAL_TEAC_REPORT
      PURPOSE: RETRIEVE The Provincial Teac Report
  ******************************************************************************/
  PROCEDURE PROVINCIAL_TEAC_REPORT(P_ORG_UNIT_NUMBER       IN VARCHAR2,
                                   P_LISTING_STARTING_DATE IN number,
                                   P_TEAC_REPORT           IN OUT REF_CUR_GENERAL) IS

  BEGIN

    OPEN P_TEAC_REPORT FOR
      SELECT A.APPLICATION_NUMBER,
             CASE
               WHEN A.OWNER_CLIENT_NUMBER IS NOT NULL THEN
                E.CLIENT_NAME
               ELSE
                F.CLIENT_NAME
             END AS CLIENT_NAME,
             B.PACKAGE_NUMBER,
             RETRIEVE_SPECIES_ENDUSE(A.APPLICATION_NUMBER),
             C.EXPORT_SPECIES_CODE,
             ESC.DESCRIPTION AS SPECIES,
             C.EXPORT_GRADE_CODE,
             C.SPECIES_GRADE_VOLUME,
             A.AVERAGE_LOG_VOLUME,
             B.AVERAGE_DIAMETER,
             B.AVERAGE_LENGTH,
             D.COMPANY_NAME,
             D.PURCHASE_OFFER_AMOUNT,
             C.TIMBER_MARK,
             A.PRODUCT_LOCATION
        FROM EXPORT_EXEMPTION_APPLICATION A
       INNER JOIN EXPORT_PACKAGE B
          ON A.APPLICATION_NUMBER = B.APPLICATION_NUMBER
       INNER JOIN EXPORT_SCALE_DETAIL C
          ON C.PACKAGE_NUMBER = B.PACKAGE_NUMBER
       INNER JOIN EXPORT_SPECIES_CODE ESC
          ON C.EXPORT_SPECIES_CODE = ESC.EXPORT_SPECIES_CODE
       INNER JOIN EXPORT_PURCHASE_OFFER D
          ON D.PACKAGE_NUMBER = C.PACKAGE_NUMBER
       INNER JOIN FOREST_CLIENT E
          ON A.OWNER_CLIENT_NUMBER = E.CLIENT_NUMBER
        LEFT JOIN FOREST_CLIENT F
          ON A.AGENT_CLIENT_NUMBER = F.CLIENT_NUMBER
       INNER JOIN EXPORT_SCHEDULE G
          ON A.EXPORT_SCHEDULE_ID = G.EXPORT_SCHEDULE_ID
       WHERE G.EXPORT_SCHEDULE_ID = P_LISTING_STARTING_DATE
         and A.Export_Jurisdiction_Code = 'P'
         AND P_ORG_UNIT_NUMBER like '%' || A.ORG_UNIT_NO || '%';
  END PROVINCIAL_TEAC_REPORT;

  /******************************************************************************
      PROCEDURE:  FEDERAL_TEAC_REPORT
      PURPOSE: RETRIEVE The FEDERAL Teac Report
  ******************************************************************************/
  PROCEDURE FEDERAL_TEAC_REPORT(P_ORG_UNIT_NUMBER       IN VARCHAR2,
                                P_LISTING_STARTING_DATE IN number,
                                P_TEAC_REPORT           IN OUT REF_CUR_GENERAL) IS

  BEGIN

    OPEN P_TEAC_REPORT FOR
      SELECT A.FED_APPLICATION_NUMBER,
             CASE
               WHEN A.OWNER_CLIENT_NUMBER IS NOT NULL THEN
                E.CLIENT_NAME
               ELSE
                F.CLIENT_NAME
             END AS CLIENT_NAME,
             B.PACKAGE_NUMBER,
             H.Description as product_type,
             RETRIEVE_SPECIES_ENDUSE(A.APPLICATION_NUMBER),
             C.EXPORT_SPECIES_CODE,
             ESC.DESCRIPTION AS SPECIES,
             GTC.DESCRIPTION AS GROWTH,
             C.EXPORT_GRADE_CODE,
             C.SPECIES_GRADE_VOLUME,
             A.AVERAGE_LOG_VOLUME,
             B.AVERAGE_DIAMETER,
             B.AVERAGE_LENGTH,
             D.COMPANY_NAME,
             D.PURCHASE_OFFER_AMOUNT,
             case
               when d.offer_withdrawal_date is null then
                'N'
               else
                'Y'
             end as withdrawn,
             a.export_growth_type_code,
             C.TIMBER_MARK,
             A.PRODUCT_LOCATION
        FROM EXPORT_EXEMPTION_APPLICATION A
       INNER JOIN EXPORT_PACKAGE B
          ON A.APPLICATION_NUMBER = B.APPLICATION_NUMBER
       INNER JOIN EXPORT_SCALE_DETAIL C
          ON C.PACKAGE_NUMBER = B.PACKAGE_NUMBER
       INNER JOIN EXPORT_SPECIES_CODE ESC
          ON C.EXPORT_SPECIES_CODE = ESC.EXPORT_SPECIES_CODE
       INNER JOIN EXPORT_GROWTH_TYPE_CODE GTC
          ON A.EXPORT_GROWTH_TYPE_CODE = GTC.EXPORT_GROWTH_TYPE_CODE
       INNER JOIN EXPORT_PURCHASE_OFFER D
          ON D.PACKAGE_NUMBER = C.PACKAGE_NUMBER
       INNER JOIN FOREST_CLIENT E
          ON A.OWNER_CLIENT_NUMBER = E.CLIENT_NUMBER
        LEFT JOIN FOREST_CLIENT F
          ON A.AGENT_CLIENT_NUMBER = F.CLIENT_NUMBER
       INNER JOIN EXPORT_SCHEDULE G
          ON A.EXPORT_SCHEDULE_ID = G.EXPORT_SCHEDULE_ID
       inner join export_product_type_code H
          on a.export_product_type_code = H.Export_Product_Type_Code
       WHERE G.EXPORT_SCHEDULE_ID = P_LISTING_STARTING_DATE
         and A.Export_Jurisdiction_Code = 'F'
         AND P_ORG_UNIT_NUMBER like '%' || A.ORG_UNIT_NO || '%';
  END FEDERAL_TEAC_REPORT;

  /******************************************************************************
      PROCEDURE:  TENURE_ANALYSIS
      PURPOSE:
  ******************************************************************************/
  PROCEDURE TENURE_ANALYSIS(P_ORG_UNIT_NUMBER  IN VARCHAR2,
                            P_EXEMPTION_REASON IN EXPORT_EXEMPTION_REASON_CODE.EXPORT_EXEMPTION_REASON_CODE%TYPE,
                            P_EXEMPTION_TYPE   IN EXPORT_EXEMPTION_TYPE_CODE.EXPORT_EXEMPTION_TYPE_CODE%TYPE,
                            P_EXEMPTION_NUMBER IN EXPORT_EXEMPTION.EXEMPTION_NUMBER%TYPE,
                            P_CLIENT_NUMBER    IN VARCHAR2,
                            P_CLIENT_TYPE      IN VARCHAR2,
                            P_FROM_DATE        IN VARCHAR2,
                            P_TO_DATE          IN VARCHAR2,
                            P_TENURE_TYPE_1    IN PROV_FOREST_USE.FILE_TYPE_CODE%TYPE,
                            P_TENURE_TYPE_2    IN PROV_FOREST_USE.FILE_TYPE_CODE%TYPE,
                            P_TENURE_TYPE_3    IN PROV_FOREST_USE.FILE_TYPE_CODE%TYPE,
                            P_TENURE_TYPE_4    IN PROV_FOREST_USE.FILE_TYPE_CODE%TYPE,
                            P_TENURE_TYPE_5    IN PROV_FOREST_USE.FILE_TYPE_CODE%TYPE,
                            P_TENURE_TYPE_6    IN PROV_FOREST_USE.FILE_TYPE_CODE%TYPE,
                            P_TIMBER_MARK_1    IN HAULING_AUTHORITY.TIMBER_MARK%TYPE,
                            P_TIMBER_MARK_2    IN HAULING_AUTHORITY.TIMBER_MARK%TYPE,
                            P_TIMBER_MARK_3    IN HAULING_AUTHORITY.TIMBER_MARK%TYPE,
                            P_TIMBER_MARK_4    IN HAULING_AUTHORITY.TIMBER_MARK%TYPE,
                            P_TIMBER_MARK_5    IN HAULING_AUTHORITY.TIMBER_MARK%TYPE,
                            P_TIMBER_MARK_6    IN HAULING_AUTHORITY.TIMBER_MARK%TYPE,
                            P_FOREST_FILE_ID   IN HAULING_AUTHORITY.FOREST_FILE_ID%TYPE,
                            P_TENURE_ANALYSIS  IN OUT REF_CUR_GENERAL) IS

    V_FROM_DATE        DATE := TO_DATE('1900-01-01', 'YYYY-MM-DD');
    V_TO_DATE          DATE := TO_DATE('9999-01-01', 'YYYY-MM-DD');

    V_EXEMPTION_NUMBER VARCHAR2(12) := P_EXEMPTION_NUMBER;
    V_EXEMPTION_REASON VARCHAR2(12) := P_EXEMPTION_REASON;
    V_EXEMPTION_TYPE   VARCHAR2(12) := P_EXEMPTION_TYPE;

    V_TENURE_TYPE_1 VARCHAR2(12) := P_TENURE_TYPE_1;
    V_TENURE_TYPE_2 VARCHAR2(12) := P_TENURE_TYPE_2;
    V_TENURE_TYPE_3 VARCHAR2(12) := P_TENURE_TYPE_3;
    V_TENURE_TYPE_4 VARCHAR2(12) := P_TENURE_TYPE_4;
    V_TENURE_TYPE_5 VARCHAR2(12) := P_TENURE_TYPE_5;
    V_TENURE_TYPE_6 VARCHAR2(12) := P_TENURE_TYPE_6;

    V_TIMBER_MARK_1 VARCHAR2(12) := P_TIMBER_MARK_1;
    V_TIMBER_MARK_2 VARCHAR2(12) := P_TIMBER_MARK_2;
    V_TIMBER_MARK_3 VARCHAR2(12) := P_TIMBER_MARK_3;
    V_TIMBER_MARK_4 VARCHAR2(12) := P_TIMBER_MARK_4;
    V_TIMBER_MARK_5 VARCHAR2(12) := P_TIMBER_MARK_5;
    V_TIMBER_MARK_6 VARCHAR2(12) := P_TIMBER_MARK_6;

    V_FOREST_FILE_ID VARCHAR2(12) := P_FOREST_FILE_ID;

    V_CLIENT_PERMIT VARCHAR2(12) := '%';
    V_CLIENT_MARK   VARCHAR2(12) := '%';

    V_UNMANU_SEARCH NUMBER := 0;

  BEGIN

    IF V_EXEMPTION_NUMBER IS NULL THEN
      V_EXEMPTION_NUMBER := '%';
    END IF;

    IF V_EXEMPTION_REASON IS NULL OR V_EXEMPTION_REASON = '0' THEN
      V_EXEMPTION_REASON := '%';
    END IF;

    IF V_EXEMPTION_TYPE IS NULL THEN
      V_EXEMPTION_TYPE := '%';
    END IF;

    IF V_TENURE_TYPE_1 IS NULL THEN
      V_TENURE_TYPE_1 := '%';
    END IF;

    IF V_TENURE_TYPE_2 IS NULL THEN
      V_TENURE_TYPE_2 := '';
    END IF;

    IF V_TENURE_TYPE_3 IS NULL THEN
      V_TENURE_TYPE_3 := '';
    END IF;

    IF V_TENURE_TYPE_4 IS NULL THEN
      V_TENURE_TYPE_4 := '';
    END IF;

    IF V_TENURE_TYPE_5 IS NULL THEN
      V_TENURE_TYPE_5 := '';
    END IF;

    IF V_TENURE_TYPE_6 IS NULL THEN
      V_TENURE_TYPE_6 := '';
    END IF;

    IF V_TIMBER_MARK_1 IS NULL THEN
      V_TIMBER_MARK_1 := '%';
      V_UNMANU_SEARCH := 1;
    ELSIF V_TIMBER_MARK_1 = 'UNMANU' THEN
      V_UNMANU_SEARCH := 1;
    END IF;

    IF V_TIMBER_MARK_2 IS NULL THEN
      V_TIMBER_MARK_2 := '';
    ELSIF V_TIMBER_MARK_2 = 'UNMANU' THEN
      V_UNMANU_SEARCH := 1;
    END IF;

    IF V_TIMBER_MARK_3 IS NULL THEN
      V_TIMBER_MARK_3 := '';
    ELSIF V_TIMBER_MARK_3 = 'UNMANU' THEN
      V_UNMANU_SEARCH := 1;
    END IF;

    IF V_TIMBER_MARK_4 IS NULL THEN
      V_TIMBER_MARK_4 := '';
    ELSIF V_TIMBER_MARK_4 = 'UNMANU' THEN
      V_UNMANU_SEARCH := 1;
    END IF;

    IF V_TIMBER_MARK_5 IS NULL THEN
      V_TIMBER_MARK_5 := '';
    ELSIF V_TIMBER_MARK_5 = 'UNMANU' THEN
      V_UNMANU_SEARCH := 1;
    END IF;

    IF V_TIMBER_MARK_6 IS NULL THEN
      V_TIMBER_MARK_6 := '';
    ELSIF V_TIMBER_MARK_6 = 'UNMANU' THEN
      V_UNMANU_SEARCH := 1;
    END IF;

    IF V_FOREST_FILE_ID IS NULL THEN
      V_FOREST_FILE_ID := '%';
    END IF;

    IF P_CLIENT_TYPE = 'P' THEN
      V_CLIENT_PERMIT := P_CLIENT_NUMBER;
      V_CLIENT_MARK   := '';
    END IF;

    IF P_CLIENT_TYPE = 'M' THEN
      V_CLIENT_PERMIT := '';
      V_CLIENT_MARK   := P_CLIENT_NUMBER;
    END IF;

    IF P_FROM_DATE IS NOT NULL THEN
      V_FROM_DATE := TO_DATE(P_FROM_DATE, 'YYYY-MM-DD');
    END IF;

    IF P_TO_DATE IS NOT NULL THEN
      V_TO_DATE := TO_DATE(P_TO_DATE, 'YYYY-MM-DD');
    END IF;

    OPEN P_TENURE_ANALYSIS FOR
    SELECT
        EXEMPTION_REASON_DESC,
        EXEMPTION_NUMBER,
        ORG_UNIT_NAME,
        TENURE_TYPE,
        TENURE_DESC,
        FOREST_FILE_ID,
        TIMBER_MARK,
        MARK_CLIENT_NUMBER,
        CLIENT_NAME,
        LEGAL_FIRST_NAME,
        LEGAL_MIDDLE_NAME,
        SUM(SPECIES_GRADE_VOLUME) AS SPECIES_GRADE_VOLUME,
        EXPORT_PERMIT_DETAIL_NUMBER
      FROM(
       SELECT EEA.EXPORT_EXEMPTION_REASON_CODE,
             EERC.DESCRIPTION AS EXEMPTION_REASON_DESC,
             EPD.EXEMPTION_NUMBER,
             OU.ORG_UNIT_NAME,
             EPD.ORG_UNIT_NO,
             NVL(PFU.FILE_TYPE_CODE, ' ') AS TENURE_TYPE,
             NVL(FTC.DESCRIPTION, ' ') AS TENURE_DESC,
             HA.FOREST_FILE_ID,
             ESD.TIMBER_MARK,
             FC.CLIENT_NUMBER AS MARK_CLIENT_NUMBER,
             FC.CLIENT_NAME,
             FC.LEGAL_FIRST_NAME,
             FC.LEGAL_MIDDLE_NAME,
             FC.CLIENT_TYPE_CODE,
             ESD.PACKAGE_NUMBER,
             ESD.EXPORT_SPECIES_CODE,
             ESD.EXPORT_GRADE_CODE,
             ESD.SPECIES_GRADE_VOLUME,
             EPD.EXPORT_PERMIT_DETAIL_NUMBER,
             EPD.CLIENT_NUMBER AS PERMIT_CLIENT_NUMBER,
             FC2.CLIENT_NAME AS PERMIT_CLIENT_NAME,
             EPD.PERMIT_VOLUME AS PERMIT_VOLUME
        FROM EXPORT_PERMIT_DETAIL EPD
       INNER JOIN EXPORT_EXEMPTION EE on EE.Exemption_Number = EPD.Exemption_Number
       INNER JOIN EXPORT_EXEMPTION_APPLICATION EEA
          ON EE.Exemption_Number = EEA.Exemption_Number
       INNER JOIN EXPORT_EXEMPTION_REASON_CODE EERC
          ON EEA.EXPORT_EXEMPTION_REASON_CODE =
             EERC.EXPORT_EXEMPTION_REASON_CODE
       INNER JOIN ORG_UNIT OU
          ON EPD.ORG_UNIT_NO = OU.ORG_UNIT_NO
       INNER JOIN EXPORT_SCALE_DETAIL ESD
          ON EPD.EXPORT_PERMIT_DETAIL_NUMBER = ESD.EXPORT_PERMIT_DETAIL_NUMBER
       INNER JOIN HAULING_AUTHORITY HA
          ON ESD.TIMBER_MARK = HA.TIMBER_MARK
       INNER JOIN PROV_FOREST_USE PFU
          ON HA.FOREST_FILE_ID = PFU.FOREST_FILE_ID
       INNER JOIN FILE_TYPE_CODE FTC
          ON FTC.FILE_TYPE_CODE = PFU.FILE_TYPE_CODE
       INNER JOIN FOR_CLIENT_LINK FCL
          ON PFU.FOREST_FILE_ID = FCL.FOREST_FILE_ID
       INNER JOIN FOREST_CLIENT FC
          ON FCL.CLIENT_NUMBER = FC.CLIENT_NUMBER
       INNER JOIN FOREST_CLIENT FC2
          ON EPD.CLIENT_NUMBER = FC2.CLIENT_NUMBER
       INNER JOIN EXPORT_PACKAGE EP on EP.Application_Number = EEA.Application_Number and esd.package_number = EP.Package_Number
       WHERE EPD.EXPORT_PERMIT_STATUS_CODE IN ('COM')
         AND FCL.FILE_CLIENT_TYPE = 'A'
            -- issue date
         and EPD.EXPORT_PERMIT_ISSUE_DATE between V_FROM_DATE AND V_TO_DATE
            -- region
         AND (P_ORG_UNIT_NUMBER like '%' || EPD.ORG_UNIT_NO || '%' OR P_ORG_UNIT_NUMBER = '-1')
            -- exemption reason
         AND EEA.EXPORT_EXEMPTION_REASON_CODE LIKE V_EXEMPTION_REASON
            -- exemption type
         AND EE.EXPORT_EXEMPTION_TYPE_CODE LIKE V_EXEMPTION_TYPE
            -- exemption number
         AND EEA.EXEMPTION_NUMBER LIKE V_EXEMPTION_NUMBER
            -- client number on permit or client number on mark
         AND (EPD.CLIENT_NUMBER LIKE V_CLIENT_PERMIT OR
             FC.CLIENT_NUMBER LIKE V_CLIENT_MARK)
            -- tenure type
         AND (PFU.FILE_TYPE_CODE LIKE V_TENURE_TYPE_1 OR
             PFU.FILE_TYPE_CODE = V_TENURE_TYPE_2 OR
             PFU.FILE_TYPE_CODE = V_TENURE_TYPE_3 OR
             PFU.FILE_TYPE_CODE = V_TENURE_TYPE_4 OR
             PFU.FILE_TYPE_CODE = V_TENURE_TYPE_5 OR
             PFU.FILE_TYPE_CODE = V_TENURE_TYPE_6)
            -- TIMBER MARK
         AND (HA.TIMBER_MARK LIKE V_TIMBER_MARK_1 OR
             HA.TIMBER_MARK = V_TIMBER_MARK_2 OR
             HA.TIMBER_MARK = V_TIMBER_MARK_3 OR
             HA.TIMBER_MARK = V_TIMBER_MARK_4 OR
             HA.TIMBER_MARK = V_TIMBER_MARK_5 OR
             HA.TIMBER_MARK = V_TIMBER_MARK_6)
            -- FOREST FILE ID
         AND HA.FOREST_FILE_ID LIKE V_FOREST_FILE_ID
      UNION ALL
      SELECT distinct EEA.EXPORT_EXEMPTION_REASON_CODE,
                      EERC.DESCRIPTION AS EXEMPTION_REASON_DESC,
                      EPD.EXEMPTION_NUMBER,
                      OU.ORG_UNIT_NAME,
                      EPD.ORG_UNIT_NO,
                      'N/A' AS TENURE_TYPE,
                      '' AS TENURE_DESC,
                      'N/A' AS FOREST_FILE_ID,
                      'UNMANU' as TIMBER_MARK,
                      'N/A' AS MARK_CLIENT_NUMBER,
                      '' AS CLIENT_NAME,
                      '' AS LEGAL_FIRST_NAME,
                      '' AS LEGAL_MIDDLE_NAME,
                      'N/A' AS CLIENT_TYPE_CODE,
                      ESD.PACKAGE_NUMBER,
                      ESD.EXPORT_SPECIES_CODE,
                      ESD.EXPORT_GRADE_CODE,
                      ESD.SPECIES_GRADE_VOLUME,
                      EPD.EXPORT_PERMIT_DETAIL_NUMBER,
                      EPD.CLIENT_NUMBER AS PERMIT_CLIENT_NUMBER,
                      fc.client_name AS PERMIT_CLIENT_NAME,
                      EPD.PERMIT_VOLUME AS PERMIT_VOLUME
        FROM EXPORT_PERMIT_DETAIL EPD
        INNER JOIN EXPORT_EXEMPTION EE ON EPD.Exemption_Number = EE.Exemption_Number
       INNER JOIN EXPORT_EXEMPTION_APPLICATION EEA
          ON EE.Exemption_Number = EEA.Exemption_Number
       INNER JOIN EXPORT_EXEMPTION_REASON_CODE EERC
          ON EEA.EXPORT_EXEMPTION_REASON_CODE =
             EERC.EXPORT_EXEMPTION_REASON_CODE
       INNER JOIN ORG_UNIT OU
          ON EPD.ORG_UNIT_NO = OU.ORG_UNIT_NO
       INNER JOIN EXPORT_SCALE_DETAIL ESD
          ON EPD.EXPORT_PERMIT_DETAIL_NUMBER = ESD.EXPORT_PERMIT_DETAIL_NUMBER
       inner join forest_client fc
          on fc.client_number = epd.client_number
       WHERE EPD.EXPORT_PERMIT_STATUS_CODE IN ('COM')
            -- issue date
         and EPD.EXPORT_PERMIT_ISSUE_DATE between V_FROM_DATE AND V_TO_DATE
            -- region
         AND (P_ORG_UNIT_NUMBER like '%' || EEA.ORG_UNIT_NO || '%' OR P_ORG_UNIT_NUMBER = '-1')
            -- exemption reason
         AND EEA.EXPORT_EXEMPTION_REASON_CODE LIKE V_EXEMPTION_REASON
            -- exemption type
         AND EE.EXPORT_EXEMPTION_TYPE_CODE LIKE V_EXEMPTION_TYPE
            -- exemption number
         AND EEA.EXEMPTION_NUMBER LIKE V_EXEMPTION_NUMBER
            -- client number on permit
         AND EPD.CLIENT_NUMBER LIKE V_CLIENT_PERMIT
            -- UNAMNU Timber Mark
         and esd.timber_mark is null
         and 1 = V_UNMANU_SEARCH

         )TT
         GROUP BY  EXEMPTION_REASON_DESC,
                      EXEMPTION_NUMBER,
                      ORG_UNIT_NAME,
                      TENURE_TYPE,
                      TENURE_DESC,
                      FOREST_FILE_ID,
                      TIMBER_MARK,
                      MARK_CLIENT_NUMBER,
                      CLIENT_NAME,
                      LEGAL_FIRST_NAME,
                      LEGAL_MIDDLE_NAME,
                      EXPORT_PERMIT_DETAIL_NUMBER;

  END TENURE_ANALYSIS;

  /******************************************************************************
      PROCEDURE:  TENURE_ANALYSIS_CSV
      PURPOSE: RETRIEVE A TENURE ANALYSIS THAT WILL BE OUTPUTTED AS A CSV.
  ******************************************************************************/
  PROCEDURE TENURE_ANALYSIS_CSV(P_WHERE_CLAUSE IN VARCHAR,
                                P_PARAMS_ARRAY IN CBR_VARCHAR2_ARRAY,
                                P_NUM_PARAMS   IN NUMBER,
                                P_FEES         IN OUT REF_CUR_GENERAL) IS
    V_TENURE_SELECT VARCHAR2(4000);

  BEGIN

    V_TENURE_SELECT := 'SELECT EEA.EXPORT_EXEMPTION_REASON_CODE, ' ||
                       'EERC.DESCRIPTION AS EXEMPTION_REASON_DESC, ' ||
                       'EPD.EXEMPTION_NUMBER, ' || 'OU.ORG_UNIT_NAME, ' ||
                       'EPD.ORG_UNIT_NO, ' ||
                       'NVL(PFU.FILE_TYPE_CODE, '' '') AS TENURE_TYPE, ' ||
                       'HA.FOREST_FILE_ID, ' || 'ESD.TIMBER_MARK, ' ||
                       'FC.CLIENT_NUMBER AS MARK_CLIENT_NUMBER, ' ||
                       'FC.CLIENT_NAME, ' || 'FC.LEGAL_FIRST_NAME, ' ||
                       'FC.LEGAL_MIDDLE_NAME, ' || 'FC.CLIENT_TYPE_CODE, ' ||
                       'ESD.PACKAGE_NUMBER, ' ||
                       'ESD.EXPORT_SPECIES_CODE, ' ||
                       'ESD.EXPORT_GRADE_CODE, ' ||
                       'ESD.SPECIES_GRADE_VOLUME, ' ||
                       'EPD.EXPORT_PERMIT_DETAIL_NUMBER, ' ||
                       'EPD.CLIENT_NUMBER AS PERMIT_CLIENT_NUMBER, ' ||
                       'FC2.CLIENT_NAME AS PERMIT_CLIENT_NAME ' ||
                       'FROM EXPORT_PERMIT_DETAIL EPD ' ||
                       'INNER JOIN EXPORT_PACKAGE EP ON EP.EXPORT_PERMIT_DETAIL_NUMBER = EPD.EXPORT_PERMIT_DETAIL_NUMBER ' ||
                       'INNER JOIN EXPORT_EXEMPTION_APPLICATION EEA ON EP.APPLICATION_NUMBER = EEA.APPLICATION_NUMBER ' ||
                       'INNER JOIN EXPORT_EXEMPTION_REASON_CODE EERC ON EEA.EXPORT_EXEMPTION_REASON_CODE = EERC.EXPORT_EXEMPTION_REASON_CODE ' ||
                       'INNER JOIN ORG_UNIT OU ON EPD.ORG_UNIT_NO = OU.ORG_UNIT_NO ' ||
                       'INNER JOIN EXPORT_SCALE_DETAIL ESD ON EP.PACKAGE_NUMBER = ESD.PACKAGE_NUMBER ' ||
                       'INNER JOIN HAULING_AUTHORITY HA ON ESD.TIMBER_MARK = HA.TIMBER_MARK ' ||
                       'INNER JOIN PROV_FOREST_USE PFU ON HA.FOREST_FILE_ID = PFU.FOREST_FILE_ID ' ||
                       'INNER JOIN FOR_CLIENT_LINK FCL ON PFU.FOREST_FILE_ID = FCL.FOREST_FILE_ID ' ||
                       'INNER JOIN FOREST_CLIENT FC ON FCL.CLIENT_NUMBER = FC.CLIENT_NUMBER ' ||
                       'INNER JOIN FOREST_CLIENT FC2 ON EPD.CLIENT_NUMBER = FC2.CLIENT_NUMBER ' ||
                       'INNER JOIN EXPORT_EXEMPTION EE ON EEA.EXEMPTION_NUMBER = EE.EXEMPTION_NUMBER ';

    LEXIS.FIND_BY(V_TENURE_SELECT || P_WHERE_CLAUSE,
                  P_PARAMS_ARRAY,
                  P_NUM_PARAMS,
                  P_FEES);
  END TENURE_ANALYSIS_CSV;

END LEXIS_REPORTING;
/
