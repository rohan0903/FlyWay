CREATE OR REPLACE VIEW SOWGBSDBA01.V_PL_OUTBOUND_AR AS
SELECT bro.route_type,
       bro.route_id,
       bro.route_version,
       br.billing_rel_id,
       br.relationship_ind bill_rel_key,
       NVL(br.consumption_per, 1) consumption_per,
       CASE
         WHEN u.first_name IS NULL THEN
          ''
         ELSE
          u.first_name || ' ' || u.last_name || ' (' || u.work_id || ')'
       END plContact,
       SELLER_BU.BUC S_BUC,
       BUYER_MG.MARS_MGMT_ENTITY B_ME,
       SELLER_MG.MARS_MGMT_ENTITY S_ME,
       BUYER_BU.BUC B_BUC,
       BUYER_BU.MARS_LE B_BUC_MARS_LE,
       BUYER_LE.GOLD_ID B_LE,
       SELLER_LE.GOLD_ID S_LE,
       p.CCL_BILL_TO_SITE,
       buyer_p.CCL_BILL_TO_SITE BUYER_CCL_BILL_TO_SITE,
       p.Gl_Identifier s_gl,
       buyer_p.Gl_Identifier b_gl,
       DECODE(BR.BUYER_ROLE, NULL, ' ', buyer_p.BALANCING_SEGMENT) B_BAL_SEG,
       DECODE(BR.SELLER_ROLE, NULL, ' ', p.BALANCING_SEGMENT) S_BAL_SEG,
       p.max_unit_price AS max_unit_price,
       SELLER_CO.country_code,
       country.country_code_iso3 AS country_code_iso3166,
       country.ccl_ar_header_dff_context cclArHeaderDffContext,
       buyer_p.pl_seq_id as buyer_pl,
       seller_pl.pl_seq_id as seller_pl,
       bro.buc,
       bro.adn,
       p.ccl_ar_ibs,
       p.use_global_cust_site,
	   p.CCL_AR_INTERFACE_REQUIRED,/*RITM15507379*/
	   BR.SETTLEMENT_TYPE/*RITM15507379*/,
	   --VAT_GST.SABRIX_TAX_TYPE /*RITM16419869*/,
	   nvl(country.IS_VAT_REGIME, 'Y') SELLER_VAT_REGIME/*RITM15507414*/
  FROM SOWGBSDBA01.T_BILLING_ROUTE BRO
 INNER JOIN SOWGBSDBA01.T_BILLING_REL BR
    ON BR.BILLING_ROUTE_ID = BRO.BILLING_ROUTE_ID
 INNER JOIN SOWGBSDBA01.T_PL_HIS SELLER_PL
    ON BR.SELLER_PL_SEQ_ID = SELLER_PL.PL_HIS_ID
 INNER JOIN SOWGBSDBA01.T_MGMT_ENTITY_HIS SELLER_MG
    ON SELLER_PL.MGMT_ENTITY_HIS_ID = SELLER_MG.MGMT_ENTITY_HIS_ID
 INNER JOIN SOWGBSDBA01.T_PL_HIS BUYER_PL
    ON BR.BUYER_PL_SEQ_ID = BUYER_PL.PL_HIS_ID
 INNER JOIN pl buyer_p
    ON buyer_p.pl_seq_id = BUYER_PL.pl_seq_id
 INNER JOIN SOWGBSDBA01.T_LEGAL_ENTITY_HIS SELLER_LE
    ON SELLER_PL.LEGAL_ENTITY_HIS_ID = SELLER_LE.LEGAL_ENTITY_HIS_ID
 INNER JOIN SOWGBSDBA01.T_COUNTRY_HIS SELLER_CO
    ON SELLER_LE.COUNTRY_HIS_ID = SELLER_CO.COUNTRY_HIS_ID
   AND SELLER_CO.DECIMALS IS NOT NULL
 INNER JOIN SOWGBSDBA01.COUNTRY country
    ON country.country_id = seller_co.country_id
 INNER JOIN pl p
    ON p.pl_seq_id = SELLER_PL.pl_seq_id
   /*AND p.CCL_AR_INTERFACE_REQUIRED = 'Y'  --RITM15507379*/
   AND ((p.CCL_AR_INTERFACE_REQUIRED = 'Y') or (p.CCL_AR_INTERFACE_REQUIRED = 'O' and BR.SETTLEMENT_TYPE = 'OTH'))/*RITM15507379*/
  LEFT JOIN (SELECT MAX(pc.user_seq_id) user_seq_id, pc.pl_seq_id
               FROM t_pl_contact pc
              WHERE pc.contact_type = 'GBS Contact'
                AND pc.contact_order = 1
                AND pc.is_active = 'Y'
              GROUP BY pc.pl_seq_id) pc
    ON pc.pl_seq_id = p.pl_seq_id
  LEFT JOIN gbsone_user u
    ON u.user_seq_id = pc.user_seq_id
 INNER JOIN SOWGBSDBA01.T_PL_HIS BUYER_PL
    ON BR.BUYER_PL_SEQ_ID = BUYER_PL.PL_HIS_ID
 INNER JOIN SOWGBSDBA01.T_MGMT_ENTITY_HIS BUYER_MG
    ON BUYER_PL.MGMT_ENTITY_HIS_ID = BUYER_MG.MGMT_ENTITY_HIS_ID
 INNER JOIN SOWGBSDBA01.T_LEGAL_ENTITY_HIS BUYER_LE
    ON BUYER_PL.LEGAL_ENTITY_HIS_ID = BUYER_LE.LEGAL_ENTITY_HIS_ID
 INNER JOIN SOWGBSDBA01.T_BUC SELLER_BU
    ON BR.SELLER_BUC_ID = SELLER_BU.BUC_ID
 LEFT JOIN SOWGBSDBA01.T_BUC BUYER_BU
    ON BR.BUYER_BUC_ID = BUYER_BU.BUC_ID
 --LEFT JOIN SOWGBSDBA01.T_VAT_GST VAT_GST on BR.BILLING_REL_ID = VAT_GST.BILLING_REL_ID and VAT_GST.VAT_GST_TYPE = 'SELLER'/*RITM16419869*/
 ;
