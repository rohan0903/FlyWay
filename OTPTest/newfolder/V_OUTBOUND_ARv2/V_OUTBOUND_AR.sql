CREATE OR REPLACE VIEW SOWGBSDBA01.V_OUTBOUND_AR AS
SELECT je.run_seq_id,
  je.invoice_prefix_seq_id,
  je.in_sending_gl,
  je.je_line_seq_id,
  je.brin_op_gl op_gl_system,
  je.brin_balancing_segment balancing_segment,
   CASE
      WHEN je.in_sending_gl  in ('AHB','ARS','SVB','ESB','SRS','ERS')
      THEN ''
      ELSE je.brin_reference_id
    END as reference_id,
  inh.billing_ref invoice_billing_ref,
  je.brin_cost_center cost_center,
  je.project project,
  je.outbound_id,
  je.billing_curr original_curr,
  je.billing_amt original_amt,
  (lineHis.tax_comp_amt + lineHis.ntax_comp_amt) invoiceAmount,
  je.run_rec_number acc_record_number,
  je.billing_rel_id,
  je.gl_account expense_account,
  je.je_line__type_sub_cat expense_category,
  ru.run_date biller_run_date,
  NVL(je.vat_percentage,0) vat_percentage,
  NVL(je.tp_percentage,0) tp_percentage,
  je.je_line_type_cat,
  je.je_line_type_sub_sub_cat,
  je.je_line_tax_type,
  je.je_line_type_interco,
  je.src_je_line_seq_id,
  je.tp_type,
  je.invoice_prefix_seq_id je_invoice_prefix,
  inv.nickname invoice_prefix_nickname,
  inv.invoice_type,
  inv.nickname
  ||inv.invoice_type
  ||'-'
  ||inv.lst_invoice_nbr invoice_number,
  je.is_taxable,
  CASE
    WHEN je.in_sending_gl='AHB'
    THEN split(je.in_je_description,'|',5)
    WHEN v_pl.route_type='POP'
    THEN 'http://sc.ge.com/@gbsbillerbuyer'
    ELSE 'www.gemoves.com'
  END invoiceContact,
  v_pl.route_type
  ||'-'
  ||v_pl.route_id
  ||'-'
  ||Lpad(v_pl.route_version,2,'0')
  ||'-'
  ||v_pl.bill_rel_key AS bill_rel_key,
  je.je_type,
  NVL(jerec.in_je_description, '') AS bex_je_description,
  NVL(jerec.gl_account, '')        AS orig_gl_account,
  NVL(jerec.brin_cost_center, '')  AS seller_cost_center,
  (
  CASE
    WHEN je.je_line_type_cat     = 'AR'
    AND je.je_line__type_sub_cat ='DR4'
    THEN SUBSTR(b_bal_seg,0,6)
    WHEN je.je_line_type_cat     = 'AR'
    AND je.je_line__type_sub_cat ='ACA'
    AND je.je_line_type_id       ='CRE001'
    THEN 'GBUS01'
    WHEN je.je_line_type_cat     = 'AR'
    AND je.je_line__type_sub_cat ='ACA'
    THEN SUBSTR(b_bal_seg,0,6)
    WHEN je.je_line_type_cat     = 'AR'
    AND je.je_line__type_sub_cat ='ESB'
    THEN V_PL.B_ME
    WHEN je.je_line_type_cat     = 'BEX'
    AND je.je_line__type_sub_cat ='ESB'
    THEN V_PL.B_ME
    WHEN je.je_line_type_cat      = 'AR'
    AND je.je_line__type_sub_cat !='IBS'
    AND V_PL.B_ME                != 'EXTERN'
    THEN V_PL.B_ME
    WHEN je.je_line_type_cat     = 'AP'
    AND je.je_line__type_sub_cat ='DR4'
    THEN V_PL.S_ME
    WHEN je.je_line_type_cat     = 'AP'
    AND je.je_line__type_sub_cat ='ACA'
    AND je.je_line_type_id       ='CRE002'
    THEN SUBSTR(jerec.brin_balancing_segment,0,6)
    WHEN je.je_line_type_cat     ='AP'
    AND je.je_line__type_sub_cat ='ACA'
    THEN SUBSTR(S_BAL_SEG,0,6)
    WHEN je.je_line_type_cat      = 'AP'
    AND je.je_line__type_sub_cat !='IBS'
    AND V_PL.B_ME                != 'EXTERN'
    THEN SUBSTR(S_BAL_SEG,0,6)
    ELSE ''
  END )AS ime,
  (
  CASE
    WHEN je.je_line_type_cat     = 'AR'
    AND je.je_line__type_sub_cat ='DR4'
    THEN SUBSTR(b_bal_seg,7,6)
    WHEN je.je_line_type_cat     = 'AR'
    AND je.je_line__type_sub_cat ='ACA'
    AND je.je_line_type_id       ='CRE001'
    AND V_PL.S_LE                ='A01098'
    THEN 'M45000'
    WHEN je.je_line_type_cat     = 'AR'
    AND je.je_line__type_sub_cat ='ACA'
    AND je.je_line_type_id       ='CRE001'
    THEN 'E29000'
    WHEN je.je_line__type_sub_cat ='ACA'
    AND je.je_line_type_cat       = 'AR'
    THEN SUBSTR(b_bal_seg,7,6)
    WHEN je.je_line__type_sub_cat ='ESB'
    AND je.je_line_type_cat       = 'AR'
    THEN NVL ( v_pl.B_BUC_MARS_LE,v_pl.b_le)
    WHEN je.je_line__type_sub_cat ='ESB'
    AND je.je_line_type_cat       = 'BEX'
    THEN NVL ( v_pl.B_BUC_MARS_LE,v_pl.b_le)
    WHEN je.je_line_type_cat     = 'AP'
    AND je.je_line__type_sub_cat ='ACA'
    AND je.je_line_type_id       ='CRE002'
    THEN SUBSTR(jerec.brin_balancing_segment,7,6)
    WHEN je.je_line__type_sub_cat ='ACA'
    AND je.je_line_type_cat       ='AP'
    THEN SUBSTR(S_BAL_SEG,7,6)
    ELSE ''
  END)             AS ile,
  jeVat.gl_account AS account_vat,
  v_pl.consumption_per,
  v_pl.S_BUC,
  v_pl.B_BUC,
  v_pl. BUYER_CCL_BILL_TO_SITE CCL_BILL_TO_SITE,
  V_PL.s_gl,
  V_PL.b_gl,
  V_PL.S_BAL_SEG,
  V_PL.B_BAL_SEG,
  CASE
    WHEN inh.settlement_type ='IBS'
    THEN 'IBS  '
      ||inh.from_buc
      ||' '
      ||inh.to_buc
    WHEN inh.settlement_type ='NON-IBS'
    THEN 'Due upon receipt.  Please send payment directly to our bank. '
      ||inh.bic
      ||' '
      ||inh.iban
    ELSE 'NON IBS, ACA'
  END invoiceHeader,
  v_pl.plcontact,
  NVL(v_pl.max_unit_price, 0) max_unit_price,
  v_pl.country_code,
  v_pl.country_code_iso3166,
  v_pl.cclArHeaderDffContext,
  costCenter.product_line,
  costcenterTp.product_line_tp,
   v_pl.buc,
   v_pl.adn,
   v_pl.ccl_ar_ibs,
   v_pl.use_global_cust_site,
   tradingPartner.tp_required,
   v_pl.CCL_AR_INTERFACE_REQUIRED,/*RITM15507379*/
   v_pl.SETTLEMENT_TYPE/*RITM15507379*/,
   je.SERVICE_CODE, /*RITM16419869*/
   fserv.sbx_commodity_code, /*RITM16419869*/
   v_pl.SABRIX_TAX_TYPE /*RITM16419869*/,
   v_pl.SELLER_VAT_REGIME /*RITM15507414*/
FROM t_je_line_his je
INNER JOIN v_pl_outbound_ar v_pl
ON je.billing_rel_id = v_pl.billing_rel_id
INNER JOIN t_run ru
ON je.run_seq_id = ru.run_seq_id
AND je.created_on BETWEEN ru.run_date-1 AND ru.run_date+1
INNER JOIN t_invoice_prefix_his inv
ON je.invoice_prefix_seq_id = inv.invoice_prefix_seq_id
INNER JOIN t_invoice_header inh
ON inh.invoice_prefix_seq_id = inv.invoice_prefix_seq_id
INNER JOIN t_invoice_line_his lineHis
ON lineHis.Invoice_Prefix_Seq_Id=inv.invoice_prefix_seq_id
AND inv_line_type = 'HEADER'
LEFT JOIN t_je_line_his jerec
ON je.src_je_line_seq_id = jerec.je_line_seq_id
AND jerec.je_type       IN ('INC', 'INCB', 'CRN', 'CRNB')
AND je.je_type           ='REC'
LEFT JOIN t_je_line_his jeVat
ON jeVat.je_type             ='VAT'
AND je.invoice_prefix_seq_id = jeVat.invoice_prefix_seq_id
AND je.billing_rel_id        = jeVat.billing_rel_id
LEFT JOIN (select acct_number, max(tp_required) tp_required
               from T_GL_ACCOUNT gla
              where gla.is_active = 'Y'
              group by acct_number) tradingPartner
    ON tradingPartner.acct_number = je.gl_account
LEFT JOIN
  (select cost_center, product_line
       from (select cost_center,
                    product_line,
                    row_number() over(partition by cost_center order by product_line) rn
               from t_ccl_cost_center)
      where rn = 1
  ) costCenter ON costCenter.cost_center=je.brin_cost_center
  LEFT JOIN
  (select cost_center, product_line as product_line_tp
       from (select cost_center,
                    product_line,
                    row_number() over(partition by cost_center order by product_line) rn
               from t_ccl_cost_center)
      where rn = 1
  ) costCenterTp ON costCenterTp.cost_center=je.brin_cost_center
  and je.je_type = 'TP'
  left join FPAT_SERVICE fserv /*RITM16419869*/
  on je.SERVICE_CODE= fserv.code and fserv.is_active_ind = 'Y' and fserv.code is not null /*RITM16419869*/
WHERE LENGTH (je.brin_op_gl) = 3
AND NVL(je.SHADOW_IND,'N')   ='N'
AND je.je_type              IN ( 'REC', 'TP', 'AR');
