CREATE OR REPLACE PROCEDURE SOWGBSDBA01.CCL_FILE_AR_REC_GL_ES(dir      in varchar2,
                                                              filename in varchar2,
                                                              ftpdir   in varchar2,
                                                              usr      in varchar2,
                                                              pass     in varchar2,
                                                              to_path  in varchar2,
                                                              run      in number) as
  ftype          utl_file.file_type;
  trailer_buffer varchar2(255);
  numElems       number;
  numRows        number;
  numRowsNumBrinRefId number;
  fileDate       varchar2(255);
  runDate        varchar2(255);
  runType        varchar2(255);
  contractNumber varchar2(60);
  customerSite   varchar2(60);
  rusiaError     varchar2(20) := 'false';
  custSiteError  varchar2(20) := 'false';
  v_invoice      v_outbound_ar%rowtype;
  elems          NUMBER;
  tax1           NUMBER;
  tax2           NUMBER;
  tax3           NUMBER;
  rusiaErrorDetail     varchar2(4000);
  acctNumberList       varchar2(4000);
  CURSOR countryC IS
    SELECT DISTINCT c.country_id, c.country_code
    FROM t_invoice_line_his h
   INNER JOIN t_invoice_prefix_his i
    ON i.invoice_prefix_seq_id = h.invoice_prefix_seq_id
     AND i.invoice_type != 'M'
   INNER JOIN pl p
    ON p.pl_seq_id = i.pl_seq_id
   INNER JOIN legal_entity LE
    ON le.Legal_Entity_Seq_Id = p.legal_entity_seq_id
   INNER JOIN country c
    ON c.country_id = le.country_id
   WHERE (p.gl_identifier = 'CCL')
     and (p.ccl_ar_interface_required = 'Y' or p.ccl_ar_interface_required = 'O')
     AND (h.service_desc IS NOT NULL)
     AND (h.run_seq_id = run);

  amount   NUMBER;
  quantity number;
  l_chinese_chars  varchar2(10);

begin
  select value
    into l_chinese_chars
    from t_look_up
   where type = 'OUTBOUND'
     and look_code = 'CCL_AR'
     and look_name = 'CHINESE_CHAR';

   select '|'||LISTAGG(acct_number, '|') WITHIN GROUP(ORDER BY acct_number)||'|'
   into acctNumberList
    from (select acct_number
            from T_GL_ACCOUNT gla
           where gla.is_active = 'Y'
             and tp_required in ('Y', 'T')
           group by acct_number);


  FOR countryRecord IN countryC LOOP

  -- *************************************************************************************
      -- Get all reference ID if an invoice document has various  reference ID for RU country
  -- *************************************************************************************

      IF (countryRecord.Country_Code = 'RU' ) THEN
      rusiaErrorDetail := '';
      numRowsNumBrinRefId:=0;
           for contractNumBrinRefId in ( with runDate as
                                     (select to_char(trunc(run_date), 'YYYYMMdd') rundate
                                        from t_run
                                       where run_Seq_id = run),
                                    CUSTOMDATE AS
                                     (SELECT TO_DATE((select rundate from runDate) || ' 00:00:00',
                                                     'YYYYMMdd hh24:mi:ss') AS FROM_,
                                             TO_DATE((select rundate from runDate) || ' 23:59:59',
                                                     'YYYYMMdd hh24:mi:ss') AS TO_
                                        from DUAL),
                                    invoicesRusia as
                                     (select i.invoice_prefix_seq_id AS idInvoice,
                                             o.brin_reference_id,
                                             max(i.nickname || i.invoice_type || '-' || i.lst_invoice_nbr) documentNumber
                                        FROM t_invoice_line_his h
                                       INNER JOIN t_invoice_prefix_his i
                                          ON i.invoice_prefix_seq_id = h.invoice_prefix_seq_id
                                         AND i.invoice_type != 'M'
                                       INNER JOIN pl p
                                          ON p.pl_seq_id = i.pl_seq_id
                                       INNER JOIN legal_entity LE
                                          ON le.Legal_Entity_Seq_Id = p.legal_entity_seq_id
                                       INNER JOIN country c
                                          ON c.country_id = le.country_id
                                       INNER JOIN t_je_line_his o
                                          ON o.run_seq_id = h.run_seq_id
                                         AND o.invoice_prefix_seq_id = i.invoice_prefix_seq_id
                                         AND o.brin_op_gl = 'CCL'
                                         AND o.je_type = 'REC'
                                         AND (O.CREATED_ON BETWEEN (SELECT FROM_ FROM CUSTOMDATE) AND
                                             (SELECT TO_ FROM CUSTOMDATE))
                                       WHERE /*p.ccl_ar_interface_required = 'Y'
                                         AND ----RITM15507379*/ 
  									 h.run_seq_id = run
                                         and c.country_id = countryRecord.Country_Id -- rusia
                                       group by i.invoice_prefix_seq_id, o.brin_reference_id)
                                    select invoicesRusia.documentNumber, invoicesRusia.brin_reference_id
                                      from invoicesRusia
                                      left join v_rima_contract_number@lma_refid rima
                                        on rima.reference_id = invoicesRusia.brin_reference_id
                                     where contract_number is null
                                     group by invoicesRusia.brin_reference_id, invoicesRusia.documentNumber ) loop

       IF (numRowsNumBrinRefId < 85) then
         rusiaError := 'true';
         numRowsNumBrinRefId := numRowsNumBrinRefId + 1;
         rusiaErrorDetail := rusiaErrorDetail || '<tr><td>' || contractNumBrinRefId.documentNumber ||
                            '</td><td>' ||
                            contractNumBrinRefId.brin_reference_id ||
                            '</td></tr>';
        END IF;
       END LOOP;
      END IF;


    select to_char(sysdate, 'DDMonYY') into fileDate from dual;
    amount        := 0;
    quantity      := 0;
    ftype         := utl_file.fopen(dir, filename || '.' || countryRecord.Country_Code || '-' || fileDate, 'w');
    elems         := 0;
    rusiaError    := 'false';
    custSiteError := 'false';
    --DBMS_OUTPUT.PUT_LINE('create ccl-ar file');
    FOR invoiceRecord IN (select i.invoice_prefix_seq_id AS idInvoice,
                                 nvl(max(h.service_name_translated),
                                     max(h.service_desc)) as servDesc,
                                 --max( h.service_desc) as servDesc,
                                 max(h.in_sending_gl) as sending_gl,
                                 max(o.brin_reference_id) as brin_reference_id,
                                 max(v_pl.use_global_cust_site) as use_global_cust_site,
                                 max(v_pl.BUYER_CCL_BILL_TO_SITE) as CCL_BILL_TO_SITE,
                                 max(i.nickname||i.invoice_type||'-'||i.lst_invoice_nbr) documentNumber
                            FROM t_invoice_line_his h
                           INNER JOIN t_run r
                              ON r.run_seq_id = h.run_seq_id
                             AND h.service_desc IS NOT NULL
                           INNER JOIN t_invoice_prefix_his i
                              ON i.invoice_prefix_seq_id =
                                 h.invoice_prefix_seq_id
                             AND i.invoice_type != 'M'
                           INNER JOIN pl p
                              ON p.pl_seq_id = i.pl_seq_id
                           INNER JOIN legal_entity LE
                              ON le.Legal_Entity_Seq_Id =
                                 p.legal_entity_seq_id
                           INNER JOIN country c
                              ON c.country_id = le.country_id
                           INNER JOIN t_je_line_his o
                              ON o.run_seq_id = r.run_seq_id
                             AND o.invoice_prefix_seq_id =
                                 i.invoice_prefix_seq_id
                             AND o.brin_op_gl = 'CCL'
                             AND o.je_type = 'REC'
                           INNER JOIN v_pl_outbound_ar v_pl
                              ON o.billing_rel_id = v_pl.billing_rel_id
                           WHERE /*p.ccl_ar_interface_required = 'Y'
                             AND ---------------RITM15507379*/
							 r.run_seq_id = run
                             and c.country_id = countryRecord.Country_Id
                           group by i.invoice_prefix_seq_id) LOOP
      --DBMS_OUTPUT.PUT_LINE('invoice ' || invoiceRecord.idInvoice);
      numRows        := 0;
      numElems       := 0;
      contractNumber := getContractNumber(countryRecord.Country_Code, invoiceRecord.brin_reference_id, rusiaError);
      customerSite   := getCustomerSite(invoiceRecord.use_global_cust_site, invoiceRecord.CCL_BILL_TO_SITE, invoiceRecord.brin_reference_id, custSiteError);

      --DBMS_OUTPUT.PUT_LINE('run ' || v_invoice.run_seq_id);
      -- *************************************************************************************
      -- REC Lines All Countries
      -- *************************************************************************************
      for c in (SELECT 'BILCCL' || ',' || --1
                       'CCLARINVES' || ',' || --2
                       TO_CHAR(o.BILLER_RUN_DATE, 'YYYYMMDD') || ',' || --3
                       TO_CHAR(o.BILLER_RUN_DATE, 'YYYYMMDD') || ',' || --4
                       invoice_number || ',' || --5
                       'DERIVE' || ',' || --6
                       case when o.invoiceAmount < 0 then
                          'CM'
                       else
                          'INV'
                       end || ',,,,,' || --7-11
                       o.ORIGINAL_CURR || ',' || --12
                       DECODE(o.country_code, 'AR', 'ARBANK_DAILY', 'MOR') || ',' || --13
                       TO_CHAR(o.BILLER_RUN_DATE, 'YYYYMMDD') || ',,' || --14
                       customerSite || ',,,' || --15-18
                       'IMMEDIATE' || ',' || --19
                       o.invoice_billing_ref || ',' || --20
                       replace(o.invoiceHeader, ',', '-') || -- 21
                       ',,,,,,,,,,,,,,,' || --22-35
                       nvl(o.cclArHeaderDffContext, 'CCLAR') || ',' || --36
                       DECODE(o.ccl_ar_ibs, 'Y', o.b_buc, '') || ',' || -- 37 /*RITM16419869*/
                       ',,,,,' || --38-42
                       'If questions please contact ' || o.invoicecontact || ',,' || --43-44
                       DECODE(o.ccl_ar_ibs, 'Y', o.s_buc, '') || ',' || -- 45
                       ',,' || --46-47
                       contractNumber || ',' || --48
                       ',,,' || --49-51
                       1 || ',,' || --55-53
                       replace(case when o.country_code = 'CN' then
                                  case when invoiceRecord.sending_gl = 'ESB' then
                                     'Various '|| l_chinese_chars || ' Service charge'
                                  else
                                     DECODE(o.REFERENCE_ID,NULL,'',o.REFERENCE_ID || ' - ') ||
                                     l_chinese_chars || ' Service charge'
                                  end
                               else
                                  case when invoiceRecord.sending_gl = 'ESB' then
                                     'Various - Enterprise Standard Billing'
                                  else
                                     DECODE(o.REFERENCE_ID,NULL,'',o.REFERENCE_ID || ' - ') ||
                                     invoiceRecord.servDesc
                                  end
                               end,
                               ',',
                               '-') || ',,,,,,' || --54-59
                       'CCLAR' || ',' || --60
                       replace(case when o.country_code = 'CN' then
                                  case when invoiceRecord.sending_gl = 'ESB' then
                                     'Various '|| l_chinese_chars || ' Service charge'
                                  else
                                     DECODE(o.REFERENCE_ID,NULL,'',o.REFERENCE_ID || ' - ') ||
                                     l_chinese_chars || ' Service charge'
                                  end
                               else
                                  case when invoiceRecord.sending_gl = 'ESB' then
                                     'Various - Enterprise Standard Billing'
                                  else
                                     DECODE(o.REFERENCE_ID,NULL,'',o.REFERENCE_ID || ' - ') ||
                                     invoiceRecord.servDesc
                                  end
                               end,
                               ',',
                               '-') || --61
                       ',,,,,' || --62-65
                       replace(substr(
                                      to_char(o.biller_run_date,'yyyy-mm-dd') || '|' || -- 1s
                                      o.INVOICE_NUMBER || '|' || -- 2s
                                      o.BILL_REL_KEY || '|' || -- 3s
                                      case when je_type = 'AR' then
                                         s_buc || '|' || -- 4s
                                         b_buc || '|' || -- 5s
                                         je_line_type_cat || '|' || -- 6s
                                         expense_category || '|' || -- 7s
                                         je_line_type_sub_sub_cat || '|' || -- 8s
                                         je_line_tax_type || '|' || -- 9s
                                         je_line_type_interco -- 10s
                                      when o.in_sending_gl = 'AHB' then
                                         s_buc || '|' || -- 4s
                                         b_buc || '|' || -- 5s
                                         je_line_type_cat || '|' || -- 6s
                                         expense_category || '|' || -- 7s
                                         je_line_type_sub_sub_cat || '|' || -- 8s
                                         je_line_tax_type || '|' || -- 9s
                                         je_line_type_interco || '|' || -- 10s
                                         to_char(o.tp_percentage * 100, 'FM990.000') || '|' || -- 11s
                                         to_char(o.vat_percentage * 100, 'FM990.000') || '|' || -- 12s
                                         split(o.bex_je_description, '|', 2) || '|' || -- 13s
                                         split(o.bex_je_description, '|', 3) || '|' || -- 14s
                                         split(o.bex_je_description, '|', 4) || '|' || -- 15s
                                         split(o.bex_je_description, '|', 5) || '|' || -- 16s
                                         LTRIM(split(o.bex_je_description, '|', 6)) || '|' || -- 17s
                                         LTRIM(split(o.bex_je_description, '|', 7)) -- 18s
                                      else
                                         s_buc || '|' || -- 4s
                                         b_buc || '|' || -- 5s
                                         je_line_type_cat || '|' || -- 6s
                                         expense_category || '|' || -- 7s
                                         je_line_type_sub_sub_cat || '|' || -- 8s
                                         je_line_tax_type || '|' || -- 9s
                                         je_line_type_interco || '|' || -- 10s
                                         to_char(o.tp_percentage * 100, 'FM990.000') || '|' || -- 11s
                                         to_char(o.vat_percentage * 100, 'FM990.000') || '|' || -- 12s
                                         o.seller_cost_center || '|' || -- 13s
                                         o.orig_gl_account || '|' || -- 14s
                                         o.bex_je_description -- 15s
                                      end,
                                      0,
                                      150),
                               ',') || ',' || --66
                       DECODE(o.ccl_ar_ibs, 'Y', o.adn, '') || ',' || --67
                       ',,,,,,,,' || 'REC' || ',' || --68-76
                       o.BALANCING_SEGMENT || ',' || --77 (company code)
                       o.EXPENSE_ACCOUNT || ',' || --78
                       /*nvl(CASE WHEN o.je_line_type_cat = 'AP' AND o.expense_category != 'IBS' THEN
                              CASE WHEN O.s_gl = 'CCL' THEN
                                 O.S_BAL_SEG
                              ELSE
                                 billerif_utils.get_COA_Company_Code(O.S_BAL_SEG)
                              END
                           WHEN o.je_line_type_cat = 'AR' AND o.expense_category != 'IBS' THEN
                              CASE WHEN O.b_gl = 'CCL' THEN
                                 O.b_bal_seg
                              ELSE
                                 CASE LENGTH(O.b_bal_seg) when 4 THEN
                                    O.b_bal_seg
                                 ELSE
                                    billerif_utils.get_COA_Company_Code(O.b_bal_seg)
                                 END
                              END
                           ELSE
                              ''
                           END,
                           '0000') || ',' || --79 (Trading Partner)*/
                      NVL(CASE WHEN UPPER(o.tp_required) in ('Y','T') THEN
                           CASE WHEN UPPER(o.outbound_id) = 'BUYER' THEN
                              CASE LENGTH(O.s_bal_seg) when 4 THEN
                                  O.s_bal_seg
                              ELSE
                                  billerif_utils.get_COA_Company_Code(O.s_bal_seg)
                              END
                           WHEN UPPER(o.outbound_id) = 'SELLER' THEN
                              CASE LENGTH(O.b_bal_seg) when 4 THEN
                                  O.b_bal_seg
                              ELSE
                                  billerif_utils.get_COA_Company_Code(O.b_bal_seg)
                              END
                           END
                       END,'0000')  || ',' || --79 (Trading Partner)
                       '000000' || ',' || --80
                       '000' || ',' || --81
                       trim(nvl(o.project, '0000000000')) || ',' || --82
                       '000000' || ',' || --83
                       nvl(o.product_line, '000000') || ',' || --84
                       'P' || ',' || --85
                       '000000000' || ',' || --86
                       '000000' || ',' || --87
                       TRIM(TO_CHAR(NVL((select sum(billing_amt)
                                          from t_je_line_his
                                         where RUN_SEQ_ID = run
                                           and invoice_prefix_seq_id = invoiceRecord.idInvoice
                                           AND brin_op_gl = 'CCL'
                                           and je_type = 'AR'),
                                        '0'),'999999999999999990.00')) --88
										|| ',,,,,,,,'  --89-96  /*RITM16419869*/
                       as CCL_REV
                  from v_outbound_ar o
                 where o.RUN_SEQ_ID = run
                   and o.invoice_prefix_seq_id = invoiceRecord.idInvoice
                   AND o.OP_GL_SYSTEM = 'CCL'
                   and o.je_type = 'AR'
                   and rownum = 1) loop
        elems := elems + 1;
        utl_file.put_line(ftype, c.CCL_REV);
      end loop;

      for billingRel in (select billing_rel_id
                           from t_je_line_his
                          where run_seq_id = run
                            and invoice_prefix_seq_id = invoiceRecord.idInvoice
                            and brin_op_gl = 'CCL'
                            and je_type = 'REC'
                            and NVL(SHADOW_IND, 'N') = 'N'
                          group by billing_rel_id) loop
        SELECT *
          into v_invoice
          from v_outbound_ar o
         where o.RUN_SEQ_ID = run
           and o.invoice_prefix_seq_id = invoiceRecord.idInvoice
           and o.billing_rel_id = billingRel.billing_rel_id
           AND o.OP_GL_SYSTEM = 'CCL'
           and o.je_type = 'REC'
           and rownum = 1;
        --DBMS_OUTPUT.PUT_LINE('run ' || v_invoice.run_seq_id);
        if (v_invoice.run_seq_id is not null) then
          -- DBMS_OUTPUT.PUT_LINE('country ' || v_invoice.country_code);
          -- *************************************************************************************
          -- REV and TAX Lines for All Countries except China, Colombia and India
          -- *************************************************************************************
          if (v_invoice.country_code NOT IN ('CN','IN','CO')) then
            -- +++++++++++++++++++++++++++++++++++++++++++++
            -- REV Lines
            -- +++++++++++++++++++++++++++++++++++++++++++++
            for c in (SELECT 'BILCCL' || ',' || --1
                             'CCLARINVES' || ',' || --2
                             TO_CHAR(v_invoice.BILLER_RUN_DATE, 'YYYYMMDD') || ',' || --3
                             TO_CHAR(v_invoice.BILLER_RUN_DATE, 'YYYYMMDD') || ',' || --4
                             v_invoice.invoice_number || ',' || --5
                             'DERIVE' || ',' || --6
                             case when v_invoice.invoiceAmount < 0 then
                                'CM'
                             else
                                'INV'
                             end || ',,,,,' || --7-11
                             v_invoice.ORIGINAL_CURR || ',' || --12
                             DECODE(v_invoice.country_code,'AR','ARBANK_DAILY','MOR') || ',' || --13
                             TO_CHAR(v_invoice.BILLER_RUN_DATE, 'YYYYMMDD') || ',,' || --14-15
                             customerSite || ',,,' || --16-18
                             'IMMEDIATE' || ',' || --19
                             v_invoice.invoice_billing_ref || ',' || --20
                             replace(v_invoice.invoiceHeader, ',', '-') || --21
                             ',,,,,,,,,,,,,,,' || --22-35
                             nvl(v_invoice.cclArHeaderDffContext, 'CCLAR') || ',' || --36
                             DECODE(v_invoice.ccl_ar_ibs,'Y',v_invoice.b_buc,'') || ',' || -- 37 /*RITM16419869*/
                             ',,,,,' || --38-42
                             'If questions please contact ' ||
                             v_invoice.invoicecontact || ',,' || --43-44
                             DECODE(v_invoice.ccl_ar_ibs,'Y',v_invoice.s_buc,'') || ',' || -- 45
                             ',,' || contractNumber || ',' || --46-48
                             ',,,' || --49-51
                             (numElems + rownum) || ',,' || --52-53
                             replace(DECODE(v_invoice.REFERENCE_ID,NULL,'',v_invoice.REFERENCE_ID || ' - ') ||
                                     service_desc,
                                     ',',
                                     '-') || ',' || --54
                             case when (item + vat) < 0 then
                                -1
                             else
                                1
                             end || ',' || --55
                             TRIM(TO_CHAR(abs(NVL(item, '0')),'999999999999999990.00')) || ',' || --56
                             'EA' || ',' || --57
							 DECODE(v_invoice.SELLER_VAT_REGIME,'Y',TRIM(TO_CHAR(NVL(rate_percentage, 0) * 100,'999999999999999990.000')),'')|| ',,' || --58-59 /*RITM15507414*/
                             'CCLAR' || ',' || --60
                             replace(DECODE(v_invoice.REFERENCE_ID,NULL,'',v_invoice.REFERENCE_ID || ' - ') ||
                                     service_desc,
                                     ',',
                                     '-') || ',,,,,' || --61-65
                             replace(substr(
                                            to_char(v_invoice.biller_run_date,'yyyy-mm-dd') || '|' || -- 1s
                                            v_invoice.INVOICE_NUMBER || '|' || -- 2s
                                            v_invoice.BILL_REL_KEY || '|' || -- 3s
                                            case when v_invoice.je_type = 'TP' then
                                               v_invoice.s_buc || '|' || -- 4s
                                               v_invoice.b_buc || '|' || -- 5s
                                               v_invoice.je_line_type_cat || '|' || -- 6s
                                               v_invoice.expense_category || '|' || -- 7s
                                               v_invoice.je_line_type_sub_sub_cat || '|' || -- 8s
                                               v_invoice.je_line_tax_type || '|' || -- 9s
                                               v_invoice.je_line_type_interco -- 10s
                                            when v_invoice.in_sending_gl = 'AHB' then
                                               v_invoice.s_buc || '|' || -- 4s
                                               v_invoice.b_buc || '|' || -- 5s
                                               v_invoice.je_line_type_cat || '|' || -- 6s
                                               v_invoice.expense_category || '|' || -- 7s
                                               v_invoice.je_line_type_sub_sub_cat || '|' || -- 8s
                                               v_invoice.je_line_tax_type || '|' || -- 9s
                                               v_invoice.je_line_type_interco || '|' || -- 10s
                                               to_char(v_invoice.tp_percentage * 100, 'FM990.000') || '|' || -- 11s
                                               to_char(v_invoice.vat_percentage * 100, 'FM990.000') || '|' || -- 12s
                                               split(v_invoice.bex_je_description, '|', 2) || '|' || -- 13s
                                               split(v_invoice.bex_je_description, '|', 3) || '|' || -- 14s
                                               split(v_invoice.bex_je_description, '|', 4) || '|' || -- 15s
                                               split(v_invoice.bex_je_description, '|', 5) || '|' || -- 16s
                                               LTRIM(split(v_invoice.bex_je_description, '|', 6)) || '|' || -- 17s
                                               LTRIM(split(v_invoice.bex_je_description, '|', 7)) -- 18s
                                            else
                                               v_invoice.s_buc || '|' || -- 4s
                                               v_invoice.b_buc || '|' || -- 5s
                                               v_invoice.je_line_type_cat || '|' || -- 6s
                                               v_invoice.expense_category || '|' || -- 7s
                                               v_invoice.je_line_type_sub_sub_cat || '|' || -- 8s
                                               v_invoice.je_line_tax_type || '|' || -- 9s
                                               v_invoice.je_line_type_interco || '|' || -- 10s
                                               to_char(v_invoice.tp_percentage * 100, 'FM990.000') || '|' || -- 11s
                                               to_char(v_invoice.vat_percentage * 100, 'FM990.000') || '|' || -- 12s
                                               v_invoice.seller_cost_center || '|' || -- 13s
                                               v_invoice.orig_gl_account || '|' || -- 14s
                                               v_invoice.bex_je_description
                                            end,
                                            0,
                                            150),
                                     ',') || ',' || --66
                             DECODE(v_invoice.ccl_ar_ibs,'Y',v_invoice.adn,'') || ',' || --67
                             ',,,,,,,,' || 'REV' || ',' || --68-76
                             v_invoice.BALANCING_SEGMENT || ',' || --77
                            CASE when v_invoice.country_code = 'MX' and nvl(rate_percentage,0) = 0 THEN
                              DECODE(v_invoice.je_line_type_interco,
                                      'OOB',  '4020601650',
                                      'WOB',  '4020613650',
                                      'GEC',  '4020201650',
                                      'EXT',  '4020101650',
                                      v_invoice.EXPENSE_ACCOUNT)
                            ELSE
                               v_invoice.EXPENSE_ACCOUNT
                            END || ',' || --78
                            /* nvl(CASE WHEN v_invoice.je_line_type_cat = 'AP' AND v_invoice.expense_category != 'IBS' THEN
                                    CASE WHEN v_invoice.s_gl = 'CCL' THEN
                                       v_invoice.S_BAL_SEG
                                    ELSE
                                       billerif_utils.get_COA_Company_Code(v_invoice.S_BAL_SEG)
                                    END
                                 WHEN v_invoice.je_line_type_cat = 'AR' AND v_invoice.expense_category != 'IBS' THEN
                                    CASE WHEN v_invoice.b_gl = 'CCL' THEN
                                       v_invoice.b_bal_seg
                                    ELSE
                                       CASE LENGTH(v_invoice.b_bal_seg) when 4 THEN
                                          v_invoice.b_bal_seg
                                       ELSE
                                          billerif_utils.get_COA_Company_Code(v_invoice.b_bal_seg)
                                       END
                                    END
                                 ELSE
                                    ''
                                 END
                                ,'0000') || ',' || --79 (Trading Partner)*/
                            NVL(CASE WHEN acctNumberList like '%|'||
                                     UPPER(CASE when v_invoice.country_code = 'MX' and nvl(rate_percentage,0) = 0 THEN
                                                                                  DECODE(v_invoice.je_line_type_interco,
                                                                                          'OOB',  '4020601650',
                                                                                          'WOB',  '4020613650',
                                                                                          'GEC',  '4020201650',
                                                                                          'EXT',  '4020101650',
                                                                                          v_invoice.EXPENSE_ACCOUNT)
                                            ELSE
                                               v_invoice.EXPENSE_ACCOUNT
                                            END )||'|%' THEN
                               CASE WHEN UPPER(v_invoice.outbound_id) = 'BUYER' THEN
                                  CASE LENGTH(v_invoice.s_bal_seg) when 4 THEN
                                      v_invoice.s_bal_seg
                                  ELSE
                                      billerif_utils.get_COA_Company_Code(v_invoice.s_bal_seg)
                                  END
                               WHEN UPPER(v_invoice.outbound_id) = 'SELLER' THEN
                                  CASE LENGTH(v_invoice.b_bal_seg) when 4 THEN
                                      v_invoice.b_bal_seg
                                  ELSE
                                      billerif_utils.get_COA_Company_Code(v_invoice.b_bal_seg)
                                  END
                               END
                             END,'0000') || ',' || --79 (Trading Partner)
                             nvl(v_invoice.COST_CENTER, '000000') || ',' || --80
                             '000' || ',' || --81
                             trim(nvl(v_invoice.project, '0000000000')) || ',' || --82
                             nvl(v_invoice.REFERENCE_ID, '000000') || ',' || --83
                             nvl(v_invoice.product_line, '000000') || ',' || --84
                             'P' || ',' || --85
                             '000000000' || ',' || --86
                             '000000' || ',' || --87
                             TRIM(TO_CHAR(NVL(item, '0'),'999999999999999990.00')) --88
							 || ',,,,,,,,'  --89-96  /*RITM16419869*/
                             as ITEMLINE,
                             -- +++++++++++++++++++++++++++++++++++++++++++++
                             -- TAX Lines
                             -- +++++++++++++++++++++++++++++++++++++++++++++
                             'BILCCL' || ',' || --1
                              'CCLARINVES' || ',' || --2
                              TO_CHAR(v_invoice.BILLER_RUN_DATE, 'YYYYMMDD') || ',' || --3
                              TO_CHAR(v_invoice.BILLER_RUN_DATE, 'YYYYMMDD') || ',' || --4
                              v_invoice.invoice_number || ',' || --5
                              'DERIVE' || ',' || --6
                              case when v_invoice.invoiceAmount < 0 then
                                 'CM'
                              else
                                 'INV'
                              end || ',,,,,' || --7-11
                              v_invoice.ORIGINAL_CURR || ',' || --12
                              DECODE(v_invoice.country_code,'AR','ARBANK_DAILY','MOR') || ',' || --13
                              TO_CHAR(v_invoice.BILLER_RUN_DATE, 'YYYYMMDD') || ',,' || --14-15
                              customerSite || ',,,' || --16-18
                              'IMMEDIATE' || ',' || --19
                              v_invoice.invoice_billing_ref || ',' || --20
                              replace(v_invoice.invoiceHeader, ',', '-') || --21
                              ',,,,,,,,,,,,,,,' || --22-35
                              nvl(v_invoice.cclArHeaderDffContext, 'CCLAR') || ',' || --36
                              DECODE(v_invoice.ccl_ar_ibs,'Y',v_invoice.b_buc,'') || ',' || -- 37 /*RITM16419869*/
                              ',,,,,' || --68-42
                              'If questions please contact ' || v_invoice.invoicecontact || ',,' || --43-44
                              DECODE(v_invoice.ccl_ar_ibs,'Y',v_invoice.s_buc,'') || ',' || -- 45
                              ',,' || contractNumber || ',' || --46-48
                              ',,,' || --49-51
                              (numElems + rownum) || ',,' || --52-53
                              replace(DECODE(v_invoice.REFERENCE_ID,NULL,'',v_invoice.REFERENCE_ID || ' - ') ||
                                      service_desc,
                                      ',',
                                      '-') || ',,,,' || --54-57
							  DECODE(v_invoice.SELLER_VAT_REGIME,'Y',TRIM(TO_CHAR(NVL(rate_percentage, 0) * 100,'999999999999999990.000')),'')|| ',,' || --58-59 /*RITM15507414*/
                              'CCLAR' || ',' || --60
                              replace(DECODE(v_invoice.REFERENCE_ID,NULL,'',v_invoice.REFERENCE_ID || ' - ') ||
                                      service_desc,
                                      ',',
                                      '-') || ',,,,,' || --61-65
                              replace(substr(
                                             to_char(v_invoice.biller_run_date,'yyyy-mm-dd') || '|' || -- 1s
                                             v_invoice.INVOICE_NUMBER || '|' || -- 2s
                                             v_invoice.BILL_REL_KEY || '|' || -- 3s
                                             case when v_invoice.je_type = 'TP' then
                                               v_invoice.s_buc || '|' || -- 4s
                                               v_invoice.b_buc || '|' || -- 5s
                                               v_invoice.je_line_type_cat || '|' || -- 6s
                                               v_invoice.expense_category || '|' || -- 7s
                                               v_invoice.je_line_type_sub_sub_cat || '|' || -- 8s
                                               v_invoice.je_line_tax_type || '|' || -- 9s
                                               v_invoice.je_line_type_interco -- 10s
                                             when v_invoice.in_sending_gl = 'AHB' then
                                               v_invoice.s_buc || '|' || -- 4s
                                               v_invoice.b_buc || '|' || -- 5s
                                               v_invoice.je_line_type_cat || '|' || -- 6s
                                               v_invoice.expense_category || '|' || -- 7s
                                               v_invoice.je_line_type_sub_sub_cat || '|' || -- 8s
                                               v_invoice.je_line_tax_type || '|' || -- 9s
                                               v_invoice.je_line_type_interco || '|' || -- 10s
                                               to_char(v_invoice.tp_percentage * 100, 'FM990.000') || '|' || -- 11s
                                               to_char(v_invoice.vat_percentage * 100, 'FM990.000') || '|' || -- 12s
                                               split(v_invoice.bex_je_description, '|', 2) || '|' || -- 13s
                                               split(v_invoice.bex_je_description, '|', 3) || '|' || -- 14s
                                               split(v_invoice.bex_je_description, '|', 4) || '|' || -- 15s
                                               split(v_invoice.bex_je_description, '|', 5) || '|' || -- 16s
                                               LTRIM(split(v_invoice.bex_je_description, '|', 6)) || '|' || -- 17s
                                               LTRIM(split(v_invoice.bex_je_description, '|', 7)) -- 18s
                                             else
                                               v_invoice.s_buc || '|' || -- 4s
                                               v_invoice.b_buc || '|' || -- 5s
                                               v_invoice.je_line_type_cat || '|' || -- 6s
                                               v_invoice.expense_category || '|' || -- 7s
                                               v_invoice.je_line_type_sub_sub_cat || '|' || -- 8s
                                               v_invoice.je_line_tax_type || '|' || -- 9s
                                               v_invoice.je_line_type_interco || '|' || -- 10s
                                               to_char(v_invoice.tp_percentage * 100, 'FM990.000') || '|' || -- 11s
                                               to_char(v_invoice.vat_percentage * 100, 'FM990.000') || '|' || -- 12s
                                               v_invoice.seller_cost_center || '|' || -- 13s
                                               v_invoice.orig_gl_account || '|' || -- 14s
                                               v_invoice.bex_je_description -- 15s
                                             end,
                                                0,
                                                150),
                                         ',') || ',' || --66
                              DECODE(v_invoice.ccl_ar_ibs,'Y',v_invoice.adn,'') || ',' || --67
                              ',,,,,,,,' || 'TAX' || ',' || --68-76
                              v_invoice.BALANCING_SEGMENT || ',' || --77  (Company Code)
                              CASE when v_invoice.country_code = 'MX' THEN
                                 '2076001008'
                              when v_invoice.country_code = 'IN' THEN
                                 '2076001311'
                              ELSE
                                 nvl(v_invoice.ACCOUNT_VAT, '2076001001')
                              END || ',' || --78
                              /*nvl(CASE WHEN v_invoice.je_line_type_cat = 'AP' AND v_invoice.expense_category != 'IBS' THEN
                                     CASE WHEN v_invoice.s_gl = 'CCL' THEN
                                        v_invoice.S_BAL_SEG
                                     ELSE
                                        billerif_utils.get_COA_Company_Code(v_invoice.S_BAL_SEG)
                                     END
                                  WHEN v_invoice.je_line_type_cat = 'AR' AND v_invoice.expense_category != 'IBS' THEN
                                     CASE WHEN v_invoice.b_gl = 'CCL' THEN
                                        v_invoice.b_bal_seg
                                     ELSE
                                        CASE LENGTH(v_invoice.b_bal_seg) when 4 THEN
                                           v_invoice.b_bal_seg
                                        ELSE
                                           billerif_utils.get_COA_Company_Code(v_invoice.b_bal_seg)
                                        END
                                     END
                                  ELSE
                                     ''
                                  END,'0000') || ',' || --79 (Trading Partner)*/
                                 /*NVL(CASE WHEN UPPER(v_invoice.tp_required) in ('Y','T') THEN
                             CASE WHEN UPPER(v_invoice.outbound_id) = 'BUYER' THEN v_invoice.s_bal_seg
                                WHEN UPPER(v_invoice.outbound_id) = 'SELLER' THEN v_invoice.b_bal_seg END
                             END,'0000')  || ',' || --79 (Trading Partner)*/
                              '0000'|| ',' || --79 (Trading Partner)*/
                              '000000' || ',' || --80
                              '000' || ',' || --81
                              '0000000000' || ',' || --82 Project Code
                              '000000' || ',' || --83
                              nvl(v_invoice.product_line_tp, '000000') || ',' || --84
                              'P' || ',' || --85
                              '000000000' || ',' || --86
                              '000000' || ',' || --87
                              TRIM(TO_CHAR(NVL(round(vat, 2), '0'),'999999999999999990.00')) --88
							  || ',,,,,,,,'  --89-96  /*RITM16419869*/
                              as VATLINE,
							  v_invoice.SELLER_VAT_REGIME as SELLER_VAT_REGIME /*RITM15507414*/
                        from (select sum(item_amt) item,
                                     sum(vat_amt) vat,
                                     rate_percentage,
                                     max(service_desc) service_desc
                                from (select case when inv_line_type = 'ITEM' then
                                                sum(tax_comp_amt + ntax_comp_amt)
                                             else
                                                0
                                             end as item_amt,
                                             case when inv_line_type = 'VAT' then
                                                sum(tax_comp_amt + ntax_comp_amt)
                                             else
                                                0
                                             end as vat_amt,
                                             rate_percentage,
                                             inv_line_type,
                                             nvl(max(service_name_translated),
                                                 max(service_desc)) service_desc
                                      --max(service_desc) service_desc
                                        from t_invoice_line_his
                                       where invoice_prefix_seq_id = invoiceRecord.idInvoice
                                         and inv_line_type != 'HEADER'
                                         and brid_billing_rel_id = billingRel.billing_rel_id
                                       group by rate_percentage, inv_line_type)
                               group by rate_percentage)) loop
				if (c.SELLER_VAT_REGIME = 'Y' ) then  /*RITM15507414 -start*/
					elems    := elems + 2;
					numElems := numElems + 1;
					utl_file.put_line(ftype, c.ITEMLINE);
					utl_file.put_line(ftype, c.VATLINE);
				else
					elems    := elems + 1; /*to-do check logic for col 52*/
					numElems := numElems + 1; /*to-do check logic for col 52*/
					utl_file.put_line(ftype, c.ITEMLINE);
					/*utl_file.put_line(ftype, c.VATLINE); no VAT line for */
				end if;	/*RITM15507414 -end*/
              
            end loop;
          end if;
          -- *************************************************************************************
          -- India REV and TAX
          -- *************************************************************************************
          if (v_invoice.country_code = 'IN') then
            -- +++++++++++++++++++++++++++++++++++++++++++++
            -- REV Lines India
            -- +++++++++++++++++++++++++++++++++++++++++++++
            for c in (SELECT 'BILCCL' || ',' || --1
                             'CCLARINVES' || ',' || --2
                             TO_CHAR(v_invoice.BILLER_RUN_DATE, 'YYYYMMDD') || ',' || --3
                             TO_CHAR(v_invoice.BILLER_RUN_DATE, 'YYYYMMDD') || ',' || --4
                             v_invoice.invoice_number || ',' || --5
                             'DERIVE' || ',' || --6
                             case when v_invoice.invoiceAmount < 0 then
                                'CM'
                             else
                                'INV'
                             end || ',,,,,' || --7-11
                             v_invoice.ORIGINAL_CURR || ',' || --12
                             'MOR' || ',' || --13
                             TO_CHAR(v_invoice.BILLER_RUN_DATE, 'YYYYMMDD') || ',,' || --14-15
                             customerSite || ',,,' || --16-18
                             'IMMEDIATE' || ',' || --19
                             v_invoice.invoice_billing_ref || ',' || --20
                             replace(v_invoice.invoiceHeader, ',', '-') || --21
                             ',,,,,,,,,,,,,,,' || --22-35
                             nvl(v_invoice.cclArHeaderDffContext, 'CCLAR') || ',' || --36
                             DECODE(v_invoice.ccl_ar_ibs,'Y',v_invoice.b_buc,'') || ',' || -- 37 /*RITM16419869*/
                             ',,,,,' || --38-42
                             'If questions please contact ' || v_invoice.invoicecontact || ',,' || --43-44
                             DECODE(v_invoice.ccl_ar_ibs,'Y',v_invoice.s_buc,'') || ',' || -- 45
                             ',,' || contractNumber || ',' || --46-48
                             ',,,' || --49-51
                             (numElems + rownum) || ',,' || --52-53
                             replace(DECODE(v_invoice.REFERENCE_ID,NULL,'',v_invoice.REFERENCE_ID || ' - ') ||
                                     service_desc,
                                     ',',
                                     '-') || ',' || --54
                             case when (item + vat) < 0 then
                                -1
                             else
                                1
                             end || ',' || --55
                             TRIM(TO_CHAR(abs(NVL(item, '0')),'999999999999999990.00')) || ',' || --56
                             'EA' || ',' || --57
                             DECODE(v_invoice.SELLER_VAT_REGIME,'Y',TRIM(TO_CHAR(NVL(rate_percentage, 0) * 100,'999999999999999990.000')),'')|| ',,' || --58-59 /*RITM15507414*/
                             'CCLAR' || ',' || --60
                             replace(DECODE(v_invoice.REFERENCE_ID,NULL,'',v_invoice.REFERENCE_ID || ' - ') ||
                                     service_desc,
                                     ',',
                                     '-') || ',,,,,' || --61-65
                             replace(substr(
                                            to_char(v_invoice.biller_run_date,'yyyy-mm-dd') || '|' || -- 1s
                                            v_invoice.INVOICE_NUMBER || '|' || -- 2s
                                            v_invoice.BILL_REL_KEY || '|' || -- 3s
                                            case when v_invoice.je_type = 'TP' then
                                               v_invoice.s_buc || '|' || -- 4s
                                               v_invoice.b_buc || '|' || -- 5s
                                               v_invoice.je_line_type_cat || '|' || -- 6s
                                               v_invoice.expense_category || '|' || -- 7s
                                               v_invoice.je_line_type_sub_sub_cat || '|' || -- 8s
                                               v_invoice.je_line_tax_type || '|' || -- 9s
                                               v_invoice.je_line_type_interco -- 10s
                                            when v_invoice.in_sending_gl = 'AHB' then
                                               v_invoice.s_buc || '|' || -- 4s
                                               v_invoice.b_buc || '|' || -- 5s
                                               v_invoice.je_line_type_cat || '|' || -- 6s
                                               v_invoice.expense_category || '|' || -- 7s
                                               v_invoice.je_line_type_sub_sub_cat || '|' || -- 8s
                                               v_invoice.je_line_tax_type || '|' || -- 9s
                                               v_invoice.je_line_type_interco || '|' || -- 10s
                                               to_char(v_invoice.tp_percentage * 100, 'FM990.000') || '|' || -- 11s
                                               to_char(v_invoice.vat_percentage * 100, 'FM990.000') || '|' || -- 12s
                                               split(v_invoice.bex_je_description, '|', 2) || '|' || -- 13s
                                               split(v_invoice.bex_je_description, '|', 3) || '|' || -- 14s
                                               split(v_invoice.bex_je_description, '|', 4) || '|' || -- 15s
                                               split(v_invoice.bex_je_description, '|', 5) || '|' || -- 16s
                                               LTRIM(split(v_invoice.bex_je_description, '|', 6)) || '|' || -- 17s
                                               LTRIM(split(v_invoice.bex_je_description, '|', 7)) -- 18s
                                            else
                                               v_invoice.s_buc || '|' || -- 4s
                                               v_invoice.b_buc || '|' || -- 5s
                                               v_invoice.je_line_type_cat || '|' || -- 6s
                                               v_invoice.expense_category || '|' || -- 7s
                                               v_invoice.je_line_type_sub_sub_cat || '|' || -- 8s
                                               v_invoice.je_line_tax_type || '|' || -- 9s
                                               v_invoice.je_line_type_interco || '|' || -- 10s
                                               to_char(v_invoice.tp_percentage * 100, 'FM990.000') || '|' || -- 11s
                                               to_char(v_invoice.vat_percentage * 100, 'FM990.000') || '|' || -- 12s
                                               v_invoice.seller_cost_center || '|' || -- 13s
                                               v_invoice.orig_gl_account || '|' || -- 14s
                                               v_invoice.bex_je_description -- 15s
                                            end,
                                            0,
                                            150),
                                     ',') || ',' || --66
                             DECODE(v_invoice.ccl_ar_ibs,'Y',v_invoice.adn,'') || ',' || --67
                             ',,,,,,,,' || 'REV' || ',' || --68-76
                             v_invoice.BALANCING_SEGMENT || ',' || --77
                             v_invoice.EXPENSE_ACCOUNT || ',' || --78
                            /* nvl(CASE WHEN v_invoice.je_line_type_cat = 'AP' AND v_invoice.expense_category != 'IBS' THEN
                                    CASE WHEN v_invoice.s_gl = 'CCL' THEN
                                       v_invoice.S_BAL_SEG
                                    ELSE
                                       billerif_utils.get_COA_Company_Code(v_invoice.S_BAL_SEG)
                                    END
                                 WHEN v_invoice.je_line_type_cat = 'AR' AND v_invoice.expense_category != 'IBS' THEN
                                    CASE WHEN v_invoice.b_gl = 'CCL' THEN
                                       v_invoice.b_bal_seg
                                    ELSE
                                       CASE LENGTH(v_invoice.b_bal_seg) when 4 THEN
                                          v_invoice.b_bal_seg
                                       ELSE
                                          billerif_utils.get_COA_Company_Code(v_invoice.b_bal_seg)
                                       END
                                    END
                                 ELSE
                                    ''
                                 END,'0000') || ',' || --79 (Trading Partner)*/
                            NVL(CASE WHEN UPPER(v_invoice.tp_required) in ('Y','T') THEN
                                 CASE WHEN UPPER(v_invoice.outbound_id) = 'BUYER' THEN
                                  CASE LENGTH(v_invoice.s_bal_seg) when 4 THEN
                                      v_invoice.s_bal_seg
                                  ELSE
                                      billerif_utils.get_COA_Company_Code(v_invoice.s_bal_seg)
                                  END
                               WHEN UPPER(v_invoice.outbound_id) = 'SELLER' THEN
                                  CASE LENGTH(v_invoice.b_bal_seg) when 4 THEN
                                      v_invoice.b_bal_seg
                                  ELSE
                                      billerif_utils.get_COA_Company_Code(v_invoice.b_bal_seg)
                                  END
                               END
                             END,'0000')  || ',' || --79 (Trading Partner)
                             nvl(v_invoice.COST_CENTER, '000000') || ',' || --80
                             '000' || ',' || --81
                             trim(nvl(v_invoice.project, '0000000000')) || ',' || --82
                             nvl(v_invoice.REFERENCE_ID, '000000') || ',' || --83
                             nvl(v_invoice.product_line, '000000') || ',' || --84
                             'P' || ',' || --85
                             '000000000' || ',' || --86
                             '000000' || ',' || --87
                             TRIM(TO_CHAR(NVL(item, '0'),'999999999999999990.00')) --88
							 || ',,,,'||--89-91 /*RITM16419869*/
							CASE UPPER(NVL(v_invoice.SABRIX_TAX_TYPE,''))
								WHEN 'IGS' THEN 'IGST'
								WHEN 'CGS' THEN 'CGST'
								WHEN 'SGS' THEN 'SGST'
								ELSE ''
							END --92 /*RITM16419869*/
							 ||',DL,,'||v_invoice.sbx_commodity_code||','  --89-96  /*RITM16419869*/
                             as ITEMLINE,
                             item as distributionAmount,
                             -- +++++++++++++++++++++++++++++++++++++++++++++
                             -- TAX Lines India
                             -- +++++++++++++++++++++++++++++++++++++++++++++
                             'BILCCL' || ',' || --1
                             'CCLARINVES' || ',' || --2
                             TO_CHAR(v_invoice.BILLER_RUN_DATE, 'YYYYMMDD') || ',' || --3
                             TO_CHAR(v_invoice.BILLER_RUN_DATE, 'YYYYMMDD') || ',' || --4
                             v_invoice.invoice_number || ',' || --5
                             'DERIVE' || ',' || --6
                             case when v_invoice.invoiceAmount < 0 then
                                'CM'
                             else
                                'INV'
                             end || ',,,,,' || --7-11
                             v_invoice.ORIGINAL_CURR || ',' || --12
                             'MOR' || ',' || --13
                             TO_CHAR(v_invoice.BILLER_RUN_DATE, 'YYYYMMDD') || ',,' || --14-15
                             customerSite || ',,,' || --16-18
                             'IMMEDIATE' || ',' || --19
                             v_invoice.invoice_billing_ref || ',' || --20
                             replace(v_invoice.invoiceHeader, ',', '-') --21
                             || ',,,,,,,,,,,,,,,' || --22-35
                             nvl(v_invoice.cclArHeaderDffContext, 'CCLAR') || ',' || --36
                             DECODE(v_invoice.ccl_ar_ibs,'Y',v_invoice.b_buc,'') || ',' || -- 37 /*RITM16419869*/
                             ',,,,,' || --38-42
                             'If questions please contact ' || v_invoice.invoicecontact || ',,' || --43-44
                             DECODE(v_invoice.ccl_ar_ibs,'Y',v_invoice.s_buc,'') || ',' || -- 45
                             ',,' || contractNumber || ',' || --46-48
                             ',,,' --49-51
                             as vatPart1,
                             -- +++++++++++++++++++++++++++++++++++++++++++++++++++++++
                             --(numElems + rownum) ||
                             ',,' || --52-53
                             replace(DECODE(v_invoice.REFERENCE_ID,NULL,'',v_invoice.REFERENCE_ID || ' - ') ||
                                     service_desc,
                                     ',',
                                     '-') || ',,,,' --54-57
                             vatPart2,
                             -- +++++++++++++++++++++++++++++++++++++++++++++++++++++++
                             DECODE(v_invoice.SELLER_VAT_REGIME,'Y',NVL(rate_percentage, 0) * 100,'') as vatPercentage,/*RITM15507414*/
                             ',,' || --58-59
                             'CCLAR' || ',' || --60
                             replace(DECODE(v_invoice.REFERENCE_ID,NULL,'',v_invoice.REFERENCE_ID || ' - ') ||
                                     service_desc,
                                     ',',
                                     '-') || ',,,,,' || --61-65
                             replace(substr(
                                            to_char(v_invoice.biller_run_date,'yyyy-mm-dd') || '|' || -- 1s
                                            v_invoice.INVOICE_NUMBER || '|' || -- 2s
                                            v_invoice.BILL_REL_KEY || '|' || -- 3s
                                            case when v_invoice.je_type = 'TP' then
                                               v_invoice.s_buc || '|' || -- 4s
                                               v_invoice.b_buc || '|' || -- 5s
                                               v_invoice.je_line_type_cat || '|' || -- 6s
                                               v_invoice.expense_category || '|' || -- 7s
                                               v_invoice.je_line_type_sub_sub_cat || '|' || -- 8s
                                               v_invoice.je_line_tax_type || '|' || -- 9s
                                               v_invoice.je_line_type_interco -- 10s
                                            when v_invoice.in_sending_gl = 'AHB' then
                                               v_invoice.s_buc || '|' || -- 4s
                                               v_invoice.b_buc || '|' || -- 5s
                                               v_invoice.je_line_type_cat || '|' || -- 6s
                                               v_invoice.expense_category || '|' || -- 7s
                                               v_invoice.je_line_type_sub_sub_cat || '|' || -- 8s
                                               v_invoice.je_line_tax_type || '|' || -- 9s
                                               v_invoice.je_line_type_interco || '|' || -- 10s
                                               to_char(v_invoice.tp_percentage * 100, 'FM990.000') || '|' || -- 11s
                                               to_char(v_invoice.vat_percentage * 100, 'FM990.000') || '|' || -- 12s
                                               split(v_invoice.bex_je_description, '|', 2) || '|' || -- 13s
                                               split(v_invoice.bex_je_description, '|', 3) || '|' || -- 14s
                                               split(v_invoice.bex_je_description, '|', 4) || '|' || -- 15s
                                               split(v_invoice.bex_je_description, '|', 5) || '|' || -- 16s
                                               LTRIM(split(v_invoice.bex_je_description, '|', 6)) || '|' || -- 17s
                                               LTRIM(split(v_invoice.bex_je_description, '|', 7)) -- 18s
                                            else
                                               v_invoice.s_buc || '|' || -- 4s
                                               v_invoice.b_buc || '|' || -- 5s
                                               v_invoice.je_line_type_cat || '|' || -- 6s
                                               v_invoice.expense_category || '|' || -- 7s
                                               v_invoice.je_line_type_sub_sub_cat || '|' || -- 8s
                                               v_invoice.je_line_tax_type || '|' || -- 9s
                                               v_invoice.je_line_type_interco || '|' || -- 10s
                                               to_char(v_invoice.tp_percentage * 100, 'FM990.000') || '|' || -- 11s
                                               to_char(v_invoice.vat_percentage * 100, 'FM990.000') || '|' || -- 12s
                                               v_invoice.seller_cost_center || '|' || -- 13s
                                               v_invoice.orig_gl_account || '|' || -- 14s
                                               v_invoice.bex_je_description -- 15s
                                            end,
                                            0,
                                            150),
                                     ',') || ',' || --66
                             DECODE(v_invoice.ccl_ar_ibs,'Y',v_invoice.adn,'') || ',' || --67
                             ',,,,,,,,' || 'TAX' || ',' || --68-76
                             v_invoice.BALANCING_SEGMENT || ',' --77  (Company Code)
                             vatPart3,
                             -- +++++++++++++++++++++++++++++++++++++++++++++++++++++++
                             ',' || --78
                            /* nvl(CASE WHEN v_invoice.je_line_type_cat = 'AP' AND v_invoice.expense_category != 'IBS' THEN
                                     CASE WHEN v_invoice.s_gl = 'CCL' THEN
                                        v_invoice.S_BAL_SEG
                                     ELSE
                                        billerif_utils.get_COA_Company_Code(v_invoice.S_BAL_SEG)
                                     END
                                    WHEN v_invoice.je_line_type_cat = 'AR' AND v_invoice.expense_category != 'IBS' THEN
                                     CASE WHEN v_invoice.b_gl = 'CCL' THEN
                                        v_invoice.b_bal_seg
                                     ELSE
                                        CASE LENGTH(v_invoice.b_bal_seg) when 4 THEN
                                           v_invoice.b_bal_seg
                                        ELSE
                                           billerif_utils.get_COA_Company_Code(v_invoice.b_bal_seg)
                                        END
                                     END
                                  ELSE
                                     ''
                                  END,'0000') || ',' || --79 (Trading Partner)*/
                             /* NVL(CASE WHEN UPPER(v_invoice.tp_required) in ('Y','T') THEN
                                 CASE WHEN UPPER(v_invoice.outbound_id) = 'BUYER' THEN v_invoice.s_bal_seg
                                    WHEN UPPER(v_invoice.outbound_id) = 'SELLER' THEN v_invoice.b_bal_seg END
                              END,'0000')  || ',' || --79 (Trading Partner)*/
                              '0000'|| ',' || --79 (Trading Partner)*/
                              '000000' || ',' || --80
                              '000' || ',' || --81
                              '0000000000' || ',' || --82
                              '000000' || ',' || --83
                              nvl(v_invoice.product_line_tp, '000000') || ',' || --84
                              'P' || ',' || --85
                              '000000000' || ',' || --86
                              '000000' || ',' --87
                              as vatPart4,
							  v_invoice.sbx_commodity_code sbx_code,/*RITM16419869*/
								CASE UPPER(NVL(v_invoice.SABRIX_TAX_TYPE,''))
									WHEN 'IGS' THEN 'IGST'
									WHEN 'CGS' THEN 'CGST'
									WHEN 'SGS' THEN 'SGST'
									ELSE ''
								END sbx_tx_type /*RITM16419869*/,
								v_invoice.ACCOUNT_VAT ACCOUNT_VAT /*RITM16419869*/
                              -- +++++++++++++++++++++++++++++++++++++++++++++++++++++++
                        from (select sum(item_amt) item,
                                     sum(vat_amt) vat,
                                     rate_percentage,
                                     max(service_desc) service_desc
                                from (select case when inv_line_type = 'ITEM' then
                                                sum(tax_comp_amt + ntax_comp_amt)
                                             else
                                                0
                                             end as item_amt,
                                             case when inv_line_type = 'VAT' then
                                                sum(tax_comp_amt + ntax_comp_amt)
                                             else
                                                0
                                             end as vat_amt,
                                             rate_percentage,
                                             inv_line_type,
                                             nvl(max(service_name_translated),
                                                 max(service_desc)) service_desc
                                        from t_invoice_line_his
                                       where invoice_prefix_seq_id = invoiceRecord.idInvoice
                                         and inv_line_type != 'HEADER'
                                         and brid_billing_rel_id = billingRel.billing_rel_id
                                       group by rate_percentage, inv_line_type)
                               group by rate_percentage)) loop
              utl_file.put_line(ftype, c.ITEMLINE);
              /*
              IF(c.vatPercentage=12.36) THEN
                tax1:=round((c.distributionAmount*12)/100,2);
                tax2:=round((tax1*2)/100,2);
                tax3:=round(((c.distributionAmount*12.36)/100),2)-tax1-tax2;
                utl_file.put_line(ftype,
                                  c.vatPart1 || (numElems +1) ||
                                  c.vatPart2 || '12.000' ||
                                  c.vatPart3 ||'2076001311' ||
                                  c.vatPart4 ||  TRIM(TO_CHAR(NVL(round(tax1, 2),'0'),'999999999999999990.00')));
                utl_file.put_line(ftype,
                                  c.vatPart1 || (numElems +1) ||
                                  c.vatPart2 || '2.000' ||
                                  c.vatPart3 || '2076001322' ||
                                  c.vatPart4 || TRIM(TO_CHAR(NVL(round(tax2, 2),'0'),'999999999999999990.00')));
                utl_file.put_line(ftype,
                                  c.vatPart1 || (numElems +1) ||
                                  c.vatPart2 || '1.000' ||
                                  c.vatPart3 || '2076001332' ||
                                  c.vatPart4 || TRIM(TO_CHAR(NVL(round(tax3, 2),'0'),'999999999999999990.00')));
              ELSIF(c.vatPercentage=0)THEN
                 utl_file.put_line(ftype,
                                  c.vatPart1 || (numElems +1) ||
                                  c.vatPart2 || '0.000' ||
                                  c.vatPart3 ||'2076001311' ||
                                  c.vatPart4 ||  TRIM(TO_CHAR(0,'999999999999999990.00')));
                utl_file.put_line(ftype,
                                  c.vatPart1 || (numElems +1) ||
                                  c.vatPart2 || '0.000' ||
                                  c.vatPart3 || '2076001322' ||
                                  c.vatPart4 || TRIM(TO_CHAR(0,'999999999999999990.00')));
                utl_file.put_line(ftype,
                                  c.vatPart1 || (numElems +1) ||
                                  c.vatPart2 || '0.000' ||
                                  c.vatPart3 || '2076001332' ||
                                  c.vatPart4 || TRIM(TO_CHAR(0,'999999999999999990.00')));
              ELSE
                   raise_application_error(-20101, 'India Tax is not 0.00% or 12.36% for Invoice seq id # '|| invoiceRecord.idInvoice || '.');
              END IF;*/

              IF (c.vatPercentage = 12.36) THEN
                tax1 := round((c.distributionAmount * 12.36) / 100, 2);
                tax2 := 0;
                utl_file.put_line(ftype,
                                  c.vatPart1 || (numElems + 1) ||
                                  c.vatPart2 || '12.360' ||
                                  c.vatPart3 || '2076001311' ||
                                  c.vatPart4 || TRIM(TO_CHAR(NVL(round(tax1, 2), '0'),'999999999999999990.00'))
								  || ',,,,'||c.sbx_tx_type||',,,,'); --89-96  /*RITM16419869*/
                utl_file.put_line(ftype,
                                  c.vatPart1 || (numElems + 1) ||
                                  c.vatPart2 || '0.000' ||
                                  c.vatPart3 ||'2076001322' ||
                                  c.vatPart4 || TRIM(TO_CHAR(NVL(round(tax2, 2), '0'),'999999999999999990.00'))
								  || ',,,,'||c.sbx_tx_type||',,,,'); --89-96  /*RITM16419869*/
              ELSIF (c.vatPercentage = 14.0) THEN
                tax1 := round((c.distributionAmount * 14.0) / 100, 2);
                tax2 := 0;
                utl_file.put_line(ftype,
                                  c.vatPart1 || (numElems + 1) ||
                                  c.vatPart2 || '14.000' ||
                                  c.vatPart3 || '2076001311' ||
                                  c.vatPart4 || TRIM(TO_CHAR(NVL(round(tax1, 2), '0'),'999999999999999990.00'))
								  || ',,,,'||c.sbx_tx_type||',,,,'); --89-96  /*RITM16419869*/
                utl_file.put_line(ftype,
                                  c.vatPart1 || (numElems + 1) ||
                                  c.vatPart2 || '0.000' ||
                                  c.vatPart3 || '2076001322' ||
                                  c.vatPart4 || TRIM(TO_CHAR(NVL(round(tax2, 2), '0'),'999999999999999990.00'))
								  || ',,,,'||c.sbx_tx_type||',,,,'); --89-96  /*RITM16419869*/
              ELSIF (c.vatPercentage = 14.5) THEN
                tax1 := round((c.distributionAmount * 14) / 100, 2);
                tax2 := round(((c.distributionAmount * 14.5) / 100), 2) - tax1;
                utl_file.put_line(ftype,
                                  c.vatPart1 || (numElems + 1) ||
                                  c.vatPart2 || '14.000' ||
                                  c.vatPart3 || '2076001311' ||
                                  c.vatPart4 || TRIM(TO_CHAR(NVL(round(tax1, 2), '0'),'999999999999999990.00'))
								  || ',,,,'||c.sbx_tx_type||',,,,'); --89-96  /*RITM16419869*/
                utl_file.put_line(ftype,
                                  c.vatPart1 || (numElems + 1) ||
                                  c.vatPart2 || '0.500' ||
                                  c.vatPart3 || '2076001322' ||
                                  c.vatPart4 || TRIM(TO_CHAR(NVL(round(tax2, 2), '0'),'999999999999999990.00'))
								  || ',,,,'||c.sbx_tx_type||',,,,'); --89-96  /*RITM16419869*/
        ELSIF (c.vatPercentage = 15) THEN
                tax1 := round((c.distributionAmount  * 14)   / 100, 2);
                tax2 := round(((c.distributionAmount * 0.5) / 100), 2);
                tax3 := round(((c.distributionAmount * 15)   / 100), 2) - tax1 - tax2;
                utl_file.put_line(ftype,
                                  c.vatPart1 || (numElems + 1) ||
                                  c.vatPart2 || '14.000' ||
                                  c.vatPart3 || '2076001311' ||
                                  c.vatPart4 || TRIM(TO_CHAR(NVL(round(tax1, 2), '0'),'999999999999999990.00'))
								  || ',,,,'||c.sbx_tx_type||',,,,'); --89-96  /*RITM16419869*/
                utl_file.put_line(ftype,
                                  c.vatPart1 || (numElems + 1) ||
                                  c.vatPart2 || '0.500' ||
                                  c.vatPart3 || '2076001322' ||
                                  c.vatPart4 || TRIM(TO_CHAR(NVL(round(tax2, 2), '0'),'999999999999999990.00'))
								  || ',,,,'||c.sbx_tx_type||',,,,'); --89-96  /*RITM16419869*/
                utl_file.put_line(ftype,
                                  c.vatPart1 || (numElems + 1) ||
                                  c.vatPart2 || '0.500' ||
                                  c.vatPart3 || '2270403000' ||
                                  c.vatPart4 || TRIM(TO_CHAR(NVL(round(tax3, 2), '0'),'999999999999999990.00'))
								  || ',,,,'||c.sbx_tx_type||',,,,'); --89-96  /*RITM16419869*/
              ELSIF (c.vatPercentage = 0) THEN
                utl_file.put_line(ftype,
                                  c.vatPart1 || (numElems + 1) ||
                                  c.vatPart2 || '0.000' ||
                                  c.vatPart3 || nvl(c.ACCOUNT_VAT, '2076001302') ||
                                  c.vatPart4 || TRIM(TO_CHAR(0, '999999999999999990.00'))
								  || ',,,,'||c.sbx_tx_type||',,,,'); --89-96  /*RITM16419869*/
                /*utl_file.put_line(ftype,
                                  c.vatPart1 || (numElems + 1) ||
                                  c.vatPart2 || '0.000' ||
                                  c.vatPart3 || '2076001322' ||
                                  c.vatPart4 || TRIM(TO_CHAR(0, '999999999999999990.00'))
								  || ',,,,'||c.sbx_tx_type||',DL,,'||c.sbx_code||','); --89-96  */
         /*utl_file.put_line(ftype,
                                  c.vatPart1 || (numElems + 1) ||
                                  c.vatPart2 || '0.000' ||
                                  c.vatPart3 || '2270403000' ||
                                  c.vatPart4 || TRIM(TO_CHAR(0, '999999999999999990.00'))
								  || ',,,,'||c.sbx_tx_type||',DL,,'||c.sbx_code||','); --89-96  */
               ELSIF (c.vatPercentage = 18) THEN
                tax1 := round((c.distributionAmount * 18) / 100, 2);
                tax2 := 0;
                utl_file.put_line(ftype,
                                  c.vatPart1 || (numElems + 1) ||
                                  c.vatPart2 || '18.000' ||
                                  c.vatPart3 ||  nvl(c.ACCOUNT_VAT, '2076001302') ||  /*RITM16419869*/
                                  c.vatPart4 || TRIM(TO_CHAR(NVL(round(tax1, 2), '0'),'999999999999999990.00'))
								  || ',,,,'||c.sbx_tx_type||',,,,'); --89-96  /*RITM16419869*/
			 
			ELSIF (c.vatPercentage = 9) THEN
                tax1 := round((c.distributionAmount * 9) / 100, 2);
                tax2 := round(((c.distributionAmount * 9) / 100), 2);
                utl_file.put_line(ftype,
                                  c.vatPart1 || (numElems + 1) ||
                                  c.vatPart2 || '9.000' ||
                                  c.vatPart3 || nvl(c.ACCOUNT_VAT, '2076001302') ||  /*RITM16419869*/
                                  c.vatPart4 || TRIM(TO_CHAR(NVL(round(tax1, 2), '0'),'999999999999999990.00'))
								  || ',,,,'||c.sbx_tx_type||',,,,'); --89-96  /*RITM16419869*/
                utl_file.put_line(ftype,
                                  c.vatPart1 || (numElems + 1) ||
                                  c.vatPart2 || '9.000' ||
                                  c.vatPart3 || nvl(c.ACCOUNT_VAT, '2076001302') ||  /*RITM16419869*/
                                  c.vatPart4 || TRIM(TO_CHAR(NVL(round(tax2, 2), '0'),'999999999999999990.00'))
								  || ',,,,'||c.sbx_tx_type||',,,,'); --89-96  /*RITM16419869*/
			 ELSE
                raise_application_error(-20101,
                                        'India Tax is not 0.00% or 12.36% or 14.0% or 14.5% or 15.0% for Invoice seq id # ' ||
                                        invoiceRecord.idInvoice || '.');
              END IF;
              -- Original Elements Count (REV and TAX)
              if (c.vatPercentage = 15 /*or c.vatPercentage = 0*/) THEN
                elems := elems + 4;
			  elsif (c.vatPercentage = 0 or c.vatPercentage = 18) THEN
				elems := elems + 2;
              else
                elems :=elems + 3;
              end if;
              numElems := numElems + 1;
            end loop;
          end if;
          -- *************************************************************************************
          -- END INDIA REV and TAX
          -- *************************************************************************************

          -- *************************************************************************************
          -- China REV and TAX
          -- *************************************************************************************
          if (v_invoice.country_code = 'CN') then
            -- +++++++++++++++++++++++++++++++++++++++++++++
            -- REV Lines China
            -- +++++++++++++++++++++++++++++++++++++++++++++
            for c in (SELECT 'BILCCL' || ',' || --1
                             'CCLARINVES' || ',' || --2
                             TO_CHAR(v_invoice.BILLER_RUN_DATE, 'YYYYMMDD') || ',' || --3
                             TO_CHAR(v_invoice.BILLER_RUN_DATE, 'YYYYMMDD') || ',' || --4
                             v_invoice.invoice_number || ',' || --5
                             'DERIVE' || ',' || --6
                             case when v_invoice.invoiceAmount < 0 then
                                'CM'
                             else
                                'INV'
                             end || ',,,,,' || --7-11
                             v_invoice.ORIGINAL_CURR || ',' || --12
                             'MOR' || ',' || --13
                             TO_CHAR(v_invoice.BILLER_RUN_DATE, 'YYYYMMDD') || ',,' || --14-15
                             customerSite || ',,,' || --16-18
                             'IMMEDIATE' || ',' || --19
                             v_invoice.invoice_billing_ref || ',' || --20
                             replace(v_invoice.invoiceHeader, ',', '-') || --21
                             ',,,,,,,,,,,,,,,' || --22-35
                             nvl(v_invoice.cclArHeaderDffContext, 'CCLAR') || ',' || --36
                             DECODE(v_invoice.ccl_ar_ibs,'Y',v_invoice.b_buc,'') || ',' || -- 37 /*RITM16419869*/
                             ',,,,,' || --38-42
                             'If questions please contact ' || v_invoice.invoicecontact || ',,' || --43-44
                             DECODE(v_invoice.ccl_ar_ibs,'Y',v_invoice.s_buc,'') || ',' || -- 45
                             ',,' || contractNumber || ',' || --46-48
                             ',,,' --49-51
                             as item01, --57
                             -- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++
                             ',,' || --59
                             replace(DECODE(v_invoice.REFERENCE_ID,NULL,'',v_invoice.REFERENCE_ID || ' - ') ||
                                     l_chinese_chars || ' Service charge',
                                     ',',
                                     '-') as item02, --60
                             TRIM(TO_CHAR(NVL(rate_percentage, 0) * 100,'999999999999999990.000')) || ',,' ||
                              'CCLAR' || ',' || --63
                              replace(DECODE(v_invoice.REFERENCE_ID,NULL,'',v_invoice.REFERENCE_ID || ' - ') ||
                                      l_chinese_chars || ' Service charge',
                                      ',',
                                      '-') || ',,,,,' || --65
                              replace(substr(
                                             to_char(v_invoice.biller_run_date,'yyyy-mm-dd') || '|' || --1s
                                             v_invoice.INVOICE_NUMBER || '|' || --2s
                                             v_invoice.BILL_REL_KEY || '|' || --3s
                                             case when v_invoice.je_type = 'TP' then
                                                v_invoice.s_buc || '|' || --4s
                                                v_invoice.b_buc || '|' || --5s
                                                v_invoice.je_line_type_cat || '|' || --6s
                                                v_invoice.expense_category || '|' || --7s
                                                v_invoice.je_line_type_sub_sub_cat || '|' || --8s
                                                v_invoice.je_line_tax_type || '|' || --9s
                                                v_invoice.je_line_type_interco --10s
                                             when v_invoice.in_sending_gl = 'AHB' then
                                                v_invoice.s_buc || '|' || --4s
                                                v_invoice.b_buc || '|' || --5s
                                                v_invoice.je_line_type_cat || '|' || --6s
                                                v_invoice.expense_category || '|' || --7s
                                                v_invoice.je_line_type_sub_sub_cat || '|' || --8s
                                                v_invoice.je_line_tax_type || '|' || --9s
                                                v_invoice.je_line_type_interco || '|' || --10s
                                                to_char(v_invoice.tp_percentage * 100, 'FM990.000') || '|' || --11s
                                                to_char(v_invoice.vat_percentage * 100, 'FM990.000') || '|' || --12s
                                                split(v_invoice.bex_je_description, '|', 2) || '|' || --13s
                                                split(v_invoice.bex_je_description, '|', 3) || '|' || --14s
                                                split(v_invoice.bex_je_description, '|', 4) || '|' || --15s
                                                split(v_invoice.bex_je_description, '|', 5) || '|' || --16s
                                                LTRIM(split(v_invoice.bex_je_description, '|', 6)) || '|' || --17s
                                                LTRIM(split(v_invoice.bex_je_description, '|', 7)) --18s
                                             else
                                                v_invoice.s_buc || '|' || --4s
                                                v_invoice.b_buc || '|' || --5s
                                                v_invoice.je_line_type_cat || '|' || --6s
                                                v_invoice.expense_category || '|' || --7s
                                                v_invoice.je_line_type_sub_sub_cat || '|' || --8s
                                                v_invoice.je_line_tax_type || '|' || --9s
                                                v_invoice.je_line_type_interco || '|' || --10s
                                                to_char(v_invoice.tp_percentage * 100, 'FM990.000') || '|' || --11s
                                                to_char(v_invoice.vat_percentage * 100, 'FM990.000') || '|' || --12s
                                                v_invoice.seller_cost_center || '|' || --13s
                                                v_invoice.orig_gl_account || '|' || --14s
                                                v_invoice.bex_je_description --15s
                                             end,
                                             0,
                                             150),
                                      ',') || ',' ||--66
                              DECODE(v_invoice.ccl_ar_ibs,'Y',v_invoice.adn,'') || ',' || --67
                              ',,,,,,,,' || 'REV' || ',' || --68-76
                              v_invoice.BALANCING_SEGMENT || ',' || --77  (Company Code)
                              nvl((select look_name
                                    from t_look_up look
                                   inner join t_je_line_type je_type
                                      on look.value = je_type.je_line_type_id
                                   where look.type = 'CCLAR-CHINA_EXCEPTION'
                                     and je_category = v_invoice.je_line_type_cat
                                     and sub_category = v_invoice.expense_category
                                     and sub_sub_category = v_invoice.je_line_type_sub_sub_cat
                                     and je_tax = v_invoice.je_line_tax_type
                                     and je_interco = v_invoice.je_line_type_interco
                                     and look_code = v_invoice.EXPENSE_ACCOUNT),
                                  v_invoice.EXPENSE_ACCOUNT) || ',' || --78 Account
                              /*nvl(CASE WHEN v_invoice.je_line_type_cat = 'AP' AND v_invoice.expense_category != 'IBS' THEN
                                     CASE WHEN v_invoice.s_gl = 'CCL' THEN
                                        v_invoice.S_BAL_SEG
                                     ELSE
                                        billerif_utils.get_COA_Company_Code(v_invoice.S_BAL_SEG)
                                     END
                                  WHEN v_invoice.je_line_type_cat = 'AR' AND v_invoice.expense_category != 'IBS' THEN
                                     CASE WHEN v_invoice.b_gl = 'CCL' THEN
                                        v_invoice.b_bal_seg
                                     ELSE
                                        CASE LENGTH(v_invoice.b_bal_seg) when 4 THEN
                                           v_invoice.b_bal_seg
                                        ELSE
                                           billerif_utils.get_COA_Company_Code(v_invoice.b_bal_seg)
                                        END
                                     END
                                  ELSE
                                     ''
                                  END ,'0000') || ',' || --79 (Trading Partner)*/
                             NVL(CASE WHEN acctNumberList like '%|'|| UPPER(nvl((select look_name
                                    from t_look_up look
                                   inner join t_je_line_type je_type
                                      on look.value = je_type.je_line_type_id
                                   where look.type = 'CCLAR-CHINA_EXCEPTION'
                                     and je_category = v_invoice.je_line_type_cat
                                     and sub_category = v_invoice.expense_category
                                     and sub_sub_category = v_invoice.je_line_type_sub_sub_cat
                                     and je_tax = v_invoice.je_line_tax_type
                                     and je_interco = v_invoice.je_line_type_interco
                                     and look_code = v_invoice.EXPENSE_ACCOUNT),
                                  v_invoice.EXPENSE_ACCOUNT))||'|%' THEN
                                  CASE WHEN UPPER(v_invoice.outbound_id) = 'BUYER' THEN
                                    CASE LENGTH(v_invoice.s_bal_seg) when 4 THEN
                                        v_invoice.s_bal_seg
                                    ELSE
                                        billerif_utils.get_COA_Company_Code(v_invoice.s_bal_seg)
                                    END
                                 WHEN UPPER(v_invoice.outbound_id) = 'SELLER' THEN
                                    CASE LENGTH(v_invoice.b_bal_seg) when 4 THEN
                                        v_invoice.b_bal_seg
                                    ELSE
                                        billerif_utils.get_COA_Company_Code(v_invoice.b_bal_seg)
                                    END
                                 END
                             END,'0000') || ',' || --79 (Trading Partner)
                              nvl(v_invoice.COST_CENTER, '000000') || ',' || --80
                              '000' || ',' || --81
                              trim(nvl(v_invoice.project, '0000000000')) || ',' || --82
                              nvl(v_invoice.REFERENCE_ID, '000000') || ',' || --83
                              nvl(v_invoice.product_line, '000000') || ',' || --84
                              'P' || ',' || --85
                              '000000000' || ',' || --86
                              '000000' || ',' || --87
                              ''
                             -- +++++++++++++++++++++++++++++++++++++++++++++
                             -- TAX Lines China
                             -- +++++++++++++++++++++++++++++++++++++++++++++
                              as ITEMLINE,
                             'BILCCL' || ',' || 'CCLARINVES' || ',' ||
                             TO_CHAR(v_invoice.BILLER_RUN_DATE, 'YYYYMMDD') || ',' ||
                             TO_CHAR(v_invoice.BILLER_RUN_DATE, 'YYYYMMDD') || ',' ||
                             v_invoice.invoice_number || ',' || 'DERIVE' || ',' ||
                             case when v_invoice.invoiceAmount < 0 then
                                'CM'
                             else
                                'INV'
                             end || ',,,,,' || v_invoice.ORIGINAL_CURR || ',' ||
                             'MOR' || ',' ||
                             TO_CHAR(v_invoice.BILLER_RUN_DATE, 'YYYYMMDD') || ',,' ||
                             customerSite || ',,,' || 'IMMEDIATE' || ',' ||
                             v_invoice.invoice_billing_ref || ',' || --20
                             replace(v_invoice.invoiceHeader, ',', '-') || --22
                             ',,,,,,,,,,,,,,,' || --23-35
                             nvl(v_invoice.cclArHeaderDffContext, 'CCLAR') || ',' || --36
                             DECODE(v_invoice.ccl_ar_ibs,'Y',v_invoice.b_buc,'') || ',' || -- 37 /*RITM16419869*/
                             ',,,,,' || 'If questions please contact ' ||
                             v_invoice.invoicecontact || ',,' || --38-44
                             DECODE(v_invoice.ccl_ar_ibs,'Y',v_invoice.s_buc,'') || ',' || -- 45
                             ',,' || contractNumber || ',' || --46-48
                             ',,,' --49-51
                             as vat01,
                             -- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++
                             ',,' || --52-53
                             replace(v_invoice.REFERENCE_ID || ' - '|| l_chinese_chars || ' Service charge',
                                      ',',
                                      '-') || ',,,,' || --54-57
                              DECODE(v_invoice.SELLER_VAT_REGIME,'Y',TRIM(TO_CHAR(NVL(rate_percentage, 0) * 100,'999999999999999990.000')),'')|| ',,' || --58-59 /*RITM15507414*/
                              'CCLAR' || ',' || --60
                              replace(v_invoice.REFERENCE_ID || ' - '|| l_chinese_chars || ' Service charge',
                                      ',',
                                      '-') || ',,,,,' || --61-65
                              replace(substr(
                                             to_char(v_invoice.biller_run_date,'yyyy-mm-dd') || '|' || --1s
                                             v_invoice.INVOICE_NUMBER || '|' || --2s
                                             v_invoice.BILL_REL_KEY || '|' || --2s
                                             case when v_invoice.je_type = 'TP' then
                                                v_invoice.s_buc || '|' || --4s
                                                v_invoice.b_buc || '|' || --5s
                                                v_invoice.je_line_type_cat || '|' || --6s
                                                v_invoice.expense_category || '|' || --7s
                                                v_invoice.je_line_type_sub_sub_cat || '|' || --8s
                                                v_invoice.je_line_tax_type || '|' || --9s
                                                v_invoice.je_line_type_interco --10s
                                             when v_invoice.in_sending_gl = 'AHB' then
                                                v_invoice.s_buc || '|' || --4s
                                                v_invoice.b_buc || '|' || --5s
                                                v_invoice.je_line_type_cat || '|' || --6s
                                                v_invoice.expense_category || '|' || --7s
                                                v_invoice.je_line_type_sub_sub_cat || '|' || --8s
                                                v_invoice.je_line_tax_type || '|' || --9s
                                                v_invoice.je_line_type_interco || '|' || --10s
                                                to_char(v_invoice.tp_percentage * 100, 'FM990.000') || '|' || --11s
                                                to_char(v_invoice.vat_percentage * 100, 'FM990.000') || '|' || --12s
                                                split(v_invoice.bex_je_description, '|', 2) || '|' || --13s
                                                split(v_invoice.bex_je_description, '|', 3) || '|' || --14s
                                                split(v_invoice.bex_je_description, '|', 4) || '|' || --15s
                                                split(v_invoice.bex_je_description, '|', 5) || '|' || --16s
                                                LTRIM(split(v_invoice.bex_je_description, '|', 6)) || '|' || --17s
                                                LTRIM(split(v_invoice.bex_je_description, '|', 7)) --18s
                                             else
                                                v_invoice.s_buc || '|' || --4s
                                                v_invoice.b_buc || '|' || --5s
                                                v_invoice.je_line_type_cat || '|' || --6s
                                                v_invoice.expense_category || '|' || --7s
                                                v_invoice.je_line_type_sub_sub_cat || '|' || --8s
                                                v_invoice.je_line_tax_type || '|' || --9s
                                                v_invoice.je_line_type_interco || '|' || --10s
                                                to_char(v_invoice.tp_percentage * 100, 'FM990.000') || '|' || --11s
                                                to_char(v_invoice.vat_percentage * 100, 'FM990.000') || '|' || --12s
                                                v_invoice.seller_cost_center || '|' || --13s
                                                v_invoice.orig_gl_account || '|' || --14s
                                                v_invoice.bex_je_description --15s
                                             end,
                                             0,
                                             150),
                                      ',') || ',' || --66
                              DECODE(v_invoice.ccl_ar_ibs,'Y',v_invoice.adn,'') || ',' || --67
                              ',,,,,,,,' || 'TAX' || ',' || --68-76
                              v_invoice.BALANCING_SEGMENT || ',' || --77
                              nvl(v_invoice.ACCOUNT_VAT, '2076001001') || ',' || --78
                              /*nvl(CASE WHEN v_invoice.je_line_type_cat = 'AP' AND v_invoice.expense_category != 'IBS' THEN
                                     CASE WHEN v_invoice.s_gl = 'CCL' THEN
                                        v_invoice.S_BAL_SEG
                                     ELSE
                                        billerif_utils.get_COA_Company_Code(v_invoice.S_BAL_SEG)
                                     END
                                  WHEN v_invoice.je_line_type_cat = 'AR' AND v_invoice.expense_category != 'IBS' THEN
                                     CASE WHEN v_invoice.b_gl = 'CCL' THEN
                                        v_invoice.b_bal_seg
                                     ELSE
                                        CASE LENGTH(v_invoice.b_bal_seg) when 4 THEN
                                           v_invoice.b_bal_seg
                                        ELSE
                                           billerif_utils.get_COA_Company_Code(v_invoice.b_bal_seg)
                                        END
                                  END
                                  ELSE
                                     ''
                                  END,'0000') || ',' || --79 (Trading Partner)*/
                              /*NVL(CASE WHEN UPPER(v_invoice.tp_required) in ('Y','T') THEN
                                CASE WHEN UPPER(v_invoice.outbound_id) = 'BUYER' THEN v_invoice.s_bal_seg
                                WHEN UPPER(v_invoice.outbound_id) = 'SELLER' THEN v_invoice.b_bal_seg END
                              END,'0000')  || ',' || --79 (Trading Partner)*/
                              '0000'|| ',' || --79 (Trading Partner)*/
                              '000000' || ',' || --80
                              '000' || ',' || --81
                              '0000000000' || ',' || --82
                              '000000' || ',' || --83
                              nvl(v_invoice.product_line_tp, '000000') || ',' || --84
                              'P' || ',' || --85
                              '000000000' || ',' || --86
                              '000000' || ',' || --87
                              '' --88
							  || ',,,,,,,,'  --89-96  /*RITM16419869*/
                              as VATLINE,
                             item,
                             vat
                        from (select sum(item_amt) item,
                                     sum(vat_amt) vat,
                                     rate_percentage,
                                     max(service_desc) service_desc
                                from (select case when inv_line_type = 'ITEM' then
                                                sum(tax_comp_amt + ntax_comp_amt)
                                             else
                                                0
                                             end as item_amt,
                                             case when inv_line_type = 'VAT' then
                                                sum(tax_comp_amt + ntax_comp_amt)
                                             else
                                                0
                                             end as vat_amt,
                                             rate_percentage,
                                             inv_line_type,
                                             nvl(max(service_name_translated),
                                                 max(service_desc)) service_desc
                                      --max(service_desc) service_desc
                                        from t_invoice_line_his
                                       where invoice_prefix_seq_id = invoiceRecord.idInvoice
                                         and inv_line_type != 'HEADER'
                                         and brid_billing_rel_id = billingRel.billing_rel_id
                                       group by rate_percentage, inv_line_type)
                               group by rate_percentage)) loop
              -- DBMS_OUTPUT.PUT_LINE('c.item ' || c.item || ' v_invoice.max_unit_price ' || v_invoice.max_unit_price);
              if (abs(c.item) > v_invoice.max_unit_price and
                 v_invoice.max_unit_price != 0) then
                quantity := trunc(c.item / v_invoice.max_unit_price);
                amount   := quantity * v_invoice.max_unit_price;
                utl_file.put_line(ftype,
                                  c.item01 || (numElems + 1) ||
                                  c.item02 || ',' || quantity || ',' ||
                                  TRIM(TO_CHAR(NVL(v_invoice.max_unit_price,'0'),'999999999999999990.00')) ||
                                  ',' || 'EA' || ',' ||
                                  c.itemline || TRIM(TO_CHAR(NVL(amount, '0'),'999999999999999990.00')));
                if (abs(c.item - amount) > 0) then
                  if (sign(quantity) = -1) then
                    utl_file.put_line(ftype,
                                      c.item01 || (numElems + 2) ||
                                      c.item02 || ',' || -1 || ',' ||
                                      TRIM(TO_CHAR(NVL((abs(c.item - amount)), '0'),'999999999999999990.00')) ||
                                      ',' || 'EA' || ',' ||
                                      c.itemline || TRIM(TO_CHAR(NVL((c.item - amount),'0'),'999999999999999990.00'))
									  || ',,,,,,,,'); --89-96  /*RITM16419869*/
                  end if;
                  if (sign(quantity) != -1) then
                    utl_file.put_line(ftype,
                                      c.item01 || (numElems + 2) ||
                                      c.item02 || ',' || 1 || ',' ||
                                      TRIM(TO_CHAR(NVL((abs(c.item - amount)),'0'),'999999999999999990.00')) ||
                                      ',' || 'EA' || ',' ||
                                      c.itemline ||
                                      TRIM(TO_CHAR(NVL((c.item - amount),'0'),'999999999999999990.00'))
									  || ',,,,,,,,'); --89-96  /*RITM16419869*/
                  end if;
                end if;
                utl_file.put_line(ftype,
                                  c.vat01 || (numElems + 1) || c.vatline ||
                                  TRIM(TO_CHAR(NVL(round(((amount * c.vat)/c.item),2),'0'),'999999999999999990.00'))
								  || ',,,,,,,,'); --89-96  /*RITM16419869*/
                if (abs(c.item - amount) > 0) then
                  elems := elems + 4;
                  utl_file.put_line(ftype,
                                    c.vat01 || (numElems + 2) || c.vatline ||
                                    TRIM(TO_CHAR(NVL(round((c.vat - ((amount * c.vat)/c.item)),2),'0'),'999999999999999990.00'))
									|| ',,,,,,,,'); --89-96  /*RITM16419869*/
                  numElems := numElems + 2;
                end if;
                if (abs(c.item - amount) = 0) then
                  elems    := elems + 2;
                  numElems := numElems + 1;
                end if;
              end if;
              if (abs(c.item) <= v_invoice.max_unit_price or v_invoice.max_unit_price = 0) then
                elems := elems + 2;
                if (v_invoice.invoiceAmount < 0) then
                  utl_file.put_line(ftype,
                                    c.item01 || (numElems + 1) ||
                                    c.item02 || ',' || -1 || ',' ||
                                    TRIM(TO_CHAR(abs(NVL(c.item, '0')),'999999999999999990.00')) ||
                                    ',' || 'EA' || ',' || c.itemline ||
                                    TRIM(TO_CHAR(NVL(c.item, '0'),'999999999999999990.00'))
									|| ',,,,,,,,'); --89-96  /*RITM16419869*/
                else
                  utl_file.put_line(ftype,
                                    c.item01 || (numElems + 1) ||
                                    c.item02 || ',' || 1 || ',' ||
                                    TRIM(TO_CHAR(abs(NVL(c.item, '0')),'999999999999999990.00')) ||
                                    ',' || 'EA' || ',' || c.itemline ||
                                    TRIM(TO_CHAR(NVL(c.item, '0'),'999999999999999990.00'))
									|| ',,,,,,,,'); --89-96  /*RITM16419869*/
                end if;
                utl_file.put_line(ftype,
                                  c.vat01 || (numElems + 1) ||
                                  c.vatLine || TRIM(TO_CHAR(NVL(round(c.vat, 2), '0'),'999999999999999990.00'))
								  || ',,,,,,,,'); --89-96  /*RITM16419869*/
                numElems := numElems + 1;
              end if;
            end loop;
          end if;

          -- *************************************************************************************
          -- END CHINA REV and TAX
          -- *************************************************************************************

          -- *************************************************************************************
          -- BEGIN Colombia REV and TAX
          -- *************************************************************************************

            if (v_invoice.country_code IN ('CO')) then
            -- +++++++++++++++++++++++++++++++++++++++++++++
            -- REV Lines
            -- +++++++++++++++++++++++++++++++++++++++++++++
            for c in (SELECT 'BILCCL' || ',' || --1
                             'CCLARINVES' || ',' || --2
                             TO_CHAR(v_invoice.BILLER_RUN_DATE, 'YYYYMMDD') || ',' || --3
                             TO_CHAR(v_invoice.BILLER_RUN_DATE, 'YYYYMMDD') || ',' || --4
                             v_invoice.invoice_number || ',' || --5
                             'DERIVE' || ',' || --6
                             case when v_invoice.invoiceAmount < 0 then
                                'CM'
                             else
                                'INV'
                             end || ',,,,,' || --7-11
                             v_invoice.ORIGINAL_CURR || ',' || --12
                             DECODE(v_invoice.country_code,'AR','ARBANK_DAILY','MOR') || ',' || --13
                             TO_CHAR(v_invoice.BILLER_RUN_DATE, 'YYYYMMDD') || ',,' || --14-15
                             customerSite || ',,,' || --16-18
                             'IMMEDIATE' || ',' || --19
                             v_invoice.invoice_billing_ref || ',' || --20
                             replace(v_invoice.invoiceHeader, ',', '-') || --21
                             ',,,,,,,,,,,,,,,' || --22-35
                             nvl(v_invoice.cclArHeaderDffContext, 'CCLAR') || ',' || --36
                             DECODE(v_invoice.ccl_ar_ibs,'Y',v_invoice.b_buc,'') || ',' || -- 37 /*RITM16419869*/
                             ',,,,,' || --38-42
                             'If questions please contact ' ||
                             v_invoice.invoicecontact || ',,' || --43-44
                             DECODE(v_invoice.ccl_ar_ibs,'Y',v_invoice.s_buc,'') || ',' || -- 45
                             ',,' || contractNumber || ',' || --46-48
                             ',,,' || --49-51
                             (numElems + rownum) || ',,' || --52-53
                             replace(DECODE(v_invoice.REFERENCE_ID,NULL,'',v_invoice.REFERENCE_ID || ' - ') ||
                                     service_desc,
                                     ',',
                                     '-') || ',' || --54
                             case when (item + vat) < 0 then
                                -1
                             else
                                1
                             end || ',' || --55
                             TRIM(TO_CHAR(abs(NVL(item, '0')),'999999999999999990.00')) || ',' || --56
                             'EA' || ',' || --57
                             DECODE(v_invoice.SELLER_VAT_REGIME,'Y',TRIM(TO_CHAR(NVL(rate_percentage, 0) * 100,'999999999999999990.000')),'')|| ',,' || --58-59 /*RITM15507414*/
                             'CCLAR' || ',' || --60
                             replace(DECODE(v_invoice.REFERENCE_ID,NULL,'',v_invoice.REFERENCE_ID || ' - ') ||
                                     service_desc,
                                     ',',
                                     '-') || ',,,,,' || --61-65
                             replace(substr(
                                            to_char(v_invoice.biller_run_date,'yyyy-mm-dd') || '|' || -- 1s
                                            v_invoice.INVOICE_NUMBER || '|' || -- 2s
                                            v_invoice.BILL_REL_KEY || '|' || -- 3s
                                            case when v_invoice.je_type = 'TP' then
                                               v_invoice.s_buc || '|' || -- 4s
                                               v_invoice.b_buc || '|' || -- 5s
                                               v_invoice.je_line_type_cat || '|' || -- 6s
                                               v_invoice.expense_category || '|' || -- 7s
                                               v_invoice.je_line_type_sub_sub_cat || '|' || -- 8s
                                               v_invoice.je_line_tax_type || '|' || -- 9s
                                               v_invoice.je_line_type_interco -- 10s
                                            when v_invoice.in_sending_gl = 'AHB' then
                                               v_invoice.s_buc || '|' || -- 4s
                                               v_invoice.b_buc || '|' || -- 5s
                                               v_invoice.je_line_type_cat || '|' || -- 6s
                                               v_invoice.expense_category || '|' || -- 7s
                                               v_invoice.je_line_type_sub_sub_cat || '|' || -- 8s
                                               v_invoice.je_line_tax_type || '|' || -- 9s
                                               v_invoice.je_line_type_interco || '|' || -- 10s
                                               to_char(v_invoice.tp_percentage * 100, 'FM990.000') || '|' || -- 11s
                                               to_char(v_invoice.vat_percentage * 100, 'FM990.000') || '|' || -- 12s
                                               split(v_invoice.bex_je_description, '|', 2) || '|' || -- 13s
                                               split(v_invoice.bex_je_description, '|', 3) || '|' || -- 14s
                                               split(v_invoice.bex_je_description, '|', 4) || '|' || -- 15s
                                               split(v_invoice.bex_je_description, '|', 5) || '|' || -- 16s
                                               LTRIM(split(v_invoice.bex_je_description, '|', 6)) || '|' || -- 17s
                                               LTRIM(split(v_invoice.bex_je_description, '|', 7)) -- 18s
                                            else
                                               v_invoice.s_buc || '|' || -- 4s
                                               v_invoice.b_buc || '|' || -- 5s
                                               v_invoice.je_line_type_cat || '|' || -- 6s
                                               v_invoice.expense_category || '|' || -- 7s
                                               v_invoice.je_line_type_sub_sub_cat || '|' || -- 8s
                                               v_invoice.je_line_tax_type || '|' || -- 9s
                                               v_invoice.je_line_type_interco || '|' || -- 10s
                                               to_char(v_invoice.tp_percentage * 100, 'FM990.000') || '|' || -- 11s
                                               to_char(v_invoice.vat_percentage * 100, 'FM990.000') || '|' || -- 12s
                                               v_invoice.seller_cost_center || '|' || -- 13s
                                               v_invoice.orig_gl_account || '|' || -- 14s
                                               v_invoice.bex_je_description
                                            end,
                                            0,
                                            150),
                                     ',') || ',' || --66
                             DECODE(v_invoice.ccl_ar_ibs,'Y',v_invoice.adn,'') || ',' || --67
                             ',,,,,,,,' || 'REV' || ',' || --68-76
                             v_invoice.BALANCING_SEGMENT || ',' || --77
                             v_invoice.EXPENSE_ACCOUNT || ',' || --78
                            /* nvl(CASE WHEN v_invoice.je_line_type_cat = 'AP' AND v_invoice.expense_category != 'IBS' THEN
                                    CASE WHEN v_invoice.s_gl = 'CCL' THEN
                                       v_invoice.S_BAL_SEG
                                    ELSE
                                       billerif_utils.get_COA_Company_Code(v_invoice.S_BAL_SEG)
                                    END
                                 WHEN v_invoice.je_line_type_cat = 'AR' AND v_invoice.expense_category != 'IBS' THEN
                                    CASE WHEN v_invoice.b_gl = 'CCL' THEN
                                       v_invoice.b_bal_seg
                                    ELSE
                                       CASE LENGTH(v_invoice.b_bal_seg) when 4 THEN
                                          v_invoice.b_bal_seg
                                       ELSE
                                          billerif_utils.get_COA_Company_Code(v_invoice.b_bal_seg)
                                       END
                                    END
                                 ELSE
                                    ''
                                 END
                                ,'0000') || ',' || --79 (Trading Partner)*/
                             NVL(CASE WHEN UPPER(v_invoice.tp_required) in ('Y','T') THEN
                               CASE WHEN UPPER(v_invoice.outbound_id) = 'BUYER' THEN
                                  CASE LENGTH(v_invoice.s_bal_seg) when 4 THEN
                                      v_invoice.s_bal_seg
                                  ELSE
                                      billerif_utils.get_COA_Company_Code(v_invoice.s_bal_seg)
                                  END
                               WHEN UPPER(v_invoice.outbound_id) = 'SELLER' THEN
                                  CASE LENGTH(v_invoice.b_bal_seg) when 4 THEN
                                      v_invoice.b_bal_seg
                                  ELSE
                                      billerif_utils.get_COA_Company_Code(v_invoice.b_bal_seg)
                                  END
                               END
                             END,'0000')  || ',' || --79 (Trading Partner)
                             nvl(v_invoice.COST_CENTER, '000000') || ',' || --80
                             '000' || ',' || --81
                             trim(nvl(v_invoice.project, '0000000000')) || ',' || --82
                             nvl(v_invoice.REFERENCE_ID, '000000') || ',' || --83
                             nvl(v_invoice.product_line, '000000') || ',' || --84
                             'P' || ',' || --85
                             '000000000' || ',' || --86
                             '000000' || ',' || --87
                             TRIM(TO_CHAR(NVL(item, '0'),'999999999999999990.00')) --88
							 || ',,,,,,,,'  --89-96  /*RITM16419869*/
                             as ITEMLINE,
                             -- +++++++++++++++++++++++++++++++++++++++++++++
                             -- TAX Lines
                             -- +++++++++++++++++++++++++++++++++++++++++++++
                             'BILCCL' || ',' || --1
                              'CCLARINVES' || ',' || --2
                              TO_CHAR(v_invoice.BILLER_RUN_DATE, 'YYYYMMDD') || ',' || --3
                              TO_CHAR(v_invoice.BILLER_RUN_DATE, 'YYYYMMDD') || ',' || --4
                              v_invoice.invoice_number || ',' || --5
                              'DERIVE' || ',' || --6
                              case when v_invoice.invoiceAmount < 0 then
                                 'CM'
                              else
                                 'INV'
                              end || ',,,,,' || --7-11
                              v_invoice.ORIGINAL_CURR || ',' || --12
                              DECODE(v_invoice.country_code,'AR','ARBANK_DAILY','MOR') || ',' || --13
                              TO_CHAR(v_invoice.BILLER_RUN_DATE, 'YYYYMMDD') || ',,' || --14-15
                              customerSite || ',,,' || --16-18
                              'IMMEDIATE' || ',' || --19
                              v_invoice.invoice_billing_ref || ',' || --20
                              replace(v_invoice.invoiceHeader, ',', '-') || --21
                              ',,,,,,,,,,,,,,,' || --22-35
                              nvl(v_invoice.cclArHeaderDffContext, 'CCLAR') || ',' || --36
                              DECODE(v_invoice.ccl_ar_ibs,'Y',v_invoice.b_buc,'') || ',' || -- 37 /*RITM16419869*/
                              ',,,,,' || --68-42
                              'If questions please contact ' || v_invoice.invoicecontact || ',,' || --43-44
                              DECODE(v_invoice.ccl_ar_ibs,'Y',v_invoice.s_buc,'') || ',' || -- 45
                              ',,' || contractNumber || ',' || --46-48
                              ',,,' || --49-51
                              (numElems + rownum) || ',,' || --52-53
                              replace(DECODE(v_invoice.REFERENCE_ID,NULL,'',v_invoice.REFERENCE_ID || ' - ') ||
                                      service_desc,
                                      ',',
                                      '-') || ',,,,' || --54-57
                              DECODE(v_invoice.SELLER_VAT_REGIME,'Y',TRIM(TO_CHAR(NVL(rate_percentage, 0) * 100,'999999999999999990.000')),'')|| ',,' || --58-59 /*RITM15507414*/
                              'CCLAR' || ',' || --60
                              replace(DECODE(v_invoice.REFERENCE_ID,NULL,'',v_invoice.REFERENCE_ID || ' - ') ||
                                      service_desc,
                                      ',',
                                      '-') || ',,,,,' || --61-65
                              replace(substr(
                                             to_char(v_invoice.biller_run_date,'yyyy-mm-dd') || '|' || -- 1s
                                             v_invoice.INVOICE_NUMBER || '|' || -- 2s
                                             v_invoice.BILL_REL_KEY || '|' || -- 3s
                                             case when v_invoice.je_type = 'TP' then
                                               v_invoice.s_buc || '|' || -- 4s
                                               v_invoice.b_buc || '|' || -- 5s
                                               v_invoice.je_line_type_cat || '|' || -- 6s
                                               v_invoice.expense_category || '|' || -- 7s
                                               v_invoice.je_line_type_sub_sub_cat || '|' || -- 8s
                                               v_invoice.je_line_tax_type || '|' || -- 9s
                                               v_invoice.je_line_type_interco -- 10s
                                             when v_invoice.in_sending_gl = 'AHB' then
                                               v_invoice.s_buc || '|' || -- 4s
                                               v_invoice.b_buc || '|' || -- 5s
                                               v_invoice.je_line_type_cat || '|' || -- 6s
                                               v_invoice.expense_category || '|' || -- 7s
                                               v_invoice.je_line_type_sub_sub_cat || '|' || -- 8s
                                               v_invoice.je_line_tax_type || '|' || -- 9s
                                               v_invoice.je_line_type_interco || '|' || -- 10s
                                               to_char(v_invoice.tp_percentage * 100, 'FM990.000') || '|' || -- 11s
                                               to_char(v_invoice.vat_percentage * 100, 'FM990.000') || '|' || -- 12s
                                               split(v_invoice.bex_je_description, '|', 2) || '|' || -- 13s
                                               split(v_invoice.bex_je_description, '|', 3) || '|' || -- 14s
                                               split(v_invoice.bex_je_description, '|', 4) || '|' || -- 15s
                                               split(v_invoice.bex_je_description, '|', 5) || '|' || -- 16s
                                               LTRIM(split(v_invoice.bex_je_description, '|', 6)) || '|' || -- 17s
                                               LTRIM(split(v_invoice.bex_je_description, '|', 7)) -- 18s
                                             else
                                               v_invoice.s_buc || '|' || -- 4s
                                               v_invoice.b_buc || '|' || -- 5s
                                               v_invoice.je_line_type_cat || '|' || -- 6s
                                               v_invoice.expense_category || '|' || -- 7s
                                               v_invoice.je_line_type_sub_sub_cat || '|' || -- 8s
                                               v_invoice.je_line_tax_type || '|' || -- 9s
                                               v_invoice.je_line_type_interco || '|' || -- 10s
                                               to_char(v_invoice.tp_percentage * 100, 'FM990.000') || '|' || -- 11s
                                               to_char(v_invoice.vat_percentage * 100, 'FM990.000') || '|' || -- 12s
                                               v_invoice.seller_cost_center || '|' || -- 13s
                                               v_invoice.orig_gl_account || '|' || -- 14s
                                               v_invoice.bex_je_description -- 15s
                                             end,
                                                0,
                                                150),
                                         ',') || ',' || --66
                              DECODE(v_invoice.ccl_ar_ibs,'Y',v_invoice.adn,'') || ',' || --67
                              ',,,,,,,,' || 'TAX' || ',' || --68-76
                              v_invoice.BALANCING_SEGMENT || ',' as --77  (Company Code)
                              PART1,
                              CASE when v_invoice.country_code = 'MX' THEN
                                 '2076001008'
                              when v_invoice.country_code = 'IN' THEN
                                 '2076001311'
                              ELSE
                                 nvl(v_invoice.ACCOUNT_VAT, '2076001001')
                              END || ',' as PART2, --78
                             /* nvl(CASE WHEN v_invoice.je_line_type_cat = 'AP' AND v_invoice.expense_category != 'IBS' THEN
                                     CASE WHEN v_invoice.s_gl = 'CCL' THEN
                                        v_invoice.S_BAL_SEG
                                     ELSE
                                        billerif_utils.get_COA_Company_Code(v_invoice.S_BAL_SEG)
                                     END
                                  WHEN v_invoice.je_line_type_cat = 'AR' AND v_invoice.expense_category != 'IBS' THEN
                                     CASE WHEN v_invoice.b_gl = 'CCL' THEN
                                        v_invoice.b_bal_seg
                                     ELSE
                                        CASE LENGTH(v_invoice.b_bal_seg) when 4 THEN
                                           v_invoice.b_bal_seg
                                        ELSE
                                           billerif_utils.get_COA_Company_Code(v_invoice.b_bal_seg)
                                        END
                                     END
                                  ELSE
                                     ''
                                  END,'0000') || ',' || --79 (Trading Partner)*/
                              /*NVL(CASE WHEN UPPER(v_invoice.tp_required) in ('Y','T') THEN
                                CASE WHEN UPPER(v_invoice.outbound_id) = 'BUYER' THEN v_invoice.s_bal_seg
                                WHEN UPPER(v_invoice.outbound_id) = 'SELLER' THEN v_invoice.b_bal_seg END
                              END,'0000')  || ',' || --79 (Trading Partner)*/
                              '0000'|| ',' || --79 (Trading Partner)*/
                              '000000' || ',' || --80
                              '000' || ',' || --81
                              '0000000000' || ',' || --82 Project Code
                              '000000' || ',' || --83
                              nvl(v_invoice.product_line_tp, '000000') || ',' || --84
                              'P' || ',' || --85
                              '000000000' || ',' || --86
                              '000000' || ','  --87
                              as PART3,
                              TRIM(TO_CHAR(NVL(round(vat, 2), '0'),'999999999999999990.00')) --88
							  || ',,,,,,,,'  --89-96  /*RITM16419869*/
                              as VATLINE1,
                              TRIM(TO_CHAR(NVL(round(NVL(item, 0)*0.008*-1, 2), '0'),'999999999999999990.00')) --88
                              || ',,,,,,,,'  --89-96  /*RITM16419869*/
							  as VATLINE2,
                              TRIM(TO_CHAR(NVL(round(NVL(item, 0)*0.008, 2), '0'),'999999999999999990.00')) --88
                              || ',,,,,,,,'  --89-96  /*RITM16419869*/
							  as VATLINE3
                        from (select sum(item_amt) item,
                                     sum(vat_amt) vat,
                                     rate_percentage,
                                     max(service_desc) service_desc
                                from (select case when inv_line_type = 'ITEM' then
                                                sum(tax_comp_amt + ntax_comp_amt)
                                             else
                                                0
                                             end as item_amt,
                                             case when inv_line_type = 'VAT' then
                                                sum(tax_comp_amt + ntax_comp_amt)
                                             else
                                                0
                                             end as vat_amt,
                                             rate_percentage,
                                             inv_line_type,
                                             nvl(max(service_name_translated),
                                                 max(service_desc)) service_desc
                                      --max(service_desc) service_desc
                                        from t_invoice_line_his
                                       where invoice_prefix_seq_id = invoiceRecord.idInvoice
                                         and inv_line_type != 'HEADER'
                                         and brid_billing_rel_id = billingRel.billing_rel_id
                                       group by rate_percentage, inv_line_type)
                               group by rate_percentage)) loop
              elems    := elems + 4;
              numElems := numElems + 1;
              utl_file.put_line(ftype, c.ITEMLINE);
              utl_file.put_line(ftype, c.PART1||c.PART2||c.PART3||c.VATLINE1);
              utl_file.put_line(ftype, c.PART1||'2270506000,'||c.PART3||c.VATLINE2);
              utl_file.put_line(ftype, c.PART1||'1100401000,'||c.PART3||c.VATLINE3);
            end loop;
          end if;

          -- *************************************************************************************
          -- END Colombia REV and TAX
          -- *************************************************************************************

        end if;
      end loop;
    end loop;
    select to_char(run_on, 'YYYYMM')
      into runDate
      from t_outbound_summary v
     where run_seq_id = run
       and gl_identifier = 'CCA';
    select l.VALUE
      into runType
      from t_look_up l
     where l.type = 'INTERFACES'
       and l.look_Code = 'cclar.out'
       and l.look_name = 'runtype';
    /*IF (rusiaError = 'true') THEN
      runType := 'test';
    END IF;*/
    IF (custSiteError = 'true') THEN
      runType := 'test';
    END IF;
    trailer_buffer := 'BILCCL,CCLARINVES,' || runDate || '15,TRAILER,' ||
                      to_char(elems) || ',' || runType;
    utl_file.put_line(ftype, trailer_buffer);
    utl_file.fclose(ftype);
    send_cts(dir,
             filename || '.' || countryRecord.Country_Code || '-' ||
             fileDate,
             ftpdir,
             usr,
             pass,
             to_path);
    utl_file.fremove(dir,
                     filename || '.' || countryRecord.Country_Code || '-' ||
                     fileDate);

    IF (custSiteError = 'true') THEN
      --send email that customer site number have errors
      SEND_FORMATED_MAIL('gebiller.projectteam@ge.com',
                         'cisstkbiller.interface@ge.com, Ricardo.Carrillo@ge.com',
                         '',
                         'Biller has extracted a TEST file to CCL AR',
                         'Biller has extracted a TEST file to CCL AR',
                         '<b>Biller has extracted a TEST file to CCL AR (customer site number).<BR/><BR/>Please take any necessary actions.</b>');
    END IF;

    IF (rusiaError = 'true') THEN
    --send email that rusia have errors
    SEND_FORMATED_MAIL('gebiller.projectteam@ge.com',
                       'cisstkbiller.interface@ge.com, Ricardo.Carrillo@ge.com',
                       '',
                       'Contract number is missing in file to CCL AR',
                       'Contract number is missing in file to CCL AR',
                       '<table class="tableRecord" style="width:60%;">
                        <tr>
                         <th>Document Number</th>
                         <th>Ref Id</th>
                        </tr>'
                        || rusiaErrorDetail || '</table>');

  END IF;

  end loop;

  --DBMS_OUTPUT.PUT_LINE('finish');
End CCL_FILE_AR_REC_GL_ES;
