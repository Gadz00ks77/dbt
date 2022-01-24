
with at_position as

(
  
  select distinct 
    tl.insured_line_key,
    il.policy_reference,
    il.line_status,
    tl.risk_key,
    r.placing_basis,
    r.contract_type,
    p.party_code,
    tl.class_of_business_key,
    cob.tier_1_code,
    cob.tier_3_code,
    tl.uw_year_key,
    tl.snapshot_date

    from cwsandpit.fct_trans_periodic_snapshot tl
    
        join cwsandpit.dim_insured_line il 
            on tl.insured_line_key = il.insured_line_key
  
        join cwsandpit.dim_risk r
            on tl.risk_key = r.risk_key
  
        join cwsandpit.dim_class_of_business cob
            on tl.class_of_business_key = cob.class_of_business_key
  
        join cwsandpit.dim_parties p  
            on tl.legal_entity_key = p.party_key
  
  where tl.snapshot_date in ('2021-06-01','2021-07-01','2021-08-01')
  and tl.row_source = 'written'      


)

select distinct

sha2(records_id)                                                      as _nk_transin,
d.date                                                          as _snapshot_date,
null                                                            as EVENT_STATUS,
null                                                            as EVENT_ERROR_STRING,
null                                                            as NO_RETRIES,
null                                                            as STAN_RULE_IDENT,
null                                                            as PROCESS_ID,
null                                                            as SUB_SYSTEM_ID,
null                                                            as MESSAGE_ID,
'FDU'                                                           as REMITTING_SYSTEM_ID,
null                                                            as ARRIVAL_TIME,
null                                                            as SOURCE_TRAN_NO,
sha2(records_id)                                                      as SOURCE_TRAN_VER,
null                                                            as EVENT_AUDIT_ID,
null                                                            as BUSINESS_DATE,
'Manual'                                                        as SOURCE_SYS_INST_CODE,
null                                                            as STATIC_SYS_INST_CODE,
null                                                            as PARTY_SYS_INST_CODE,
null                                                            as CONTRACT_SYS_INST_CODE,
null                                                            as ACTIVE,
null                                                            as INPUT_BY,
null                                                            as INPUT_TIME,
null                                                            as AE_ACC_EVENT_TYPE_ID,
null                                                            as AE_SUB_EVENT_ID,
d.date                                                           as AE_ACCOUNTING_DATE,
null                                                            as AE_POS_NEG,
null                                                            as AE_DIMENSION_1,
year(ap.uw_year_key::date)                                      as AE_DIMENSION_2, --needs amend to templates?
null                                                            as AE_DIMENSION_3,
ap.tier_1_code                                                  as AE_DIMENSION_4,
tm.targetmarket                                                 as AE_DIMENSION_5,
'000'                                                           as AE_DIMENSION_6,
ap.placing_basis                                                as AE_DIMENSION_7,
ap.contract_type                                                as AE_DIMENSION_8,
null                                                            as AE_DIMENSION_9,
null                                                            as AE_DIMENSION_10,
ap.tier_3_code                                                  as AE_DIMENSION_11,
null                                                            as AE_DIMENSION_12,
null                                                            as AE_DIMENSION_13,
null                                                            as AE_DIMENSION_14,
null                                                            as AE_DIMENSION_15,
null                                                            as DESCRIPTION,
null                                                            as PARENT_TRAN_NO,
null                                                            as PARENT_TRAN_VER,
null                                                            as PARENT_TYPE,
pl.policy_line_reference                            as CONTRACT_CLICODE,
null                                                            as CONTRACT_PART_CLICODE,
null                                                            as TRANSACTION_TYPE_CLICODE,
null                                                            as BO_BOOK_CLICODE,
null                                                            as IPE_ENTITY_CLIENT_CODE,
case when ap.party_code = 'CVRB' then 120 else 130 end          as PL_PARTY_LEGAL_CLICODE,
null                                                            as PBU_PARTY_BUS_CLIENT_CODE,
settlement_currency_iso_code                     as CU_CURRENCY_ISO_CODE,
null                                                            as GROSS_AMOUNT,
null                                                            as NET_AMOUNT,
null                                                            as TAX_AMOUNT,
null                                                            as TAX_CODE1,
null                                                            as TAX_CODE2,
null                                                            as TAX_CODE3,
null                                                            as INVOICE_DATE,
date_trunc('MONTH',d.date::date)                                as OTHER_DATE1,
null                                                            as OTHER_DATE2,
null                                                            as TOTAL_AMOUNT,
'Inward Written'                                                as CLIENT_TEXT1,
'RIP Estimates'                                                 as CLIENT_TEXT2,
case when financial_category   = 'Premium' then 'Premium' else 'Deduction' end as CLIENT_TEXT3,
case when financial_category   = 'Premium' then 'Premium' else 'Brokerage' end as CLIENT_TEXT4,
financial_category                               as CLIENT_TEXT5,
'N'                                                             as CLIENT_TEXT6,
'n/a'                                                           as CLIENT_TEXT7,
'n/a'                                                           as CLIENT_TEXT8,
original_currency_iso_code                       as CLIENT_TEXT9,
null                                                            as CLIENT_TEXT10,
null                                                            as CLIENT_TEXT11,
null                                                            as CLIENT_TEXT12,
null                                                            as CLIENT_TEXT13,
null                                                            as CLIENT_TEXT14,
null                                                            as CLIENT_TEXT15,
null                                                            as CLIENT_TEXT16,
null                                                            as CLIENT_TEXT17,
null                                                            as CLIENT_TEXT18,
null                                                            as CLIENT_TEXT19,
ap.line_status                                                  as CLIENT_TEXT20,
pl.original_currency_amount                      as CLIENT_AMOUNT1,
pl.settlement_currency_amount                    as CLIENT_AMOUNT2,
null                                                            as CLIENT_AMOUNT3,
null                                                            as CLIENT_AMOUNT4,
null                                                            as CLIENT_AMOUNT5,
null                                                            as CLIENT_AMOUNT6,
null                                                            as CLIENT_AMOUNT7,
null                                                            as CLIENT_AMOUNT8,
null                                                            as CLIENT_AMOUNT9,
null                                                            as CLIENT_AMOUNT10,
null                                                            as AE_VALUE_DATE,
null                                                            as LOCAL_AMOUNT,
null                                                            as LOCAL_CU_CURRENCY_ISO_CODE

    from architecture_db.cw_are_airtable.rip_presit pl
    
        join cwsandpit.dim_dates d 
            on d.date = '2021-06-01'
           
        left join at_position ap
            on pl.policy_line_reference = ap.policy_reference
            and ap.snapshot_date = d.date
        
        left join cwsandpit.targetmarket_mapping tm
            on pl.policy_line_reference = tm.lineref