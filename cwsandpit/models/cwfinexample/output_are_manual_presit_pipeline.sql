select distinct

sha2(records_id)                                                     as _nk_transin,
pl.txtmonth                                             as _snapshot_date,
null                                                            as EVENT_STATUS,
null                                                            as EVENT_ERROR_STRING,
null                                                            as NO_RETRIES,
null                                                            as STAN_RULE_IDENT,
null                                                            as PROCESS_ID,
null                                                            as SUB_SYSTEM_ID,
null                                                            as MESSAGE_ID,
'FDU'                                                            as REMITTING_SYSTEM_ID,
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
pl.txtmonth                                                        as AE_ACCOUNTING_DATE,
null                                                            as AE_POS_NEG,
null                                                            as AE_DIMENSION_1,
uw_year                                                         as AE_DIMENSION_2,
null                                                            as AE_DIMENSION_3,
major_class                                                     as AE_DIMENSION_4,
target_market                                                   as AE_DIMENSION_5,
'000'                                                           as AE_DIMENSION_6,
placing_basis                                                   as AE_DIMENSION_7,
pm.contracttypecode                                             as AE_DIMENSION_8,
null                                                            as AE_DIMENSION_9,
null                                                            as AE_DIMENSION_10,
minorish_class                                                  as AE_DIMENSION_11,
null                                                            as AE_DIMENSION_12,
null                                                            as AE_DIMENSION_13,
null                                                            as AE_DIMENSION_14,
null                                                            as AE_DIMENSION_15,
null                                                            as DESCRIPTION,
null                                                            as PARENT_TRAN_NO,
null                                                            as PARENT_TRAN_VER,
null                                                            as PARENT_TYPE,
policy_line_reference                            as CONTRACT_CLICODE,
null                                                            as CONTRACT_PART_CLICODE,
null                                                            as TRANSACTION_TYPE_CLICODE,
null                                                            as BO_BOOK_CLICODE,
null                                                            as IPE_ENTITY_CLIENT_CODE,
entity  as PL_PARTY_LEGAL_CLICODE,
null                                                            as PBU_PARTY_BUS_CLIENT_CODE,
sett_currency_code                               as CU_CURRENCY_ISO_CODE,
null                                                            as GROSS_AMOUNT,
null                                                            as NET_AMOUNT,
null                                                            as TAX_AMOUNT,
null                                                            as TAX_CODE1,
null                                                            as TAX_CODE2,
null                                                            as TAX_CODE3,
null                                                            as INVOICE_DATE,
date_trunc('MONTH',d.date::date)                                as OTHER_DATE1,
date_truncate('MONTH',txtinception::date)                       as OTHER_DATE2,
null                                                            as TOTAL_AMOUNT,
'Inward Written'                                                as CLIENT_TEXT1,
'Pipeline'                                                      as CLIENT_TEXT2,
fc.parent_category                                              as CLIENT_TEXT3,
fc.category                                                     as CLIENT_TEXT4,
financial_sub_category                           as CLIENT_TEXT5,
'N'                                                             as CLIENT_TEXT6, --needs to be a lookup
'n/a'                                                           as CLIENT_TEXT7,
'n/a'                                                           as CLIENT_TEXT8,
orig_currency_code                               as CLIENT_TEXT9,
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
'Pipeline'                                                      as CLIENT_TEXT20,
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

    from architecture_db.cw_are_airtable.pipeline_presit pl
    
        join cwsandpit.dim_dates d 
            on d.date = pl.txtmonth
           
        join cwsandpit.placingbasismapping pm
            on pl.placing_basis = pm.placingbasiscode
    
        join cwsandpit.dim_financial_categories fc
            on pl.financial_sub_category = fc.sub_category