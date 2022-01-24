
with at_position as

(
  
  select distinct 
    tl.insured_line_key,
    il.policy_reference,
    il.line_status,
    tl.risk_key,
    r.placing_basis,
    r.period_basis,
    r.contract_type,
    p.party_code,
    tl.class_of_business_key,
    cob.tier_1_code,
    cob.tier_3_code,
    tl.uw_year_key,
    tl.inception_date_key,
    tl.expiry_date_key,
    tl.snapshot_date
    from cwsandpit.fct_trans_periodic_snapshot tl
    
        join cwsandpit.dim_insured_line il 
            on tl.insured_line_key = il.insured_line_key
  
        join cwsandpit.dim_risk r
            on tl.risk_key = r.risk_key
  
        join cwsandpit.dim_class_of_business cob
            on tl.class_of_business_key = cob.class_of_business_key
  
        join cwsandpit.dim_parties p  
            on tl.producing_team_key = p.party_key
  
  where tl.snapshot_date = '2021-06-01'
  and tl.row_source = 'written'      


)



select distinct

records_id as _nk_final_transin,
ap.snapshot_date as _snapshot_date,
null as EVENT_ERROR_STRING,
null as NO_RETRIES,
null as STAN_RULE_IDENT,
null as PROCESS_ID,
null as SUB_SYSTEM_ID,
null as MESSAGE_ID,
null as REMITTING_SYSTEM_ID,
policy_line_reference as CONTRACT_CLICODE,
'DEFAULT' as PRODUCT_CLICODE,
null as BO_BOOK_CLICODE,
null as DESCRIPTION,
policy_line_reference as CONTRACT_NUMBER,
null as CONTRACT_VERSION_NO,
null as CONTRACT_VERSION_DATE,
null as CONTRACT_STATUS,
null as ISSUE_DATE,
null as PAID_DATE,
ap.inception_date_key as START_DATE,
ap.expiry_date_key as END_DATE,
null as MOVEMENT_TYPE,
null as CH_CHANNEL_CLICODE,
null as CU_CURRENCY_ISO_CODE,
null as CONTRACT_TYPE,
null as CESSION_PERCENT,
null as PREMIUM_PERCENT,
null as CLAIM_CAP_AMOUNT,
null as NF_OPTION_FLAG,
null as DIV_OPTION_FLAG,
null as COVER_NOTE_DATE,
null as COVER_NOTE_DESCRIPTION,
null as COVER_NOTE_START_DATE,
null as COVER_NOTE_END_DATE,
null as COVER_NOTE_SIGNATURE_DATE,
null as COVER_NOTE_SIGNED_BY,
null as COVER_NOTE_SIGNED_FLAG,
null as JURISDICTION,
null as SIGNATURE_DATE,
null as INDEMNITY_AMOUNT,
null as BENEFIT_LIMIT,
null as PREMIUM_AMOUNT,
null as PREMIUM_FREQUENCY_CLICODE,
null as PREMIUM_TERM,
null as CONTRACT_FEE,
ap.placing_basis as CLIENT_TEXT1,
null as CLIENT_TEXT2,
ap.period_basis as CLIENT_TEXT3,
ap.party_code as CLIENT_TEXT4,
null as CLIENT_TEXT5,
null as CLIENT_TEXT6,
null as CLIENT_TEXT7,
null as CLIENT_TEXT8,
null as CLIENT_TEXT9,
null as CLIENT_TEXT10,
null as CLIENT_TEXT11,
null as CLIENT_TEXT12,
null as CLIENT_TEXT13,
null as CLIENT_TEXT14,
null as CLIENT_TEXT15,
null as CLIENT_TEXT16,
null as CLIENT_TEXT17,
null as CLIENT_TEXT18,
null as CLIENT_TEXT19,
null as CLIENT_TEXT20,
null as CLIENT_AMOUNT1,
null as CLIENT_AMOUNT2,
null as CLIENT_AMOUNT3,
null as CLIENT_AMOUNT4,
null as CLIENT_AMOUNT5,
null as CLIENT_AMOUNT6,
null as CLIENT_AMOUNT7,
null as CLIENT_AMOUNT8,
null as CLIENT_AMOUNT9,
null as CLIENT_AMOUNT10,
null as CLIENT_DATE1,
null as CLIENT_DATE2,
null as CLIENT_DATE3,
null as CLIENT_DATE4,
null as CLIENT_DATE5,
null as CLIENT_DATE6,
null as CLIENT_DATE7,
null as CLIENT_DATE8,
null as CLIENT_DATE9,
null as CLIENT_DATE10,
null as REINSURANCE_FLAG,
null as CESSION_AMOUNT,
null as NET_LOSS_RETENTION,
null as RETENTION_REASON,
null as CAP_AMOUNT,
null as CAP_PERCENT,
null as LOSS_OCCURRANCE,
null as CLAIM_AGG_FREQUENCY_CLICODE,
null as EXP_LOSS_RATIO,
null as DOUBLE_INSURANCE_FLAG,
null as RETROCESSION_FLAG,
null as FEE_PERCENT,
null as FEE_AMOUNT

    from architecture_db.cw_are_airtable.rip_presit rip
    
        join at_position ap on 
            rip.policy_line_reference = ap.policy_reference