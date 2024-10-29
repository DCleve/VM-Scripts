import gspread
import pandas as pd
import gspread_dataframe as gd
from datetime import datetime, timedelta
import time
import os
import numpy as np
import pytz

login = os.getlogin()
separator = '\\'
startTime = time.time()

gc=gspread.service_account()

import snowflake.connector
from snowflake.connector import connect
snowflake_pull = connect(user='Dave', password='Quantum314!', account='fva14998.us-east-1')

###Receiving
##Import Run Data
run_gen = gc.open_by_key('1mhdpT207rfUi505J33wAn3m0L_AXuwH28PBzJ22Lp-s').worksheet('Archive')
run_gen_df = pd.DataFrame.from_dict(run_gen.get_all_records())
run_gen_df.dropna(subset=['Run'], inplace=True)

##Parse down run data
run_gen_df["Date"] = run_gen_df['Run'].str.split('-').str[0]

run_gen_df = run_gen_df.loc[run_gen_df['Date'].astype('float64') >= 240608]

##Import RI Data
sql_rec = ("""
select
    reimbursement_invoices.reimbursement_invoice_number as ri_number
    , reimbursement_invoices.total_product_quantity as number_of_cards
    , reimbursement_invoices.processed_by_user_email as processor_email
    , reimbursement_invoices.verified_by_user_email as verifier_email
    , reimbursement_invoices.processing_ended_at_et as processing_ended
    , reimbursement_invoices.verification_ended_at_et as verifying_ended
    , reimbursement_invoices.active_processing_time_minutes as proc_time_minutes
    , reimbursement_invoices.active_verification_time_minutes as ver_time_minutes

    , reimbursement_invoices.processing_condition_discrepancy_quantity as proc_cond_count_entered
    , reimbursement_invoices.verification_condition_discrepancy_quantity as ver_cond_count_entered

    , reimbursement_invoices.processing_missing_discrepancy_quantity as proc_miss_count_entered
    , reimbursement_invoices.verification_missing_discrepancy_quantity as ver_miss_count_entered

    , reimbursement_invoices.processing_extra_quantity_discrepancy_quantity as proc_extra_count_entered
    , reimbursement_invoices.verification_extra_quantity_discrepancy_quantity as ver_extra_count_entered

    , reimbursement_invoices.status as ri_status

    , case
        when processor_email is not null then 'RI Proc'
        else null
        end as ri_proc_tag

    , case
        when verifier_email != processor_email and ver_time_minutes > 5 then 'RI Ver'
        else null
        end as ri_ver_tag

    , reimbursement_invoice_products.product_condition_id as pcid
    , reimbursement_invoice_products.set_name as set_name
    , reimbursement_invoice_products.condition as condition_name
    , reimbursement_invoice_products.sku_value as price
    , reimbursement_invoice_products.inspection_level as inspection_level
    , reimbursement_invoice_products.cabinet as cabinet
    , reimbursement_invoice_products.quantity_stocked as quantity_stocked
    , reimbursement_invoice_products.expected_quantity as expected_quantity
    , reimbursement_invoice_products.priority as priority

from
analytics.core.reimbursement_invoices
inner join analytics.core.reimbursement_invoice_products on reimbursement_invoices.reimbursement_invoice_number = reimbursement_invoice_products.reimbursement_invoice_number

where
    reimbursement_invoices.created_at_et::date >= cast(dateadd(dd, -120, getdate()) as date)
    and reimbursement_invoices.is_auto = false
    and reimbursement_invoices.seller_name <> 'mtg rares'

""")

cursor = snowflake_pull.cursor()
cursor.execute(sql_rec)
rec_df = cursor.fetch_pandas_all()
rec_df.drop(rec_df.filter(like='Unnamed'), axis=1, inplace=True)
rec_df.dropna(subset = ["RI_NUMBER"], inplace=True)

##Fix data types
rec_df['PRICE'] = rec_df['PRICE'].astype('float64')

rec_df.loc[(rec_df['PRIORITY'] == "High") | (rec_df['PRIORITY'] == "Medium"), 'PRIORITY'] = "High"
rec_df.loc[rec_df['PRIORITY'].str[:3] == 'Low', 'PRIORITY'] = "Low"

##Account for lanes
rec_df["Lane_Assign"] = rec_df['RI_NUMBER'].str.split('-').str[2]

rec_df.loc[rec_df['Lane_Assign'] == "PRF", 'INSPECTION_LEVEL'] = "FullCondition"
rec_df.loc[rec_df['Lane_Assign'] == "STD", 'INSPECTION_LEVEL'] = "CountNGo"

##Sleeving
rec_df["sleeved"] = None

mtg_set_list = ['Alpha Edition', 'Beta Edition', 'Unlimited Edition', 'Legends', 'Antiquities', 'Arabian Nights', 'Collector\'s Edition', 'International Edition']

pkm_set_list = ['Skyridge', 'Aquapolis', 'Expedition', 'Neo Destiny', 'Neo Revelation', 'Neo Discovery', 'Neo Genesis', 'Gym Challenge', 'Gym Heroes', 'Team Rocket', 'Base Set 2', 'Fossil', 'Jungle', 'Base Set Shadowless', 'Base Set']

rec_df.loc[(rec_df['PRICE'] >= 25) | (rec_df['SET_NAME'].isin(mtg_set_list)), 'sleeved'] = rec_df['QUANTITY_STOCKED'] ##always sleeve cards over $25 and mtg legacy sets

rec_df.loc[(rec_df['CONDITION_NAME'] == 'Near Mint Holofoil') & (rec_df['SET_NAME'].isin(pkm_set_list)), 'sleeved'] = rec_df['QUANTITY_STOCKED'] #always sleeve pkm legacy sets NMHF

rec_df.loc[(rec_df['CONDITION_NAME'].str[-8:] == 'Holofoil') & (rec_df['SET_NAME'].isin(pkm_set_list)), 'sleeved'] = rec_df['QUANTITY_STOCKED'] #sleeve pkm foils before full stop

##Aggragate quantity stocked, sleeved cards and cabinet splits per RI
rec_df['sleeved'] = rec_df['sleeved'].astype('float64')

sleeved_cards_per_ri = rec_df.groupby('RI_NUMBER')['sleeved'].sum()
rec_df = pd.merge(rec_df, sleeved_cards_per_ri, how='right', on='RI_NUMBER')

cabinet_splits = rec_df.groupby('RI_NUMBER')['CABINET'].nunique()
rec_df = pd.merge(rec_df, cabinet_splits, how='right', on='RI_NUMBER')

quantity_stocked = rec_df.groupby('RI_NUMBER')['QUANTITY_STOCKED'].sum()
rec_df = pd.merge(rec_df, quantity_stocked, how='right', on='RI_NUMBER')

rec_df.rename(columns={'sleeved_x':'sleeved', 'sleeved_y':'sleeved_cards', 'CABINET_x':'Cabinet', 'CABINET_y':'cabinet_splits', 'INSPECTION_LEVEL':'Tag', 'QUANTITY_STOCKED_y':'per_ri_quantity_stocked', 'QUANTITY_STOCKED_x':'per_card_quantity_stocked'}, inplace=True)

##Merge with run data
run_gen_df.rename(columns={'RI':'RI_NUMBER'}, inplace=True)

rec_df = pd.merge(rec_df, run_gen_df, on='RI_NUMBER', how='left')

##Parse down dataframe
rec_df = rec_df[['RI_NUMBER', 'NUMBER_OF_CARDS', 'PROC_TIME_MINUTES', 'VER_TIME_MINUTES', 'PROC_MISS_COUNT_ENTERED', 'PROC_COND_COUNT_ENTERED', 'PROC_EXTRA_COUNT_ENTERED', 'VER_MISS_COUNT_ENTERED', 'VER_COND_COUNT_ENTERED', 'VER_EXTRA_COUNT_ENTERED', 'PROCESSING_ENDED', 'VERIFYING_ENDED', 'sleeved_cards', 'cabinet_splits', 'Tag', 'PROCESSOR_EMAIL', 'VERIFIER_EMAIL', 'RI_STATUS', 'PCID', 'Cabinet', 'per_card_quantity_stocked', 'per_ri_quantity_stocked', 'PRIORITY', 'Run']]

rec_df.dropna(subset=['RI_NUMBER'], inplace=True)

##Sum quantities per run / cabinet combo
rec_df["count"] = rec_df['Run'].astype(str) + rec_df['Cabinet'].astype(str)

cards_filed_per_cabinet = rec_df.groupby('count')['per_card_quantity_stocked'].sum()
rec_df = pd.merge(rec_df, cards_filed_per_cabinet, how='right', on='count')

unique_cards_per_cabinet = rec_df.groupby('count')['PCID'].nunique()
rec_df = pd.merge(rec_df, unique_cards_per_cabinet, how='right', on='count')

rec_df.rename(columns={'per_card_quantity_stocked_x':'qty_stocked_per_pcid_per_ri', 'per_card_quantity_stocked_y':'qty_stocked_per_cabinet', 'PCID_y':'unique_cards_per_cabinet', 'PCID_x':'PCID'}, inplace=True)

##Calculate density per cabinet
rec_df["Density"] = rec_df['qty_stocked_per_cabinet'].astype('float64') / rec_df['unique_cards_per_cabinet'].astype('float64')

##Parse down frame
rec_df["unique"] = rec_df['RI_NUMBER'].astype(str) + rec_df['count'].astype(str)

rec_df.drop_duplicates(subset='unique', inplace=True)

##Rec.csv
rec_norm_df = rec_df.copy()
rec_norm_df.drop_duplicates(subset='RI_NUMBER', inplace=True)

rec_norm_df = rec_norm_df.loc[rec_norm_df['PROC_TIME_MINUTES'].astype('float64') > 0]

rec_norm_df = rec_norm_df[['RI_NUMBER', 'NUMBER_OF_CARDS', 'PROC_TIME_MINUTES', 'VER_TIME_MINUTES', 'PROC_MISS_COUNT_ENTERED', 'PROC_COND_COUNT_ENTERED', 'PROC_EXTRA_COUNT_ENTERED', 'VER_MISS_COUNT_ENTERED', 'VER_COND_COUNT_ENTERED', 'VER_EXTRA_COUNT_ENTERED', 'PROCESSING_ENDED', 'VERIFYING_ENDED', 'sleeved_cards', 'cabinet_splits', 'Tag', 'PROCESSOR_EMAIL', 'VERIFIER_EMAIL']]

rec_norm_string = ["C:", "Users", login, "OneDrive - eBay Inc", "AC-Scripting", "Data CSVs", "Snowflake", "Rec.csv"]
rec_norm_result = separator.join(rec_norm_string)
rec_norm_df.to_csv(rec_norm_result, index=False)

##Create run filing data
run_filing_df = rec_df.copy()
run_filing_df.drop_duplicates(subset='RI_NUMBER', inplace=True)
run_filing_df = run_filing_df[['RI_NUMBER', 'NUMBER_OF_CARDS', 'per_ri_quantity_stocked', 'RI_STATUS']]

##Write data to doc
dataTab = gc.open_by_key('1mhdpT207rfUi505J33wAn3m0L_AXuwH28PBzJ22Lp-s').worksheet('Data')
dataTab.clear()
gd.set_with_dataframe(dataTab, run_filing_df)

##Create new seller verify dataframe
nsv_df = rec_df.copy()
nsv_df.drop_duplicates(subset='RI_NUMBER', inplace=True)
nsv_df = nsv_df.loc[(nsv_df['Tag'] == 'NewSeller') & (nsv_df['RI_STATUS'] == 'Ready to Verify')]

##Aggragate cards
total_cards = nsv_df.groupby('Tag')['NUMBER_OF_CARDS'].sum()
nsv_df = pd.merge(nsv_df, total_cards, how='right', on='Tag')

nsv_df.drop('NUMBER_OF_CARDS_x', axis=1, inplace=True)
nsv_df.rename(columns={'NUMBER_OF_CARDS_y':'Cards'}, inplace=True)

nsv_df = nsv_df[['Cards']]
nsv_df.drop_duplicates(subset='Cards', inplace=True)

##Write data to sheet
nsvTab = gc.open_by_key('1AstvlWqWkW_Mf969kCJZzkCq6e_FEOcG5T7iDIUys98').worksheet('Data')
nsvTab.clear()
gd.set_with_dataframe(nsvTab, nsv_df)

##Filing Time Study Data
filing_ts_df = rec_df.copy()

filing_ts_df.drop_duplicates(subset=['count'], inplace=True)

filing_ts_df = filing_ts_df[['Run', 'Cabinet', 'qty_stocked_per_cabinet', 'Density']]

filing_ts_df.rename(columns={'qty_stocked_per_cabinet':'Cards'}, inplace=True)

##Write data to sheet
filing_tab = gc.open_by_key('1ZCBTdfSlfJRr0iRmDErxiRMUfs8nJuzpDTfzg5aZBxc').worksheet('CardData')
filing_tab.clear()
gd.set_with_dataframe(filing_tab, filing_ts_df)

##Rec projections
rec_proj_df = rec_df.copy()
rec_proj_df = rec_proj_df.loc[rec_proj_df['RI_STATUS'] == "Received"]
rec_proj_df.drop_duplicates(subset='RI_NUMBER', inplace=True)

##Aggragate
cards_by_ri_type = rec_proj_df.groupby('Tag')['NUMBER_OF_CARDS'].sum()
rec_proj_df = pd.merge(rec_proj_df, cards_by_ri_type, how='right', on='Tag')
rec_proj_df.rename(columns={'NUMBER_OF_CARDS_y':'cards_by_ri_type', 'NUMBER_OF_CARDS_x':'NUMBER_OF_CARDS'}, inplace=True)

##Aggragate by prio combination
rec_proj_df["prio_combine"] = rec_proj_df['Tag'].astype(str) + rec_proj_df['PRIORITY'].astype(str)

cards_by_prio = rec_proj_df.groupby('prio_combine')['NUMBER_OF_CARDS'].sum()
rec_proj_df = pd.merge(rec_proj_df, cards_by_prio, how='right', on='prio_combine')
rec_proj_df.rename(columns={'NUMBER_OF_CARDS_y':'cards_by_priority', 'NUMBER_OF_CARDS_x':'NUMBER_OF_CARDS'}, inplace=True)

##Count RIs by prio combination
ris_by_prio_comb = rec_proj_df.groupby('prio_combine')['RI_NUMBER'].nunique()
rec_proj_df = pd.merge(rec_proj_df, ris_by_prio_comb, how='right', on='prio_combine')
rec_proj_df.rename(columns={'RI_NUMBER_x':'RI_NUMBER', 'RI_NUMBER_y':'unique_ris_by_prio_comb'}, inplace=True)

##Total cards by inspection level
ris_by_prio = rec_proj_df.groupby('Tag')['RI_NUMBER'].nunique()
rec_proj_df = pd.merge(rec_proj_df, ris_by_prio, how='right', on='Tag')
rec_proj_df.rename(columns={'RI_NUMBER_x':'RI_NUMBER', 'RI_NUMBER_y':'unique_ris_by_tag'}, inplace=True)

##High/Low Ratio
rec_proj_df["High to Low Ratio"] = rec_proj_df['unique_ris_by_prio_comb'].astype('float64') / rec_proj_df['unique_ris_by_tag'].astype('float64')

rec_proj_df.drop_duplicates(subset='prio_combine', inplace=True)

##Rearrange data
rec_proj_df = rec_proj_df[['PRIORITY', 'Tag', 'cards_by_ri_type', 'cards_by_priority', 'unique_ris_by_tag', 'unique_ris_by_prio_comb', 'High to Low Ratio']]

rec_proj_high_df = rec_proj_df.copy()
rec_proj_high_df = rec_proj_high_df.loc[rec_proj_high_df['PRIORITY'] == "High"]
rec_proj_high_df.rename(columns={'cards_by_priority':'High Card Count', 'unique_ris_by_prio_comb':'High Count'}, inplace=True)

rec_proj_df = rec_proj_df.loc[rec_proj_df['PRIORITY'] == "Low"]
rec_proj_df.rename(columns={'cards_by_priority':'Low Card Count', 'unique_ris_by_prio_comb':'Low Count'}, inplace=True)

rec_proj_df = pd.merge(rec_proj_df, rec_proj_high_df, how='outer', on='Tag')

rec_proj_df.rename(columns={'cards_by_ri_type_x':'Total Cards', 'unique_ris_by_tag_x':'Total RIs', 'High to Low Ratio_y':'High to Low Ratio'}, inplace=True)

rec_proj_df = rec_proj_df[['Tag', 'Total Cards', 'High Card Count', 'Low Card Count', 'Total RIs', 'High Count', 'Low Count', 'High to Low Ratio']]

##Write data to sheet
recProjTab = gc.open_by_key('1kEPJPteHbBXVnCZD-p7h20CC35Spn8DWgni0hp9NxB4').worksheet('BacklogData')
recProjTab.batch_clear(['A1:H6'])
gd.set_with_dataframe(recProjTab, rec_proj_df, row=1, col=1)

##Yest proc data
rec_df.drop_duplicates(subset='RI_NUMBER', inplace=True)

rec_df = rec_df.loc[rec_df['PROC_TIME_MINUTES'].astype('float64') > 0]

rec_df['PROCESSING_ENDED'] = pd.to_datetime(rec_df['PROCESSING_ENDED'], format='%m-%d-%Y').dt.date

rec_df["now"] = pd.Timestamp.now()
rec_df['now'] = rec_df['now'].dt.date

rec_df = rec_df.loc[rec_df['PROCESSING_ENDED'] == (rec_df['now'] - timedelta(days = 1))]

##Sum cards processed yesterday
total_cards = rec_df.groupby('Tag')['NUMBER_OF_CARDS'].sum()
rec_df = pd.merge(rec_df, total_cards, how='right', on='Tag')
rec_df.drop('NUMBER_OF_CARDS_x', axis=1, inplace=True)
rec_df.rename(columns={'NUMBER_OF_CARDS_y':'Total Cards'}, inplace=True)
rec_df.drop_duplicates(subset='Tag', inplace=True)
rec_df = rec_df[['Tag', 'Total Cards']]

##Write data to sheet
dataTab = gc.open_by_key('1kEPJPteHbBXVnCZD-p7h20CC35Spn8DWgni0hp9NxB4').worksheet('YestProc')
dataTab.clear()
gd.set_with_dataframe(dataTab, rec_df)

#########################################
###Buylist
##Import buylist data
sql_blo = ("""
select
    buylist_offers.offer_number as buylist_offer_number
    , buylist_offers.expected_product_count as blo_product_count
    , buylist_offers.processed_by_user_email as processor
    , buylist_offers.verified_by_user_email as verifier
    , buylist_offers.processing_time_minutes as processing_time_minutes
    , buylist_offers.verification_time_minutes as verifying_time_minutes
    , buylist_offers.active_processing_started_at_et as processing_started_at
    , buylist_offers.active_verification_started_at_et as verifying_started_at
    , buylistpurchaseproduct.productconditionid as pcid
    , buylistpurchaseproduct.quantity as card_qty
    , buylistpurchaseproduct.receivedquantity as rec_card_qty
    , condition.conditionname as condition_name
    , setname.setname as set_name
    , product.marketprice as market_price
    , product.productname as card_name
    , buylist_offers.player_name as player_name

    , case
       when (buylistofferaudittrail.createdat <= ready_to_verify.timestamp) and (buylistofferaudittrail.proddiscreasonid = 1) then buylistofferaudittrail.receivedqty
       else 0
       end as proc_miss_count_entered

    , case
       when (buylistofferaudittrail.createdat <= ready_to_verify.timestamp) and (buylistofferaudittrail.proddiscreasonid = 2) then buylistofferaudittrail.receivedqty
       else 0
       end as proc_cond_count_entered

    , case
       when (buylistofferaudittrail.createdat <= ready_to_verify.timestamp) and (buylistofferaudittrail.proddiscreasonid = 3) then buylistofferaudittrail.receivedqty
       else 0
       end as proc_extra_count_entered

    , case
       when (buylistofferaudittrail.createdat > ready_to_verify.timestamp) and (buylistofferaudittrail.proddiscreasonid = 1) then buylistofferaudittrail.receivedqty
       else 0
       end as ver_miss_count_entered

    , case
       when (buylistofferaudittrail.createdat > ready_to_verify.timestamp) and (buylistofferaudittrail.proddiscreasonid = 2) then buylistofferaudittrail.receivedqty
       else 0
       end as ver_cond_count_entered

    , case
       when (buylistofferaudittrail.createdat > ready_to_verify.timestamp) and (buylistofferaudittrail.proddiscreasonid = 3) then buylistofferaudittrail.receivedqty
       else 0
       end as ver_extra_count_entered

from analytics.core.buylist_offers
inner join hvr_tcgstore_production.adt.buylistofferaudittrail on buylist_offers.id = buylistofferaudittrail.buylistofferid
inner join hvr_tcgstore_production.byl.buylistpurchase on buylist_offers.id = buylistpurchase.buylistofferid
inner join hvr_tcgstore_production.byl.buylistpurchaseproduct on buylistpurchase.buylistpurchaseid = buylistpurchaseproduct.buylistpurchaseid
inner join hvr_tcgstore_production.pdt.productcondition on buylistpurchaseproduct.productconditionid = productcondition.productconditionid
inner join hvr_tcgstore_production.pdt.condition on productcondition.conditionid = condition.conditionid
inner join hvr_tcgstore_production.pdt.product on productcondition.productid = product.productid
inner join hvr_tcgstore_production.pdt.setname on product.setnameid = setname.setnameid
left outer join
(select
    min(buylistofferaudittrail.createdat) as timestamp
    , buylistofferaudittrail.buylistofferid as id

from
hvr_tcgstore_production.adt.buylistofferaudittrail

where
    buylistofferaudittrail.buylistofferstatusid = '5'

group by
    buylistofferaudittrail.buylistofferid
) as ready_to_verify
on ready_to_verify.id = buylist_offers.id

where
    buylist_offers.active_processing_ended_at_et >= dateadd(dd, -120, getdate())

""")

cursor = snowflake_pull.cursor()
cursor.execute(sql_blo)

blo_df = cursor.fetch_pandas_all()
blo_df.drop(blo_df.filter(like='Unnamed'), axis=1, inplace=True)
blo_df.dropna(subset =['BUYLIST_OFFER_NUMBER'], inplace=True)

##Connect to cabinet data ###CHANGE AFTER CABINET DATABASE IS MADE
cabs = gc.open_by_key('16fcE7gptCnG4vpbasgwHYKHPe-GuidRjGgXQurCUzn4').worksheet('Reference')
cabsData = pd.DataFrame(cabs.get_all_values())
cabsData.columns = cabsData.iloc[0]
cabsData = cabsData[1:]
cabs_df = pd.DataFrame(cabsData)
cabs_df.dropna(subset=['Set_Name'], inplace=True)
cabs_df.drop(cabs_df.filter(like='Unnamed'), axis=1, inplace=True)
cabs_df = cabs_df[['Set_Name', 'Condition', 'Cabinet', 'Game_Name']]

##Connect BLO contents to cabinet data
blo_df["combined"] = blo_df['SET_NAME'].astype(str) + blo_df['CONDITION_NAME'].astype(str)
cabs_df["combined"] = cabs_df['Set_Name'].astype(str) + cabs_df['Condition'].astype(str)

blo_df = pd.merge(blo_df, cabs_df, how='left', on='combined')

##Remove duplicates
blo_df["dupe"] = blo_df['BUYLIST_OFFER_NUMBER'].astype(str) + blo_df['PCID'].astype(str)
blo_df.drop_duplicates(subset=['dupe'], inplace=True)
blo_df.drop('dupe', axis=1, inplace=True)
blo_df['REC_CARD_QTY'] = blo_df['REC_CARD_QTY'].fillna(0)

##Sleeving
blo_df["sleeved"] = None

mtg_set_list = ['Alpha Edition', 'Beta Edition', 'Unlimited Edition', 'Legends', 'Antiquities', 'Arabian Nights', 'Collector\'s Edition', 'International Edition']

pkm_set_list = ['Skyridge', 'Aquapolis', 'Expedition', 'Neo Destiny', 'Neo Revelation', 'Neo Discovery', 'Neo Genesis', 'Gym Challenge', 'Gym Heroes', 'Team Rocket', 'Base Set 2', 'Fossil', 'Jungle', 'Base Set Shadowless', 'Base Set']


blo_df.loc[(blo_df['MARKET_PRICE'] >= 25) | (blo_df['SET_NAME'].isin(mtg_set_list)), 'sleeved'] = blo_df['REC_CARD_QTY'] ##always sleeve cards over $25 and mtg legacy sets

blo_df.loc[(blo_df['CONDITION_NAME'] == 'Near Mint Holofoil') & (blo_df['SET_NAME'].isin(pkm_set_list)), 'sleeved'] = blo_df['REC_CARD_QTY'] #always sleeve pkm legacy sets NMHF

blo_df.loc[(blo_df['CONDITION_NAME'].str[-8:] == 'Holofoil') & (blo_df['SET_NAME'].isin(pkm_set_list)), 'sleeved'] = blo_df['REC_CARD_QTY'] #sleeve pkm foils before full stop

##Aggragate sleeved cards per BLO
blo_df['sleeved'] = blo_df['sleeved'].astype('float64')

sleeved_cards_per_blo = blo_df.groupby('BUYLIST_OFFER_NUMBER')['sleeved'].sum()
blo_df = pd.merge(blo_df, sleeved_cards_per_blo, how='right', on='BUYLIST_OFFER_NUMBER')

cabinet_splits = blo_df.groupby('BUYLIST_OFFER_NUMBER')['Cabinet'].nunique()
blo_df = pd.merge(blo_df, cabinet_splits, how='right', on='BUYLIST_OFFER_NUMBER')

blo_df.rename(columns={'sleeved_x':'sleeved', 'sleeved_y':'sleeved_cards', 'Cabinet_x':'Cabinet', 'Cabinet_y':'cabinet_splits'}, inplace=True)

blo_prod_df = blo_df.copy()

blo_df.drop_duplicates(subset=['BUYLIST_OFFER_NUMBER'], inplace=True)

blo_tag_df = blo_df.copy()

##Speed Calculations
blo_df = blo_df.loc[blo_df['PROCESSING_TIME_MINUTES'].astype('float64') > 0]

blo_df["proc_cph"] = blo_df['BLO_PRODUCT_COUNT'].astype('float64') / (blo_df['PROCESSING_TIME_MINUTES'].astype('float64')/60)

blo_df["proc_spc"] = 3600 / blo_df['proc_cph'].astype('float64')

blo_df["ver_cph"] = None
blo_df["ver_spc"] = None

blo_df.loc[blo_df['VERIFYING_TIME_MINUTES'].astype('float64') > 0, 'ver_cph'] = blo_df['BLO_PRODUCT_COUNT'].astype('float64') / (blo_df['VERIFYING_TIME_MINUTES'].astype('float64')/60)

blo_df.loc[blo_df['VERIFYING_TIME_MINUTES'].astype('float64') > 0, 'ver_spc'] = 3600 / blo_df['ver_cph'].astype('float64')

##Create Final Dataframes
blo_df = blo_df[['BUYLIST_OFFER_NUMBER', 'BLO_PRODUCT_COUNT', 'PROCESSOR', 'VERIFIER', 'PROCESSING_TIME_MINUTES', 'VERIFYING_TIME_MINUTES', 'PROC_MISS_COUNT_ENTERED', 'PROC_COND_COUNT_ENTERED', 'PROC_EXTRA_COUNT_ENTERED', 'VER_MISS_COUNT_ENTERED', 'VER_COND_COUNT_ENTERED', 'VER_EXTRA_COUNT_ENTERED', 'proc_cph', 'proc_spc', 'ver_cph', 'ver_spc', 'PROCESSING_STARTED_AT', 'VERIFYING_STARTED_AT', 'sleeved_cards', 'cabinet_splits']]

blo_prod_df = blo_prod_df[['BUYLIST_OFFER_NUMBER', 'PCID', 'CONDITION_NAME', 'SET_NAME']]

##Create BLO csvs
blo_string = ["C:", "Users", login, "OneDrive - eBay Inc", "AC-Scripting", "Data CSVs", "Snowflake", "BLO.csv"]
separator = '\\'
blo_result = separator.join(blo_string)
blo_df.to_csv(blo_result, index=False)

blo_prod_string = ["C:", "Users", login, "OneDrive - eBay Inc", "AC-Scripting", "Data CSVs", "Snowflake", "BLOProd.csv"]
blo_prod_result = separator.join(blo_prod_string)
blo_prod_df.to_csv(blo_prod_result, index=False)

##Update audit log
csv_string = ["C:", "Users", login, "OneDrive - eBay Inc", "AC-Scripting", "Audit CSVs", "AuditLog.csv"]
result = separator.join(csv_string)
audit_df = pd.read_csv(result)

executionTime = (time.time() - startTime)
dt_string = datetime.now(pytz.timezone('America/New_York')).strftime("%m/%d/%Y %H:%M:%S")
script_name = os.path.basename(__file__).replace('.py', '')

new_audit = {'Timestamp': dt_string, 'Execution Time': executionTime, 'Script': script_name}

new_audit_df = pd.DataFrame(data=new_audit, index=[0])

audit_df = pd.concat([audit_df, new_audit_df])
audit_df.dropna(subset=["Timestamp"], inplace=True)

audit_df.to_csv(result, index=False)