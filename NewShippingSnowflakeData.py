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

###Shipping
##Import SQ Slot Data
sql_slot = ("""
select
    case
        when len(shippingqueue.shippingqueuenumber) in (11, 14) then left(shippingqueue.shippingqueuenumber, 8)
        when len(shippingqueue.shippingqueuenumber) in (12, 15) then left(shippingqueue.shippingqueuenumber, 9)
        when len(shippingqueue.shippingqueuenumber) in (13, 16) then left(shippingqueue.shippingqueuenumber, 10)
        else shippingqueue.shippingqueuenumber
            end as queue_number

    , shippingqueue.shippingqueuenumber as shippingqueuenumber
    , shippingqueue.ordercount as order_count
    , shippingqueuepullsheet.slot as slot
    , shippingqueuepullsheet.quantity as card_quantity
    , category.categoryname as game_name
    , shippingqueuepullsheet.productconditionid as pcid
    , shippingqueue.createdat as created_at

from
hvr_tcgstore_production.tcgd.shippingqueue
inner join hvr_tcgstore_production.tcgd.shippingqueuepullsheet on shippingqueue.shippingqueueid = shippingqueuepullsheet.shippingqueueid
inner join hvr_tcgstore_production.pdt.productcondition on shippingqueuepullsheet.productconditionid = productcondition.productconditionid
inner join hvr_tcgstore_production.pdt.product on productcondition.productid = product.productid
inner join hvr_tcgstore_production.pdt.category on product.categoryid = category.categoryid

where
    shippingqueue.createdat::date >= cast(dateadd(dd, -120, getdate()) as date)

group by
    pcid
    , shippingqueuenumber
    , slot
    , order_count
    , game_name
    , card_quantity
    , created_at

union all

select
    concat(buylistpurchaseorderqueue.buylistpurchaseorderqueuenumber, 'POQ')
    , buylistpurchaseorderqueue.buylistpurchaseorderqueuenumber as shippingqueuenumber
    , buylistpurchaseorderqueue.ordercount as order_count
    , buylistpurchaseorderqueuepullsheet.slot as slot
    , buylistpurchaseorderqueuepullsheet.quantity as card_quantity
    , category.categoryname as game_name
    , buylistpurchaseorderqueuepullsheet.productconditionid as pcid
    , buylistpurchaseorderqueue.createdat as created_at

from
hvr_tcgstore_production.byl.buylistpurchaseorderqueue
inner join hvr_tcgstore_production.byl.buylistpurchaseorderqueuepullsheet on buylistpurchaseorderqueue.buylistpurchaseorderqueueid = buylistpurchaseorderqueuepullsheet.buylistpurchaseorderqueueid
inner join hvr_tcgstore_production.pdt.productcondition on buylistpurchaseorderqueuepullsheet.productconditionid = productcondition.productconditionid
inner join hvr_tcgstore_production.pdt.product on productcondition.productid = product.productid
inner join hvr_tcgstore_production.pdt.category on product.categoryid = category.categoryid

where
    buylistpurchaseorderqueue.createdat::date >= cast(dateadd(dd, -120, getdate()) as date)

group by
    pcid
    , shippingqueuenumber
    , slot
    , order_count
    , game_name
    , card_quantity
    , created_at

""")

cursor = snowflake_pull.cursor()
cursor.execute(sql_slot)

pvp_df = cursor.fetch_pandas_all()
pvp_df.drop(pvp_df.filter(like='Unnamed'), axis=1, inplace=True)
pvp_df.dropna(subset = ['QUEUE_NUMBER'], inplace=True)

##Parse slots
pvp_df['SLOT'] = pvp_df['SLOT'].astype(str)

pvp_df = pvp_df.loc[pvp_df['SHIPPINGQUEUENUMBER'].str[-2:] != 'HQ']

pvp_df["slot_parse"] = pvp_df['SLOT'].map(ord)

pvp_df['slot_parse'] = pvp_df['slot_parse'] - 64

pvp_df['slot_parse'] = pvp_df['slot_parse'].astype(str)

pvp_df["check"] = pvp_df['slot_parse'].str[:1]

pvp_df.loc[pvp_df['slot_parse'].str[:1] == "-", 'slot_parse'] = pvp_df['SLOT']

pvp_df['SLOT'] = pvp_df['slot_parse']

pvp_df['slot_parse'] = pvp_df['slot_parse'].astype('int64')

pvp_df.drop('check', axis=1, inplace=True)

##Create dataframe for pvp data and aggragate SQ card quantity
cards_by_sq = pvp_df.groupby('SHIPPINGQUEUENUMBER')['CARD_QUANTITY'].sum()
pvp_df = pd.merge(pvp_df, cards_by_sq, how='right', on='SHIPPINGQUEUENUMBER')
pvp_df.rename(columns={'CARD_QUANTITY_x':'CARD_QUANTITY', 'CARD_QUANTITY_y':'sq_card_quantity'}, inplace=True)

##Create sqacc csv
sqacc_string = ["C:", "Users", login, "OneDrive - eBay Inc", "AC-Scripting", "Data CSVs", "Snowflake", "sqacc.csv"]
sqacc_result = separator.join(sqacc_string)
pvp_df.to_csv(sqacc_result, index=False)

##Aggragate unique pcids and card quantity by slot
pvp_df["combined"] = pvp_df['SHIPPINGQUEUENUMBER'].astype(str) + pvp_df['SLOT'].astype(str)

unique_pcids_by_slot = pvp_df.groupby('combined')['PCID'].nunique()
pvp_df = pd.merge(pvp_df, unique_pcids_by_slot, how='right', on='combined')

card_qty_by_slot = pvp_df.groupby('combined')['CARD_QUANTITY'].sum()
pvp_df = pd.merge(pvp_df, card_qty_by_slot, how='right', on='combined')

pvp_df.drop_duplicates(subset=['combined'], inplace=True)

pvp_df.drop(['PCID_x', 'CARD_QUANTITY_x'], axis=1, inplace=True)
pvp_df.rename(columns={'PCID_y':'unique_pcids_by_slot', 'CARD_QUANTITY_y':'card_qty_by_slot'}, inplace=True)

##Create final dataframe
pvp_df = pvp_df[['QUEUE_NUMBER', 'SHIPPINGQUEUENUMBER', 'SLOT', 'unique_pcids_by_slot', 'card_qty_by_slot', 'ORDER_COUNT', 'sq_card_quantity', 'CREATED_AT']]

##Create sqslot.csv
pull_string = ["C:", "Users", login, "OneDrive - eBay Inc", "AC-Scripting", "Data CSVs", "Snowflake", "sqslot.csv"]
pull_result = separator.join(pull_string)
pvp_df.to_csv(pull_result, index=False)

##Parse down to single SQs in pvp dataframe
pvp_df.drop_duplicates(subset=['SHIPPINGQUEUENUMBER'], inplace=True)

pvp_df = pvp_df[['QUEUE_NUMBER', 'SHIPPINGQUEUENUMBER', 'ORDER_COUNT', 'sq_card_quantity', 'CREATED_AT']]

##Create PVP.csv
pvp_string = ["C:", "Users", login, "OneDrive - eBay Inc", "AC-Scripting", "Data CSVs", "Snowflake", "PVP.csv"]
pvp_result = separator.join(pvp_string)
pvp_df.to_csv(pvp_result, index=False)

###Paperless Pulling
##Import Paperless Data
sql_paperless = ("""
select
    analytics.core.paperless_pulling_agg.puller_email as puncher
    , analytics.core.paperless_pulling_agg.shipping_queue_number as sq
    , analytics.core.paperless_pulling_agg.pulling_start as pulling_start
    , analytics.core.paperless_pulling_agg.pulling_end as punch
    , analytics.core.paperless_pulling_agg.total_number_of_cards_pulled as cards_pulled
    , analytics.core.paperless_pulling_agg.density_pulled as density_pulled
    , analytics.core.paperless_pulling_agg.total_time_spent_pulling_seconds as pulling_time_seconds
    , analytics.core.paperless_pulling_agg.total_time_paused_seconds as paused_time_seconds

from
analytics.core.paperless_pulling_agg

where
    punch::date >= cast(dateadd(dd, -120, getdate()) as date)

""")

cursor = snowflake_pull.cursor()
cursor.execute(sql_paperless)

paperless_df = cursor.fetch_pandas_all()
paperless_df.drop(paperless_df.filter(like='Unnamed'), axis=1, inplace=True)
paperless_df.dropna(subset = ['PUNCHER'], inplace=True)

##Fix data types
paperless_df['DENSITY_PULLED'] = paperless_df['DENSITY_PULLED'].astype('float64')

##Convert time to hours
paperless_df['PULLING_TIME_SECONDS'] = paperless_df['PULLING_TIME_SECONDS'].astype('float64')
paperless_df["pulling_time_hours"] = paperless_df['PULLING_TIME_SECONDS']/3600

##SQ Type
paperless_df["sq_type"] = paperless_df['SQ'].str[-3:]
paperless_df.loc[(paperless_df['SQ'].map(len) == 16) & (paperless_df['SQ'].str[-3:] != 'poq'), 'sq_type'] = paperless_df['SQ'].str[-6:]

##Remove patched SQs
paperless_df.loc[(paperless_df['DENSITY_PULLED'] == 1) & (paperless_df['sq_type'] != 'sccSub') & (paperless_df['sq_type'] != 'sco'), 'Puncher'] = None

##Create final dataframe
paperless_df = paperless_df[['PUNCHER', 'SQ', 'PUNCH', 'CARDS_PULLED', 'DENSITY_PULLED', 'PAUSED_TIME_SECONDS', 'pulling_time_hours', 'sq_type', 'PULLING_START']]

##Write data to sheet
ppDataTab = gc.open_by_key('1L3que0F_p53yEOrjUIHQ9e33CvV1szUAMKPnlcCWBmc').worksheet('Data')
ppDataTab.clear()
gd.set_with_dataframe(ppDataTab, paperless_df)

##Create paperless.csv
paperless_string = ["C:", "Users", login, "OneDrive - eBay Inc", "AC-Scripting", "Data CSVs", "Snowflake", "Paperless.csv"]
paperless_result = separator.join(paperless_string)
paperless_df.to_csv(paperless_result, index=False)

##Mail check in
sql_mail = ("""
select
    reimbursement_invoices.reimbursement_invoice_number as package_number
    , reimbursement_invoices.total_product_quantity as card_count
    , users.email_address as user_email
    , convert_timezone('UTC', 'America/New_York', reimorderaudit.createdat) as check_in_time

    , case
        when reimorderauditchange.originalvalue = '2' and reimorderauditchange.newvalue = '3' then 'RI - Received'
        else null
        end as event_type

from
analytics.core.reimbursement_invoices
inner join hvr_tcgstore_production.adt.reimorderaudit on reimbursement_invoices.id = reimorderaudit.reimorderid
inner join hvr_tcgstore_production.adt.reimorderauditchange on reimorderaudit.reimorderauditid = reimorderauditchange.reimorderauditid
inner join analytics.core.users on reimorderaudit.createdbyuserid = users.id

where
    cast(check_in_time as date) >= dateadd(dd, -31, getdate())
    and reimbursement_invoices.is_auto = false
    and reimorderauditchange.originalvalue is not null
    and event_type is not null

union all

select
    buylistoffer.offernumber as package_number
    , buylistoffer.productcount as card_count
    , users.email_address as user_email
    , convert_timezone('UTC', 'America/New_York', buylistofferaudit.createdat) as check_in_time
    , case when buylistofferauditchange.newvalue = '3' then 'BLO - Received' else null end as event_type

from
hvr_tcgstore_production.byl.buylistoffer
inner join hvr_tcgstore_production.adt.buylistofferaudit on buylistoffer.buylistofferid = buylistofferaudit.buylistofferid
inner join hvr_tcgstore_production.adt.buylistofferauditchange on buylistofferaudit.buylistofferauditid = buylistofferauditchange.buylistofferauditid
inner join analytics.core.users on buylistofferaudit.createdbyuserid = users.id

where
    buylistofferauditchange.newvalue = '3'
    and cast(check_in_time as date) >= dateadd(dd, -31, getdate())
    and buylistofferauditchange.originalvalue is not null
    and event_type is not null

""")

cursor = snowflake_pull.cursor()
cursor.execute(sql_mail)

mail_df = cursor.fetch_pandas_all()

mail_df = mail_df[['PACKAGE_NUMBER', 'CHECK_IN_TIME', 'EVENT_TYPE', 'USER_EMAIL', 'CARD_COUNT']]

##Write data to sheet
mailDataTab = gc.open_by_key('1NJ0ydMI3CTKcYjzXwG-rwdcoVtaXfkDoOFA1x83X25I').worksheet('Data')
mailDataTab.clear()
gd.set_with_dataframe(mailDataTab, mail_df)

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

##Test