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

##PVP SQ Data
sq_sql = ("""
select
    distinct category.categoryname as game_name
    , directorder.directordernumber as order_number
    , shippingqueue.shippingqueuenumber as shipping_queue_number

from
hvr_tcgstore_production.tcgd.directorder
inner join hvr_tcgstore_production.tcgd.shippingqueue on directorder.shippingqueueid = shippingqueue.shippingqueueid
inner join hvr_tcgstore_production.dbo.sellerorder on directorder.orderid = sellerorder.orderid
inner join hvr_tcgstore_production.dbo.sellerorderproduct on sellerorder.sellerorderid = sellerorderproduct.sellerorderid
inner join hvr_tcgstore_production.pdt.productcondition on sellerorderproduct.storeproductconditionid = productcondition.productconditionid
inner join hvr_tcgstore_production.pdt.product on productcondition.productid = product.productid
inner join hvr_tcgstore_production.pdt.category on product.categoryid = category.categoryid

where
    directorder.createdat::date >= cast(dateadd(dd, -150, getdate()) as date)
    and ((game_name = 'Magic') or (game_name = 'Pokemon') or (game_name = 'YuGiOh'))

order by
    order_number
    , shipping_queue_number
    , game_name

""")

cursor = snowflake_pull.cursor()
cursor.execute(sq_sql)

sq_df = cursor.fetch_pandas_all()

sq_df.drop(sq_df.filter(like='Unnamed'), axis=1, inplace=True)
sq_df.dropna(subset=['ORDER_NUMBER'], inplace=True)

sq_df['SHIPPING_QUEUE_NUMBER'] = sq_df['SHIPPING_QUEUE_NUMBER'].str.lower()

sq_df.rename(columns={'SHIPPING_QUEUE_NUMBER':'Subtask'}, inplace=True)

##Create csv
sq_string = ["C:", "Users", login, "OneDrive - eBay Inc", "AC-Scripting", "Data CSVs", "Snowflake", "SQGameData.csv"]
sq_result = separator.join(sq_string)
sq_df.to_csv(sq_result, index=False)

##Tix data
sql_tix = (r"""
select
  zendesk.tickets.id as tix_id
  , left(zendesk.tickets.order_number,11) as ticket_order_number_1
  , right(zendesk.tickets.order_number,11) as ticket_order_number_2
  , regexp_substr(ticket_comments.body,'\\d{6}-\\w{4}') as comment_body
  , regexp_substr(zendesk.tickets.subject,'\\d{6}-\\w{4}') as title
  , case when contains(zendesk.tickets.tags,'warehousefeedback_cardname') or contains(zendesk.tickets.tags,'warehousefeedback_card') then 'card' end as card_tag
  , case when contains(zendesk.tickets.tags,'warehousefeedback_quantity') then 'qty' end as qty_tag
  , case when contains(zendesk.tickets.tags,'warehousefeedback_package') or contains(zendesk.tickets.tags,'wrongpackage') then 'pkg' end as pkg_tag
  , case when contains(zendesk.tickets.tags, 'counterfeit') then 'ctf' end as ctf_tag
  , case when contains(zendesk.tickets.tags, 'condition') then 'cnd' end as cnd_tag

from
segment.zendesk.tickets
inner join segment.zendesk.ticket_comments on zendesk.tickets.id = ticket_comments.ticket_id

where zendesk.tickets.received_at::date >= cast(dateadd(dd, -150, getdate()) as date)

group by
    zendesk.tickets.id
    , zendesk.tickets.order_number
    , ticket_comments.body
    , zendesk.tickets.subject
    , zendesk.tickets.tags

""")

cursor = snowflake_pull.cursor()
cursor.execute(sql_tix)

tix_data_df = cursor.fetch_pandas_all()

tix_data_df.drop(tix_data_df.filter(like='Unnamed'), axis=1, inplace=True)
tix_data_df.dropna(subset=['TIX_ID'], inplace=True)

tix_data_df[tix_data_df.isna()] = 0

##Create csv
tix_data_string = ["C:", "Users", login, "OneDrive - eBay Inc", "AC-Scripting", "Data CSVs", "Snowflake", "TixData.csv"]
tix_data_result = separator.join(tix_data_string)
tix_data_df.to_csv(tix_data_result, index=False)

##AQL RI Data
sql_ri = ("""
select
    reimbursement_invoices.reimbursement_invoice_number as ri_number
    , reimbursement_invoices.seller_name as seller
    , condition.conditionname as condition_name
    , category.categoryname as game_name
    , product.rarity as card_rarity
    , product.productname as card_name
    , product.number as card_number
    , setname.setname as set_name
    , sellerorderproduct.price as market_price
    , sum(reimordersellerorderproduct.quantity) as card_quantity
    , setname.shelforder as shelf_order

from
analytics.core.reimbursement_invoices
inner join hvr_tcgstore_production.tcgd.reimordersellerorderproduct on reimbursement_invoices.id = reimordersellerorderproduct.reimorderid
inner join hvr_tcgstore_production.adt.reimorderaudittrail on reimbursement_invoices.id = reimorderaudittrail.reimorderid
inner join hvr_tcgstore_production.dbo.sellerorderproduct on reimordersellerorderproduct.sellerorderproductid = sellerorderproduct.sellerorderproductid
inner join hvr_tcgstore_production.pdt.productcondition on sellerorderproduct.storeproductconditionid = productcondition.productconditionid
inner join hvr_tcgstore_production.pdt.product on productcondition.productid = product.productid
inner join hvr_tcgstore_production.pdt.setname on product.setnameid = setname.setnameid
inner join hvr_tcgstore_production.pdt.condition on productcondition.conditionid = condition.conditionid
inner join hvr_tcgstore_production.pdt.category on product.categoryid = category.categoryid

where
    reimbursement_invoices.processing_ended_at_et >= cast(dateadd(dd, -7, getdate()) as date)
    and reimbursement_invoices.is_auto = false
    and reimorderaudittrail.reimorderstatusid = '6'

group by
    ri_number
    , seller
    , condition_name
    , game_name
    , card_rarity
    , card_name
    , card_number
    , set_name
    , market_price
    , shelf_order

""")

cursor = snowflake_pull.cursor()
cursor.execute(sql_ri)
ri_df = cursor.fetch_pandas_all()
ri_df.drop(ri_df.filter(like='Unnamed'), axis=1, inplace=True)

##Create csv
ri_string = ["C:", "Users", login, "OneDrive - eBay Inc", "AC-Scripting", "Data CSVs", "Snowflake", "AQLRIData.csv"]
separator = '\\'
ri_result = separator.join(ri_string)
ri_df.to_csv(ri_result, index=False)

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