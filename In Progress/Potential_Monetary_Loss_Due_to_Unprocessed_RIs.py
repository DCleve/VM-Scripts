import gspread
import pandas as pd
import gspread_dataframe as gd
from datetime import datetime, timedelta
import datetime as dt
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

##Import delayed processing RIs
sql_delayed_ris = ("""
select
  reimbursement_invoice_products.product_condition_id as "pcid"
  , reimbursement_invoice_products.expected_quantity as "expected_quantity"
  , reimbursement_invoices.received_at_et::date as "receipt_date"
  , reimbursement_invoices.processing_ended_at_et::date as "processing_date"
  , reimbursement_invoices.reimbursement_invoice_number as "ri_number"

from analytics.core.reimbursement_invoices
    inner join analytics.core.reimbursement_invoice_products on reimbursement_invoices.reimbursement_invoice_number = reimbursement_invoice_products.reimbursement_invoice_number

where
  reimbursement_invoices.received_at_et::date >= cast(dateadd(dd, -90, getdate()) as date)
  and reimbursement_invoices.was_marked_missing = False
  and datediff(dd, reimbursement_invoices.received_at_et, reimbursement_invoices.processing_ended_at_et) > 1
  and datediff(dd, reimbursement_invoices.received_at_et, reimbursement_invoices.processing_ended_at_et) < 30
  and "receipt_date" is not null
  and "processing_date" is not null

order by 1, 3, 4
""")

cursor = snowflake_pull.cursor()
cursor.execute(sql_delayed_ris)

delayed_ris_df = cursor.fetch_pandas_all()
delayed_ris_df.drop(delayed_ris_df.filter(like='Unnamed'), axis=1, inplace=True)
delayed_ris_df.dropna(subset = ['pcid'], inplace=True)

##Import non-direct order information
order_sql = ("""
select
  order_items.product_condition_id as "pcid"
  , order_items.unit_price_usd as "sold_price"
  , order_items.quantity as "sold_qty"
  , order_items.ordered_at_et::date as "ordered_at_date"
  , order_items.ordered_at_et as "ordered_at_timestamp"
  , seller_orders.order_number as "seller_order_number"

from analytics.core.order_items
  inner join analytics.core.seller_orders on analytics.core.seller_orders.id = analytics.core.order_items.seller_order_id
  left outer join analytics.core.sellers on seller_orders.seller_id = sellers.id

where
  order_items.ordered_at_et::date >= cast(dateadd(dd, -90, getdate()) as date)
  and seller_orders.is_direct_order = False

order by
    "ordered_at_timestamp" asc
""")

cursor = snowflake_pull.cursor()
cursor.execute(order_sql)

order_df = cursor.fetch_pandas_all()
order_df.drop(order_df.filter(like='Unnamed'), axis=1, inplace=True)
order_df.dropna(subset = ['pcid'], inplace=True)

sales_per_day_df = order_df.copy()

marketplace_value_per_day = sales_per_day_df.groupby('ordered_at_date')['sold_price'].sum()
sales_per_day_df = pd.merge(sales_per_day_df, marketplace_value_per_day, how='right', on='ordered_at_date')
sales_per_day_df.rename(columns={'sold_price_x':'sold_price', 'sold_price_y':'total_sold_price_per_day'}, inplace=True)

marketplace_qty_per_day = sales_per_day_df.groupby('ordered_at_date')['sold_qty'].sum()
sales_per_day_df = pd.merge(sales_per_day_df, marketplace_qty_per_day, how='right', on='ordered_at_date')
sales_per_day_df.rename(columns={'sold_qty_x':'sold_qty', 'sold_qty_y':'total_sold_qty_per_day'}, inplace=True)

sales_per_day_df.drop_duplicates(subset='ordered_at_date', inplace=True)

##Find orders for pcids that happened during processing delay
delayed_ris_df = pd.merge(delayed_ris_df, order_df, how='left', on='pcid')

delayed_ris_df = delayed_ris_df.loc[(delayed_ris_df['receipt_date'] < delayed_ris_df['ordered_at_date']) & (delayed_ris_df['processing_date'] > delayed_ris_df['ordered_at_date'])]

delayed_ris_df["pcid_identifier"] = delayed_ris_df['pcid'].astype(str) + '~' + delayed_ris_df['ri_number'].astype(str)

delayed_ris_df["combined"] = delayed_ris_df['pcid'].astype(str) + '~' + delayed_ris_df['ordered_at_date'].astype(str)

delayed_ris_df.rename(columns={'sold_qty':'sold_qty_per_seller_order'}, inplace=True)

##Loop frame contents until all sales are accounted for
def process_data(min_length = 0):
    dataframe = delayed_ris_df
    orders_df = pd.DataFrame()

    ##Establish variables
    dataframe["keep_order"] = 0.0
    dataframe["missed_sales_qty"] = 0.0
    dataframe["remaining_to_fill"] = 0.0
    dataframe["remaining_to_sell"] = 0.0
    dataframe['remaining_to_sell'] = dataframe['expected_quantity']

    while len(dataframe) > min_length:
        print("Orders to Evaluate Frame Length: " + str(len(dataframe)))
        print("Missed Orders Frame Length: " +  str(len(orders_df)))
        print(len(dataframe) + len(orders_df))

        ##Find min order timestamp per pcid/order date
        min_order_timestamp = dataframe.groupby('pcid_identifier')['ordered_at_timestamp'].min()
        dataframe = pd.merge(dataframe, min_order_timestamp, how='right', on='pcid_identifier')
        dataframe.rename(columns={'ordered_at_timestamp_x':'ordered_at_timestamp', 'ordered_at_timestamp_y':'min_ordered_at_timestamp'}, inplace=True)

        dataframe.loc[dataframe['ordered_at_timestamp'].astype(str) == dataframe['min_ordered_at_timestamp'].astype(str), 'keep_order'] = 1

        dataframe['remaining_to_sell'] = dataframe['remaining_to_sell'].astype('float64') - dataframe['sold_qty_per_seller_order'].astype('float64')

        dataframe.loc[dataframe['remaining_to_sell'] < 0, 'missed_sales_qty'] = dataframe['expected_quantity']
        dataframe.loc[dataframe['remaining_to_sell'] >= 0, 'missed_sales_qty'] = dataframe['sold_qty_per_seller_order']

        dataframe.loc[dataframe['remaining_to_sell'] < 0, 'remaining_to_sell'] = 0.0

        dataframe = dataframe.loc[dataframe['remaining_to_sell'] > 0]

        ##Split orders
        orders_to_fulfill = dataframe.copy()
        orders_to_fulfill = orders_to_fulfill.loc[orders_to_fulfill['keep_order'] == 1]

        orders_to_evaluate = dataframe.copy()
        orders_to_evaluate = orders_to_evaluate.loc[orders_to_evaluate['keep_order'] == 0]

        ##Fulfill orders
        orders_to_fulfill['remaining_to_fill'] = orders_to_fulfill['sold_qty_per_seller_order'].astype('float64') - orders_to_fulfill['missed_sales_qty'].astype('float64')

        orders_df = pd.concat([orders_df, orders_to_fulfill])

        dataframe = orders_to_evaluate

        dataframe.drop('min_ordered_at_timestamp', axis=1, inplace=True)
        orders_df.drop('min_ordered_at_timestamp', axis=1, inplace=True)

    return orders_df, dataframe

orders_df, dataframe = process_data()

##Aggragate values per day
orders_df["monetary_loss"] = orders_df['missed_sales_qty'].astype('float64') * orders_df['sold_price'].astype('float64')

missed_value_per_day = orders_df.groupby('ordered_at_date')['monetary_loss'].sum()
orders_df = pd.merge(orders_df, missed_value_per_day, how='right', on='ordered_at_date')
orders_df.rename(columns={'monetary_loss_x':'monetary_loss', 'monetary_loss_y':'monetary_loss_per_day'}, inplace=True)

missed_sales_per_day = orders_df.groupby('ordered_at_date')['missed_sales_qty'].sum()
orders_df = pd.merge(orders_df, missed_sales_per_day, how='right', on='ordered_at_date')
orders_df.rename(columns={'missed_sales_qty_x':'missed_sales_qty', 'missed_sales_qty_y':'missed_sales_per_day'}, inplace=True)

orders_df.drop_duplicates(subset='ordered_at_date', inplace=True)
orders_df = orders_df[['ordered_at_date', 'monetary_loss_per_day', 'missed_sales_per_day']]

##Combine frames
orders_df = pd.merge(orders_df, sales_per_day_df, how='left', on='ordered_at_date')

orders_df["monetary_loss_%"] = orders_df['monetary_loss_per_day'].astype('float64') / orders_df['total_sold_price_per_day'].astype('float64')

orders_df["missed_sales_%"] = orders_df['missed_sales_per_day'].astype('float64') / orders_df['total_sold_qty_per_day'].astype('float64')

orders_df = orders_df[['ordered_at_date', 'monetary_loss_per_day', 'missed_sales_per_day', 'monetary_loss_%', 'missed_sales_%']]



















##Write data to sheet
dataTab = gc.open_by_key('1_w6u8R5TACFRcsLLbCHOyOTNwSSmL08eyqU00SDwCrw').worksheet('Data')
dataTab.batch_clear(['A2:E'])
gd.set_with_dataframe(dataTab, orders_df, row=2, col=1)