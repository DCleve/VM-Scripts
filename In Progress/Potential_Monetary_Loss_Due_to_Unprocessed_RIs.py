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


test = gc.open_by_key('1fSUztuAQ_mizi8cSHQCIRUuNwbttWBdHWDItXLtosR4')




##Import delayed processing RIs
sql_ris = ("""
select
  reimbursement_invoice_products.product_condition_id as "pcid"
  , reimbursement_invoice_products.expected_quantity as "expected_quantity"
  , reimbursement_invoices.received_at_et::date as "receipt_date"
  //, reimbursement_invoices.processing_ended_at_et::date as "processing_date"
  , reimbursement_invoices.shelved_at_et as "shelving_timestamp"
  , reimbursement_invoices.reimbursement_invoice_number as "ri_number"

from analytics.core.reimbursement_invoices
    inner join analytics.core.reimbursement_invoice_products on reimbursement_invoices.reimbursement_invoice_number = reimbursement_invoice_products.reimbursement_invoice_number

where
  //reimbursement_invoices.received_at_et::date >= cast(dateadd(dd, -5, getdate()) as date)
  reimbursement_invoices.received_at_et::date >='2024-12-29'
  and reimbursement_invoices.was_marked_missing = False
  and datediff(dd, reimbursement_invoices.received_at_et, reimbursement_invoices.shelved_at_et) > 1
  and datediff(dd, reimbursement_invoices.received_at_et, reimbursement_invoices.shelved_at_et) < 30
  and "receipt_date" is not null
  and "shelving_timestamp" is not null
  //and "processing_date" is not null

order by 1, 3, 4
""")

cursor = snowflake_pull.cursor()
cursor.execute(sql_ris)

ris_df = cursor.fetch_pandas_all()
ris_df.drop(ris_df.filter(like='Unnamed'), axis=1, inplace=True)
ris_df.dropna(subset = ['pcid'], inplace=True)

##Import non-direct order information
order_sql = ("""
select
  order_items.product_condition_id as "pcid"
  , order_items.unit_price_usd as "sold_price"
  , order_items.quantity as "sold_qty_per_seller_order"
  , order_items.ordered_at_et::date as "ordered_at_date"
  , order_items.ordered_at_et as "ordered_at_timestamp"
  , seller_orders.order_number as "seller_order_number"

from analytics.core.order_items
  inner join analytics.core.seller_orders on analytics.core.seller_orders.id = analytics.core.order_items.seller_order_id
  left outer join analytics.core.sellers on seller_orders.seller_id = sellers.id

where
  order_items.ordered_at_et::date >= cast(dateadd(dd, -46, getdate()) as date)
  and order_items.ordered_at_et::date <= cast(dateadd(dd, -0, getdate()) as date)
  //order_items.ordered_at_et::date = '2025-01-05'
  and seller_orders.is_direct_order = False
  //and "seller_order_number" = '4AEE8829-37A062-43C18'

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

marketplace_qty_per_day = sales_per_day_df.groupby('ordered_at_date')['sold_qty_per_seller_order'].sum()
sales_per_day_df = pd.merge(sales_per_day_df, marketplace_qty_per_day, how='right', on='ordered_at_date')
sales_per_day_df.rename(columns={'sold_qty_per_seller_order_x':'sold_qty_per_seller_order', 'sold_qty_per_seller_order_y':'total_sold_qty_per_day'}, inplace=True)

##Merge frames
ris_df = pd.merge(ris_df, order_df, how='left', on='pcid')
ris_df.dropna(subset='ordered_at_date', inplace=True)

##Exclude Ris not in backlog when order placed
ris_df["exclude"] = 0

#ris_df.loc[(ris_df['receipt_date'] < ris_df['ordered_at_date']) & (ris_df['processing_date'] > ris_df['ordered_at_date']), 'exclude'] = 1
ris_df.loc[(ris_df['receipt_date'] < ris_df['ordered_at_date']) & (ris_df['shelving_timestamp'] > ris_df['ordered_at_timestamp']), 'exclude'] = 1

max_exclude = ris_df.groupby('seller_order_number')['exclude'].min()
ris_df = pd.merge(ris_df, max_exclude, how='right', on='seller_order_number')
ris_df.rename(columns={'exclude_x':'exclude', 'exclude_y':'max_exclude'}, inplace=True)

ris_df = ris_df.loc[ris_df['max_exclude'] == 1]

ris_df.drop(['exclude', 'max_exclude'], axis=1, inplace=True)

ris_df.sort_values(by=['ordered_at_timestamp', 'pcid', 'expected_quantity'], ascending=[True, True, False], inplace=True)

##Exclude orders we could never fulfill with waiting product
ris_df["pcid_per_day"] = ris_df['pcid'].astype(str) + '~' + ris_df['ordered_at_date'].astype(str)

expected_qty_per_day = ris_df.groupby('pcid_per_day')['expected_quantity'].sum()
ris_df = pd.merge(ris_df, expected_qty_per_day, how='right', on='pcid_per_day')
ris_df.rename(columns={'expected_quantity_x':'expected_quantity', 'expected_quantity_y':'total_pcid_per_day'}, inplace=True)

ris_df.drop(['pcid_per_day'], axis=1, inplace=True)
ris_df.dropna(subset='seller_order_number', inplace=True)

ris_df["pcid_id"] = ris_df['pcid'].astype(str) + '~' + ris_df['ri_number'].astype(str)
ris_df["pcid_seller_order_id"] = ris_df['pcid'].astype(str) + '~' + ris_df['seller_order_number'].astype(str)

ris_df["exclude"] = ris_df['total_pcid_per_day'] - ris_df['sold_qty_per_seller_order']

min_exclude = ris_df.groupby('seller_order_number')['exclude'].min()
ris_df = pd.merge(ris_df, min_exclude, how='right', on='seller_order_number')
ris_df.rename(columns={'exclude_x':'exclude', 'exclude_y':'min_exclude'}, inplace=True)

ris_df = ris_df.loc[ris_df['min_exclude'] >= 0]

ris_df["pcid_id"] = ris_df['pcid'].astype(str) + '~' + ris_df['ri_number'].astype(str)
ris_df["pcid_seller_order_id"] = ris_df['pcid'].astype(str) + '~' + ris_df['seller_order_number'].astype(str)

ris_df.drop(['total_pcid_per_day', 'exclude', 'min_exclude'], axis=1, inplace=True)

ris_df["remaining_to_sell"] = 0.0
ris_df["remaining_to_fill"] = 0.0
ris_df["order_part_complete"] = "No"
ris_df["pcid_check"] = ris_df['pcid_id']
ris_df["seller_check"] = ris_df['pcid_seller_order_id']

filled_order_parts = pd.DataFrame()

##Create list of unique orders to try and fulfill then loop through them
unique_orders_df = ris_df.copy()
unique_orders_df.drop_duplicates(subset='seller_order_number', inplace=True)
unique_orders_df = unique_orders_df[['seller_order_number']]

print(len(unique_orders_df));

s = len(unique_orders_df);
#s = 1000;
i = 0;

while i < s:
    order_to_fill = unique_orders_df.head(1)

    int_check = divmod(i + 1, 100)##########################

    if int_check[1] == 0:
        print(i + 1)
        executionTime = (time.time() - startTime)
        print(executionTime)

    order_to_fill = pd.merge(order_to_fill, ris_df, how='left', on='seller_order_number')

    l = len(order_to_fill);
    j = 0;

    while j < l:
        order_part_to_fill = order_to_fill.head(1)

        order_part_to_fill.iloc[:,12] = order_part_to_fill['expected_quantity'] - order_part_to_fill['sold_qty_per_seller_order'] #remaining_to_sell
        order_part_to_fill.loc[order_part_to_fill['remaining_to_sell'] < 0, 'remaining_to_sell'] = 0

        order_part_to_fill.iloc[:,13] = order_part_to_fill['sold_qty_per_seller_order'] - order_part_to_fill['expected_quantity'] #remaining_to_fill
        order_part_to_fill.loc[order_part_to_fill['remaining_to_fill'] < 0, 'remaining_to_fill'] = 0

        order_part_to_fill.loc[order_part_to_fill['remaining_to_fill'] == 0, 'order_part_complete'] = "Yes"

        #Merge completed order parts with filled_order_parts dataframe
        order_part_to_fill_complete = order_part_to_fill.copy()
        order_part_to_fill_complete = order_part_to_fill_complete.loc[order_part_to_fill_complete['order_part_complete'] == "Yes"]

        if len(filled_order_parts) > 0:
            filled_order_parts = pd.concat([filled_order_parts, order_part_to_fill_complete])

        if len(filled_order_parts) == 0:
            filled_order_parts = order_part_to_fill_complete

        #Merge to order_to_fill frame and adjust exp_qty and remove fulfilled order parts
        order_to_fill = pd.merge(order_to_fill, order_part_to_fill, how='left', on='pcid_id')

        order_to_fill.loc[order_to_fill['pcid_check_y'] == order_to_fill['pcid_check_x'], 'expected_quantity_x'] = order_to_fill['remaining_to_sell_y']

        order_to_fill.drop(list(order_to_fill.filter(regex="_y")), axis=1, inplace=True)

        char_to_find = "_x"
        new_columns = [col.replace(char_to_find, "")if char_to_find in col else col for col in order_to_fill.columns]
        order_to_fill.columns = new_columns

        order_to_fill = pd.merge(order_to_fill, order_part_to_fill, how='left', on='pcid_seller_order_id')

        order_to_fill.loc[order_to_fill['seller_check_y'] == order_to_fill['seller_check_x'], 'order_part_complete_x'] = order_to_fill['order_part_complete_y']

        order_to_fill.drop(list(order_to_fill.filter(regex="_y")), axis=1, inplace=True)

        char_to_find = "_x"
        new_columns = [col.replace(char_to_find, "")if char_to_find in col else col for col in order_to_fill.columns]
        order_to_fill.columns = new_columns

        order_to_fill = order_to_fill.loc[(order_to_fill['expected_quantity'] > 0) & (order_to_fill['order_part_complete'] == "No")]


        #Merge examined order back with original dataframe to adjust expected_quantity and remove fulfilled order parts
        ris_df = pd.merge(ris_df, order_part_to_fill, how='left', on='pcid_id')

        ris_df.loc[ris_df['pcid_check_y'] == ris_df['pcid_check_x'], 'expected_quantity_x'] = ris_df['remaining_to_sell_y']

        ris_df.drop(list(ris_df.filter(regex="_y")), axis=1, inplace=True)

        char_to_find = "_x"
        new_columns = [col.replace(char_to_find, "")if char_to_find in col else col for col in ris_df.columns]
        ris_df.columns = new_columns


        ris_df = pd.merge(ris_df, order_part_to_fill, how='left', on='pcid_seller_order_id')

        ris_df.loc[ris_df['seller_check_y'] == ris_df['seller_check_x'], 'order_part_complete_x'] = ris_df['order_part_complete_y']

        ris_df.drop(list(ris_df.filter(regex="_y")), axis=1, inplace=True)

        char_to_find = "_x"
        new_columns = [col.replace(char_to_find, "")if char_to_find in col else col for col in ris_df.columns]
        ris_df.columns = new_columns

        ris_df = ris_df.loc[(ris_df['expected_quantity'] > 0) & (ris_df['order_part_complete'] == "No")]

        j+=1

    unique_orders_df = unique_orders_df.iloc[1:]

    i+=1


filled_order_parts.drop(['order_part_complete', 'pcid_check', 'seller_check', 'remaining_to_fill', 'pcid_id', 'pcid_seller_order_id'], axis=1, inplace=True)


filled_order_parts.sort_values(by=['ordered_at_timestamp', 'seller_order_number'], ascending=[True, True], inplace=True)





filled_order_parts_tab = test.worksheet('filled_order_parts')
filled_order_parts_tab.clear()
gd.set_with_dataframe(filled_order_parts_tab, filled_order_parts)##############################

##Aggragate values per day
filled_order_parts["monetary_loss"] = filled_order_parts['sold_qty_per_seller_order'].astype('float64') * filled_order_parts['sold_price'].astype('float64')

missed_value_per_day = filled_order_parts.groupby('ordered_at_date')['monetary_loss'].sum()
filled_order_parts = pd.merge(filled_order_parts, missed_value_per_day, how='right', on='ordered_at_date')
filled_order_parts.rename(columns={'monetary_loss_x':'monetary_loss', 'monetary_loss_y':'monetary_loss_per_day'}, inplace=True)

missed_sales_per_day = filled_order_parts.groupby('ordered_at_date')['sold_qty_per_seller_order'].sum()
filled_order_parts = pd.merge(filled_order_parts, missed_sales_per_day, how='right', on='ordered_at_date')
filled_order_parts.rename(columns={'sold_qty_per_seller_order_x':'sold_qty_per_seller_order', 'sold_qty_per_seller_order_y':'missed_sales_per_day'}, inplace=True)

filled_order_parts.drop_duplicates(subset='ordered_at_date', inplace=True)
filled_order_parts = filled_order_parts[['ordered_at_date', 'monetary_loss_per_day', 'missed_sales_per_day']]

##Combine frames
filled_order_parts = pd.merge(filled_order_parts, sales_per_day_df, how='left', on='ordered_at_date')

filled_order_parts["monetary_loss_%"] = filled_order_parts['monetary_loss_per_day'].astype('float64') / filled_order_parts['total_sold_price_per_day'].astype('float64')

filled_order_parts["missed_sales_%"] = filled_order_parts['missed_sales_per_day'].astype('float64') / filled_order_parts['total_sold_qty_per_day'].astype('float64')

filled_order_parts = filled_order_parts[['ordered_at_date', 'monetary_loss_per_day', 'missed_sales_per_day', 'monetary_loss_%', 'missed_sales_%']]


filled_order_parts.drop_duplicates(subset='ordered_at_date', inplace=True)

##Write data to sheet
filled_order_parts_tab = test.worksheet('Output')
filled_order_parts_tab.clear()
gd.set_with_dataframe(filled_order_parts_tab, filled_order_parts)##############################

