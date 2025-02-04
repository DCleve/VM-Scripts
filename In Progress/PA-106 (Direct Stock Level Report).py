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

##Calculate direct share
direct_sql = ("""
with
    direct_eligible as (
      select
        concat(products.product_name, '~', products.set_name) as identifier

      from analytics.core.products

      where
          products.max_fulfillable_quantity > 0
          and (products.product_line = 'Magic' or products.product_line = 'Pokemon' or products.product_line = 'YuGiOh')
          and products.status = 'Released Product'
     )

  , order_info as (
      select
          concat(order_items.product_name, '~', order_items.set_name) as identifier
          , order_items.total_usd as total_usd

      from analytics.core.order_items
        inner join analytics.core.seller_orders on order_items.seller_order_id = seller_orders.id

      where
          seller_orders.is_direct_order = 'True'
          and order_items.ordered_at_et::date >= cast(dateadd(dd, -90, getdate()) as date)
          and order_items.condition in('Near Mint', 'Lightly Played', 'Moderately Played', 'Near Mint Foil', 'Lightly Played Foil', 'Moderately Played Foil', 'Near Mint Unlimited', 'Lightly Played Unlimited', 'Moderately Played Unlimited', 'Near Mint 1st Edition', 'Lightly Played 1st Edition', 'Moderately Played 1st Edition', 'Near Mint Limited', 'Lightly Played Limited', 'Moderately Played Limited', 'Near Mint Holofoil', 'Lightly Played Holofoil', 'Moderately Played Holofoil', 'Near Mint 1st Edition Holofoil', 'Lightly Played 1st Edition Holofoil', 'Moderately Played 1st Edition Holofoil', 'Near Mint Reverse Holofoil', 'Lightly Played Reverse Holofoil', 'Moderately Played Reverse Holofoil', 'Near Mint Unlimited Holofoil', 'Lightly Played Unlimited Holofoil', 'Moderately Played Unlimited Holofoil')
    )

    select
        sum(order_info.total_usd) as "total_direct_usd"

    from direct_eligible
        left outer join order_info on direct_eligible.identifier = order_info.identifier
""")

cursor = snowflake_pull.cursor()
cursor.execute(direct_sql)
direct_df = cursor.fetch_pandas_all()
direct_df.drop(direct_df.filter(like='Unnamed'), axis=1, inplace=True)

non_direct_sql = ("""
with
    direct_eligible as (
      select
        concat(products.product_name, '~', products.set_name) as identifier

      from analytics.core.products

      where
          products.max_fulfillable_quantity > 0
          and (products.product_line = 'Magic' or products.product_line = 'Pokemon' or products.product_line = 'YuGiOh')
          and products.status = 'Released Product'
     )

  , order_info as (
      select
          concat(order_items.product_name, '~', order_items.set_name) as identifier
          , order_items.total_usd as total_usd

      from analytics.core.order_items
        inner join analytics.core.seller_orders on order_items.seller_order_id = seller_orders.id

      where
          seller_orders.is_direct_order = 'False'
          and order_items.ordered_at_et::date >= cast(dateadd(dd, -90, getdate()) as date)
          and order_items.condition in('Near Mint', 'Lightly Played', 'Moderately Played', 'Near Mint Foil', 'Lightly Played Foil', 'Moderately Played Foil', 'Near Mint Unlimited', 'Lightly Played Unlimited', 'Moderately Played Unlimited', 'Near Mint 1st Edition', 'Lightly Played 1st Edition', 'Moderately Played 1st Edition', 'Near Mint Limited', 'Lightly Played Limited', 'Moderately Played Limited', 'Near Mint Holofoil', 'Lightly Played Holofoil', 'Moderately Played Holofoil', 'Near Mint 1st Edition Holofoil', 'Lightly Played 1st Edition Holofoil', 'Moderately Played 1st Edition Holofoil', 'Near Mint Reverse Holofoil', 'Lightly Played Reverse Holofoil', 'Moderately Played Reverse Holofoil', 'Near Mint Unlimited Holofoil', 'Lightly Played Unlimited Holofoil', 'Moderately Played Unlimited Holofoil')
    )

    select
        sum(order_info.total_usd) as "total_non_direct_usd"

    from direct_eligible
        left outer join order_info on direct_eligible.identifier = order_info.identifier
""")

cursor = snowflake_pull.cursor()
cursor.execute(non_direct_sql)
non_direct_df = cursor.fetch_pandas_all()
non_direct_df.drop(non_direct_df.filter(like='Unnamed'), axis=1, inplace=True)

direct_df = pd.concat([direct_df, non_direct_df])
direct_df['total_direct_usd'] = direct_df['total_direct_usd'].fillna(direct_df['total_direct_usd'].max())
direct_df['total_non_direct_usd'] = direct_df['total_non_direct_usd'].fillna(direct_df['total_non_direct_usd'].max())
direct_df.drop_duplicates(subset='total_direct_usd', inplace=True)
direct_df["direct_share"] = direct_df['total_direct_usd'].astype('float64') / (direct_df['total_direct_usd'].astype('float64') + direct_df['total_non_direct_usd'].astype('float64'))

direct_share = direct_df.iloc[0, 2]

##Import base PCID info (determines what is direct eligible)
pcid_sql = ("""
with
    product_info as (
      select
        concat(products.product_name, '~', products.set_name) as identifier

      from analytics.core.products

      where
          products.max_fulfillable_quantity > 0
          and (products.product_line = 'Magic' or products.product_line = 'Pokemon' or products.product_line = 'YuGiOh')

      group by products.product_line, products.product_name, products.set_name
     )

     , pcid_info as (
       select
        order_items.product_condition_id as pcid
       , concat(order_items.product_name, '~', order_items.set_name) as identifier
       , order_items.condition as condition_name

       from analytics.core.order_items

       where
        (order_items.product_line = 'Magic' or order_items.product_line = 'Pokemon' or order_items.product_line = 'YuGiOh')
       and condition_name in('Near Mint', 'Lightly Played', 'Moderately Played', 'Near Mint Foil', 'Lightly Played Foil', 'Moderately Played Foil', 'Near Mint Unlimited', 'Lightly Played Unlimited', 'Moderately Played Unlimited', 'Near Mint 1st Edition', 'Lightly Played 1st Edition', 'Moderately Played 1st Edition', 'Near Mint Limited', 'Lightly Played Limited', 'Moderately Played Limited', 'Near Mint Holofoil', 'Lightly Played Holofoil', 'Moderately Played Holofoil', 'Near Mint 1st Edition Holofoil', 'Lightly Played 1st Edition Holofoil', 'Moderately Played 1st Edition Holofoil', 'Near Mint Reverse Holofoil', 'Lightly Played Reverse Holofoil', 'Moderately Played Reverse Holofoil', 'Near Mint Unlimited Holofoil', 'Lightly Played Unlimited Holofoil', 'Moderately Played Unlimited Holofoil')
      )

      select
        distinct pcid_info.pcid as "pcid"

      from product_info
        left outer join pcid_info on product_info.identifier = pcid_info.identifier
""")

cursor = snowflake_pull.cursor()
cursor.execute(pcid_sql)
pcid_df = cursor.fetch_pandas_all()
pcid_df.drop(pcid_df.filter(like='Unnamed'), axis=1, inplace=True)

##Import direct inventory info
sql_inv = ("""
select
    distinct direct_inventory_history.product_condition_id as "pcid"
    , avg(direct_inventory_history.quantity_available) as "avg_qty_on_hand"

from analytics.core.direct_inventory_history

where
    direct_inventory_history.valid_from_et::date >= cast(dateadd(dd, -90, getdate()) as date)
    and (direct_inventory_history.product_line = 'Magic' or direct_inventory_history.product_line = 'Pokemon' or direct_inventory_history.product_line = 'YuGiOh')

group by "pcid"

""")

cursor = snowflake_pull.cursor()
cursor.execute(sql_inv)
inv_df = cursor.fetch_pandas_all()
inv_df.drop(inv_df.filter(like='Unnamed'), axis=1, inplace=True)

##Merge PCID and inventory info
pcid_df = pd.merge(pcid_df, inv_df, how='left', on='pcid')

pcid_df['avg_qty_on_hand'] = pcid_df['avg_qty_on_hand'].apply(pd.to_numeric, errors='coerce').fillna(0)

##Import transaction data
orders_sql = ("""
select
    order_items.product_condition_id as "pcid"
    , order_items.quantity as "product_qty"
    , orders.ordered_at_et::date as "order_date"
    , seller_orders.is_direct_order as "direct_orders"
    , order_items.unit_price_usd as "sold_price"

from analytics.core.orders
    inner join analytics.core.order_items on orders.id = order_items.order_id
    inner join analytics.core.seller_orders on order_items.seller_order_id = seller_orders.id

where
    orders.ordered_at_et::date >= cast(dateadd(dd, -90, getdate()) as date)
""")

cursor = snowflake_pull.cursor()
cursor.execute(orders_sql)
orders_df = cursor.fetch_pandas_all()
orders_df.drop(orders_df.filter(like='Unnamed'), axis=1, inplace=True)

##Aggragate qtys
orders_df["combined"] = orders_df['order_date'].astype(str) + '~' + orders_df['pcid'].astype(str)
orders_direct_df = orders_df.copy()
orders_direct_df = orders_direct_df.loc[orders_direct_df['direct_orders'] == True]

qty_per_pcid_per_day = orders_df.groupby('combined')['product_qty'].sum()
orders_df = pd.merge(orders_df, qty_per_pcid_per_day, how='right', on='combined')
orders_df.rename(columns={'product_qty_x':'product_qty', 'product_qty_y':'qty_per_pcid_per_day'}, inplace=True)

avg_orders_per_day = orders_df.groupby('pcid')['qty_per_pcid_per_day'].mean()
orders_df = pd.merge(orders_df, avg_orders_per_day, how='right', on='pcid')
orders_df.rename(columns={'qty_per_pcid_per_day_x':'qty_per_pcid_per_day', 'qty_per_pcid_per_day_y':'avg_qty_per_pcid_per_day'}, inplace=True)

max_orders_per_day = orders_df.groupby('pcid')['qty_per_pcid_per_day'].max()
orders_df = pd.merge(orders_df, max_orders_per_day, how='right', on='pcid')
orders_df.rename(columns={'qty_per_pcid_per_day_x':'qty_per_pcid_per_day', 'qty_per_pcid_per_day_y':'max_qty_per_pcid_per_day'}, inplace=True)

#avg_sold_price = orders_df.groupby('pcid')['sold_price'].mean()
#orders_df = pd.merge(orders_df, avg_sold_price, how='right', on='pcid')
#orders_df.rename(columns={'sold_price_x':'sold_price', 'sold_price_y':'avg_sold_price'}, inplace=True)


direct_qty_per_pcid_per_day = orders_direct_df.groupby('combined')['product_qty'].sum()
orders_direct_df = pd.merge(orders_direct_df, direct_qty_per_pcid_per_day, how='right', on='combined')
orders_direct_df.rename(columns={'product_qty_x':'product_qty', 'product_qty_y':'direct_qty_per_pcid_per_day'}, inplace=True)

avg_direct_orders_per_day = orders_direct_df.groupby('pcid')['direct_qty_per_pcid_per_day'].mean()
orders_direct_df = pd.merge(orders_direct_df, avg_direct_orders_per_day, how='right', on='pcid')
orders_direct_df.rename(columns={'direct_qty_per_pcid_per_day_x':'direct_qty_per_pcid_per_day', 'direct_qty_per_pcid_per_day_y':'avg_direct_qty_per_pcid_per_day'}, inplace=True)

max_direct_orders_per_day = orders_direct_df.groupby('pcid')['direct_qty_per_pcid_per_day'].max()
orders_direct_df = pd.merge(orders_direct_df, max_direct_orders_per_day, how='right', on='pcid')
orders_direct_df.rename(columns={'direct_qty_per_pcid_per_day_x':'direct_qty_per_pcid_per_day', 'direct_qty_per_pcid_per_day_y':'max_direct_qty_per_pcid_per_day'}, inplace=True)

#avg_sold_price = orders_direct_df.groupby('pcid')['sold_price'].mean()
#orders_direct_df = pd.merge(orders_direct_df, avg_sold_price, how='right', on='pcid')
#orders_direct_df.rename(columns={'sold_price_x':'sold_price', 'sold_price_y':'avg_direct_sold_price'}, inplace=True)

orders_df.drop_duplicates(subset='pcid', inplace=True)
orders_direct_df.drop_duplicates(subset='pcid', inplace=True)

#orders_df = orders_df[['pcid', 'avg_qty_per_pcid_per_day', 'max_qty_per_pcid_per_day', 'avg_sold_price']]
#orders_direct_df = orders_direct_df[['pcid', 'avg_direct_qty_per_pcid_per_day', 'max_direct_qty_per_pcid_per_day', 'avg_direct_sold_price']]

orders_df = orders_df[['pcid', 'avg_qty_per_pcid_per_day', 'max_qty_per_pcid_per_day']]
orders_direct_df = orders_direct_df[['pcid', 'avg_direct_qty_per_pcid_per_day', 'max_direct_qty_per_pcid_per_day']]

##Combine frames
pcid_df = pd.merge(pcid_df, orders_df, how='left', on='pcid')

pcid_df['avg_qty_per_pcid_per_day'] = pcid_df['avg_qty_per_pcid_per_day'].apply(pd.to_numeric, errors='coerce').fillna(0)
pcid_df['max_qty_per_pcid_per_day'] = pcid_df['max_qty_per_pcid_per_day'].apply(pd.to_numeric, errors='coerce').fillna(0)
#pcid_df['avg_sold_price'] = pcid_df['avg_sold_price'].apply(pd.to_numeric, errors='coerce').fillna(0)

pcid_df = pd.merge(pcid_df, orders_direct_df, how='left', on='pcid')

pcid_df['avg_direct_qty_per_pcid_per_day'] = pcid_df['avg_direct_qty_per_pcid_per_day'].apply(pd.to_numeric, errors='coerce').fillna(0)
pcid_df['max_direct_qty_per_pcid_per_day'] = pcid_df['max_direct_qty_per_pcid_per_day'].apply(pd.to_numeric, errors='coerce').fillna(0)
#pcid_df['avg_direct_sold_price'] = pcid_df['avg_direct_sold_price'].apply(pd.to_numeric, errors='coerce').fillna(0)

pcid_df = pcid_df.loc[pcid_df['avg_qty_per_pcid_per_day'] > 0]

##Import lead time data
lead_time_sql = ("""
with
    ri_staging as (
      select
          seller_orders.order_number as seller_order_number
          , order_items.product_condition_id as pcid
          , seller_orders.ordered_at_et::date as ordered_at_date
          , seller_orders.has_refund as has_refund
          , reimbursement_invoices.reimbursement_invoice_number as ri_number
          , min(reimbursement_invoices.created_at_et::date) as min_ri_created_at_date
          , max(reimbursement_invoices.created_at_et::date) as max_ri_created_at_date
          , min(reimbursement_invoices.received_at_et::date) as min_ri_received_at_date
          , max(reimbursement_invoices.received_at_et::date) as max_ri_received_at_date
          , min(reimbursement_invoices.processing_ended_at_et::date) as min_ri_proc_at_date
          , max(reimbursement_invoices.processing_ended_at_et::date) as max_ri_proc_at_date
          , min(reimbursement_invoices.shelved_at_et::date) as min_ri_shelved_at_date
          , max(reimbursement_invoices.shelved_at_et::date) as max_ri_shelved_at_date

      from analytics.core.reimbursement_invoices
        left outer join hvr_tcgstore_production.tcgd.ReimOrderSellerOrderProduct on hvr_tcgstore_production.tcgd.ReimOrderSellerOrderProduct.ReimOrderId = analytics.core.reimbursement_invoices.id
        left outer join analytics.core.order_items on analytics.core.order_items.id = hvr_tcgstore_production.tcgd.ReimOrderSellerOrderProduct.SellerOrderProductId and hvr_tcgstore_production.tcgd.ReimOrderSellerOrderProduct.hvr_deleted = 0
        inner join analytics.core.seller_orders on analytics.core.seller_orders.id = analytics.core.order_items.seller_order_id
        inner join analytics.core.orders on analytics.core.orders.id = analytics.core.seller_orders.order_id
        left outer join analytics.core.refunds on seller_orders.id = refunds.seller_order_id
        left outer join analytics.core.sellers on seller_orders.seller_id = sellers.id

      where
        orders.status = 'Complete'
        and seller_orders.is_direct_order = true
        and seller_orders.seller_id != 249
        and seller_orders.ordered_at_et::date >= cast(dateadd(dd, -90, getdate()) as date)

      group by 1, 2, 3, 4, 5
    )

    select
        ri_staging.pcid as "pcid"
        , max(ri_staging.max_ri_shelved_at_date - ri_staging.ordered_at_date) as "max_total_lead_time"
        , avg(ri_staging.max_ri_shelved_at_date - ri_staging.ordered_at_date) as "avg_total_lead_time"

    from
        ri_staging

    group by 1
""")

cursor = snowflake_pull.cursor()
cursor.execute(lead_time_sql)
lead_time_df = cursor.fetch_pandas_all()
lead_time_df.drop(lead_time_df.filter(like='Unnamed'), axis=1, inplace=True)

##Combine frames
pcid_df = pd.merge(pcid_df, lead_time_df, how='left', on='pcid')

##Import market value info
market_sql = ("""
with
    price_info as (
      select
          concat(daily_direct_inventory.product_name, '~', daily_direct_inventory.set_name, '~', daily_direct_inventory.condition) as identifier
          , daily_direct_inventory.product_name as product_name
          , daily_direct_inventory.set_name as set_name
          , daily_direct_inventory.condition as condition_name
         ,  avg(daily_direct_inventory.market_price_usd) as avg_market_price

      from analytics.core.daily_direct_inventory

      where
          daily_direct_inventory.date_et::date >= cast(dateadd(dd, -90, getdate()) as date)

      group by identifier, product_name, set_name, condition_name
     )

     , pcid_info as (
            select
                order_items.product_condition_id as pcid
               , concat(order_items.product_name, '~', order_items.set_name, '~', order_items.condition) as identifier

             from analytics.core.order_items

             where
                (order_items.product_line = 'Magic' or order_items.product_line = 'Pokemon' or order_items.product_line = 'YuGiOh')
               and order_items.condition in('Near Mint', 'Lightly Played', 'Moderately Played', 'Near Mint Foil', 'Lightly Played Foil', 'Moderately Played Foil', 'Near Mint Unlimited', 'Lightly Played Unlimited', 'Moderately Played Unlimited', 'Near Mint 1st Edition', 'Lightly Played 1st Edition', 'Moderately Played 1st Edition', 'Near Mint Limited', 'Lightly Played Limited', 'Moderately Played Limited', 'Near Mint Holofoil', 'Lightly Played Holofoil', 'Moderately Played Holofoil', 'Near Mint 1st Edition Holofoil', 'Lightly Played 1st Edition Holofoil', 'Moderately Played 1st Edition Holofoil', 'Near Mint Reverse Holofoil', 'Lightly Played Reverse Holofoil', 'Moderately Played Reverse Holofoil', 'Near Mint Unlimited Holofoil', 'Lightly Played Unlimited Holofoil', 'Moderately Played Unlimited Holofoil')
     )

     select
        distinct pcid_info.pcid as "pcid"
        //, price_info.product_name as "product_name"
        //, price_info.set_name as "set_name"
        //, price_info.condition_name as "condition_name"
        , price_info.avg_market_price as "avg_market_price"

     from price_info
        left outer join pcid_info on pcid_info.identifier = price_info.identifier

""")

cursor = snowflake_pull.cursor()
cursor.execute(market_sql)
market_df = cursor.fetch_pandas_all()
market_df.drop(market_df.filter(like='Unnamed'), axis=1, inplace=True)

##Combine frames
pcid_df = pd.merge(pcid_df, market_df, how='left', on='pcid')

##Math
pcid_df["desired_stock_level"] = (pcid_df['max_qty_per_pcid_per_day'].astype('float64') * pcid_df['max_total_lead_time'].astype('float64')) * direct_share

pcid_df.sort_values(by=['pcid'], ascending=[True], inplace=True)

pcid_df["stock_level_delta"] = pcid_df['desired_stock_level'].astype('float64') - pcid_df['avg_qty_on_hand'].astype('float64')

pcid_df["stock_level_delta_%"] = pcid_df["stock_level_delta"].astype('float64') /pcid_df['desired_stock_level'].astype('float64')

##Make final dataframe

pcid_df = pcid_df[['pcid', 'stock_level_delta', 'stock_level_delta_%', 'desired_stock_level', 'avg_qty_on_hand', 'avg_qty_per_pcid_per_day', 'max_qty_per_pcid_per_day', 'avg_direct_qty_per_pcid_per_day', 'max_direct_qty_per_pcid_per_day', 'avg_market_price']]

pcid_df.rename(columns={'avg_qty_per_pcid_per_day':'Avg ADD (Marketplace)', 'max_qty_per_pcid_per_day':'Max ADD (Marketplace)', 'avg_direct_qty_per_pcid_per_day':'Avg ADD (Direct)', 'max_direct_qty_per_pcid_per_day':'Max ADD (Direct)', 'avg_market_price':'Avg Market Price ($)'}, inplace=True)

##Write data to sheet
report_tab = gc.open_by_key('1m2aIUUpQ7dWpuosuFdO2xjsPM5k-2hNviWcI7S3iiwc').worksheet('Data')
report_tab.clear()

pcid_df_2 = pcid_df.copy()

pcid_df_2 = pcid_df_2[['Avg ADD (Marketplace)', 'Max ADD (Marketplace)', 'Avg ADD (Direct)', 'Max ADD (Direct)', 'Avg Market Price ($)']]
pcid_df = pcid_df[['pcid', 'stock_level_delta', 'stock_level_delta_%', 'desired_stock_level', 'avg_qty_on_hand']]






gd.set_with_dataframe(report_tab, pcid_df, row=1, col=1)

time.sleep(30)


gd.set_with_dataframe(report_tab, pcid_df_2, row=1, col=6)