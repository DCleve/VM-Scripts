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

##Determine Direct eligility
pcid_sql = ("""
select
    distinct productcondition.productconditionid as "pcid"

from hvr_tcgstore_production.pdt.product
    inner join hvr_tcgstore_production.pdt.productcondition on product.productid = productcondition.productid
    inner join hvr_tcgstore_production.pdt.category on product.categoryid = category.categoryid
    inner join hvr_tcgstore_production.pdt.condition on productcondition.conditionid = hvr_tcgstore_production.pdt.condition.conditionid

where
    product.maxfulfillableqty > 0
    and ((category.categoryname = 'Magic' and hvr_tcgstore_production.pdt.condition.conditionname in('Near Mint', 'Lightly Played', 'Moderately Played', 'Near Mint Foil', 'Lightly Played Foil', 'Moderately Played Foil')) or (category.categoryname = 'Pokemon' and hvr_tcgstore_production.pdt.condition.conditionname in('Near Mint', 'Lightly Played', 'Moderately Played',  'Near Mint Holofoil', 'Lightly Played Holofoil', 'Moderately Played Holofoil',  'Near Mint Reverse Holofoil', 'Lightly Played Reverse Holofoil', 'Moderately Played Reverse Holofoil', 'Near Mint Unlimited', 'Lightly Played Unlimited', 'Moderately Played Unlimited',  'Near Mint 1st Edition', 'Lightly Played 1st Edition', 'Moderately Played 1st Edition', 'Near Mint 1st Edition Holofoil', 'Lightly Played 1st Edition Holofoil', 'Moderately Played 1st Edition Holofoil', 'Near Mint Unlimited Holofoil', 'Lightly Played Unlimited Holofoil', 'Moderately Played Unlimited Holofoil')) or (category.categoryname = 'YuGiOh' and hvr_tcgstore_production.pdt.condition.conditionname in('Near Mint Unlimited', 'Lightly Played Unlimited', 'Near Mint 1st Edition', 'Lightly Played 1st Edition', 'Near Mint Limited', 'Lightly Played Limited')))
""")

cursor = snowflake_pull.cursor()
cursor.execute(pcid_sql)
pcid_df = cursor.fetch_pandas_all()
pcid_df.drop(pcid_df.filter(like='Unnamed'), axis=1, inplace=True)

##Calculate direct share
direct_share_sql = ("""
with
    direct_orders as (
      select
        distinct order_items.product_condition_id as pcid
        , sum(order_items.unit_price_usd) as sold_price

      from analytics.core.order_items
        inner join analytics.core.seller_orders on analytics.core.seller_orders.id = analytics.core.order_items.seller_order_id
        left outer join analytics.core.sellers on seller_orders.seller_id = sellers.id

      where
        order_items.ordered_at_et::date >= cast(dateadd(dd, -90, getdate()) as date)
        and seller_orders.is_direct_order = True

      group by 1
     )

     ,
     non_direct_orders as (
      select
        distinct order_items.product_condition_id as pcid
        , sum(order_items.unit_price_usd) as sold_price

      from analytics.core.order_items
        inner join analytics.core.seller_orders on analytics.core.seller_orders.id = analytics.core.order_items.seller_order_id
        left outer join analytics.core.sellers on seller_orders.seller_id = sellers.id

      where
        order_items.ordered_at_et::date >= cast(dateadd(dd, -90, getdate()) as date)
        and seller_orders.is_direct_order = False

      group by 1
      )

      , direct_eligible as (
        select
          distinct productcondition.productconditionid as pcid

          from hvr_tcgstore_production.pdt.product
            inner join hvr_tcgstore_production.pdt.productcondition on product.productid = productcondition.productid
            inner join hvr_tcgstore_production.pdt.category on product.categoryid = category.categoryid
            inner join hvr_tcgstore_production.pdt.setname on product.setnameid = setname.setnameid
            inner join hvr_tcgstore_production.pdt.condition on productcondition.conditionid = hvr_tcgstore_production.pdt.condition.conditionid

          where
            product.maxfulfillableqty > 0
            and ((category.categoryname = 'Magic' and hvr_tcgstore_production.pdt.condition.conditionname in('Near Mint', 'Lightly Played', 'Moderately Played', 'Near Mint Foil', 'Lightly Played Foil', 'Moderately Played Foil')) or (category.categoryname = 'Pokemon' and hvr_tcgstore_production.pdt.condition.conditionname in('Near Mint', 'Lightly Played', 'Moderately Played',  'Near Mint Holofoil', 'Lightly Played Holofoil', 'Moderately Played Holofoil',  'Near Mint Reverse Holofoil', 'Lightly Played Reverse Holofoil', 'Moderately Played Reverse Holofoil', 'Near Mint Unlimited', 'Lightly Played Unlimited', 'Moderately Played Unlimited',  'Near Mint 1st Edition', 'Lightly Played 1st Edition', 'Moderately Played 1st Edition', 'Near Mint 1st Edition Holofoil', 'Lightly Played 1st Edition Holofoil', 'Moderately Played 1st Edition Holofoil', 'Near Mint Unlimited Holofoil', 'Lightly Played Unlimited Holofoil', 'Moderately Played Unlimited Holofoil')) or (category.categoryname = 'YuGiOh' and hvr_tcgstore_production.pdt.condition.conditionname in('Near Mint Unlimited', 'Lightly Played Unlimited', 'Near Mint 1st Edition', 'Lightly Played 1st Edition', 'Near Mint Limited', 'Lightly Played Limited')))
    )

    select
        sum(direct_orders.sold_price) as "total_direct_sold_price"
        , sum(non_direct_orders.sold_price) as "total_non_direct_sold_price"
        , "total_direct_sold_price" / ("total_direct_sold_price" + "total_non_direct_sold_price") as "direct_share"


    from direct_eligible
        left outer join direct_orders on direct_orders.pcid = direct_eligible.pcid
        left outer join non_direct_orders on non_direct_orders.pcid = direct_eligible.pcid
""")

cursor = snowflake_pull.cursor()
cursor.execute(direct_share_sql)
direct_share_df = cursor.fetch_pandas_all()
direct_share_df.drop(direct_share_df.filter(like='Unnamed'), axis=1, inplace=True)

#direct_share_df = direct_share_df[['direct_share']]

#direct_share_df['direct_share'] = direct_share_df['direct_share'].astype('float64')

#direct_share = direct_share_df.iloc[0, 0]

direct_share = 0.33

##Import qty_on_hand and market price info
sql_inv = ("""
with
    qty_on_hand as (
      select
        distinct direct_inventory_history.product_condition_id as pcid
        , avg(direct_inventory_history.quantity_available) as avg_qty_on_hand

      from analytics.core.direct_inventory_history

      where
          direct_inventory_history.valid_from_et::date >= cast(dateadd(dd, -90, getdate()) as date)
          and (direct_inventory_history.product_line = 'Magic' or direct_inventory_history.product_line = 'Pokemon' or direct_inventory_history.product_line = 'YuGiOh')

      group by pcid
     )

     ,market_price as (
        select
          distinct daily_sku_market_prices.sku as pcid
          , avg(daily_sku_market_prices.market_price_usd) as avg_market_price

        from analytics.core.daily_sku_market_prices

        where
          daily_sku_market_prices.date::date >= cast(dateadd(dd, -90, getdate()) as date)

        group by pcid
     )

     select
        qty_on_hand.pcid as "pcid"
        , qty_on_hand.avg_qty_on_hand as "avg_qty_on_hand"
        , market_price.avg_market_price as "avg_market_price"

     from qty_on_hand
        left outer join market_price on qty_on_hand.pcid = market_price.pcid

    group by "pcid", "avg_qty_on_hand", "avg_market_price"

""")

cursor = snowflake_pull.cursor()
cursor.execute(sql_inv)
inv_df = cursor.fetch_pandas_all()
inv_df.drop(inv_df.filter(like='Unnamed'), axis=1, inplace=True)

##Merge PCID and inventory info
pcid_df = pd.merge(pcid_df, inv_df, how='left', on='pcid')

pcid_df['avg_qty_on_hand'] = pcid_df['avg_qty_on_hand'].apply(pd.to_numeric, errors='coerce').fillna(0)
pcid_df['avg_market_price'] = pcid_df['avg_market_price'].apply(pd.to_numeric, errors='coerce').fillna(0)

##Import lead time data
lead_time_sql = ("""
with
    ri_staging as (
      select
          order_items.product_condition_id as pcid
          , seller_orders.ordered_at_et::date as ordered_at_date
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
        and seller_orders.has_refund = false

      group by 1, 2
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

##Merge with pcid frame
pcid_df = pd.merge(pcid_df, lead_time_df, how='left', on='pcid')

##Import transaction information
orders_sql = ("""
    select
        order_items.product_condition_id as "pcid"
        , order_items.ordered_at_et::date as "order_date"
        , seller_orders.is_direct_order as "is_direct"
        , order_items.quantity as "product_qty"
        , order_items.unit_price_usd as "sold_price"

    from analytics.core.order_items
        inner join analytics.core.seller_orders on analytics.core.seller_orders.id = analytics.core.order_items.seller_order_id
        left outer join analytics.core.sellers on seller_orders.seller_id = sellers.id

    where
        order_items.ordered_at_et::date >= cast(dateadd(dd, -90, getdate()) as date)
""")

cursor = snowflake_pull.cursor()
cursor.execute(orders_sql)
orders_df = cursor.fetch_pandas_all()
orders_df.drop(orders_df.filter(like='Unnamed'), axis=1, inplace=True)

orders_df["combined"] = orders_df['order_date'].astype(str) + '~' + orders_df['pcid'].astype(str)
direct_orders_df = orders_df.copy()
direct_orders_df = direct_orders_df.loc[direct_orders_df['is_direct'] == True]

##Aggragate qtys
platform_qty_per_pcid_per_day = orders_df.groupby('combined')['product_qty'].sum()
orders_df = pd.merge(orders_df, platform_qty_per_pcid_per_day, how='right', on='combined')
orders_df.rename(columns={'product_qty_x':'product_qty', 'product_qty_y':'platform_qty_per_pcid_per_day'}, inplace=True)

avg_platform_orders_per_day = orders_df.groupby('pcid')['platform_qty_per_pcid_per_day'].mean()
orders_df = pd.merge(orders_df, avg_platform_orders_per_day, how='right', on='pcid')
orders_df.rename(columns={'platform_qty_per_pcid_per_day_x':'platform_qty_per_pcid_per_day', 'platform_qty_per_pcid_per_day_y':'platform_avg_qty_per_pcid_per_day'}, inplace=True)

max_platform_orders_per_day = orders_df.groupby('pcid')['platform_qty_per_pcid_per_day'].max()
orders_df = pd.merge(orders_df, max_platform_orders_per_day, how='right', on='pcid')
orders_df.rename(columns={'platform_qty_per_pcid_per_day_x':'platform_qty_per_pcid_per_day', 'platform_qty_per_pcid_per_day_y':'platform_max_qty_per_pcid_per_day'}, inplace=True)


direct_qty_per_pcid_per_day = direct_orders_df.groupby('combined')['product_qty'].sum()
direct_orders_df = pd.merge(direct_orders_df, direct_qty_per_pcid_per_day, how='right', on='combined')
direct_orders_df.rename(columns={'product_qty_x':'product_qty', 'product_qty_y':'direct_qty_per_pcid_per_day'}, inplace=True)

avg_direct_orders_per_day = direct_orders_df.groupby('pcid')['direct_qty_per_pcid_per_day'].mean()
direct_orders_df = pd.merge(direct_orders_df, avg_direct_orders_per_day, how='right', on='pcid')
direct_orders_df.rename(columns={'direct_qty_per_pcid_per_day_x':'direct_qty_per_pcid_per_day', 'direct_qty_per_pcid_per_day_y':'direct_avg_qty_per_pcid_per_day'}, inplace=True)

max_direct_orders_per_day = direct_orders_df.groupby('pcid')['direct_qty_per_pcid_per_day'].max()
direct_orders_df = pd.merge(direct_orders_df, max_direct_orders_per_day, how='right', on='pcid')
direct_orders_df.rename(columns={'direct_qty_per_pcid_per_day_x':'direct_qty_per_pcid_per_day', 'direct_qty_per_pcid_per_day_y':'direct_max_qty_per_pcid_per_day'}, inplace=True)


orders_df.drop_duplicates(subset='pcid', inplace=True)
direct_orders_df.drop_duplicates(subset='pcid', inplace=True)


orders_df = orders_df[['pcid', 'platform_avg_qty_per_pcid_per_day', 'platform_max_qty_per_pcid_per_day']]
direct_orders_df = direct_orders_df[['pcid', 'direct_avg_qty_per_pcid_per_day', 'direct_max_qty_per_pcid_per_day']]

##Merge with pcid frame
pcid_df = pd.merge(pcid_df, orders_df, how='left', on='pcid')
pcid_df = pd.merge(pcid_df, direct_orders_df, how='left', on='pcid')

pcid_df['platform_avg_qty_per_pcid_per_day'] = pcid_df['platform_avg_qty_per_pcid_per_day'].apply(pd.to_numeric, errors='coerce').fillna(0)
pcid_df['platform_max_qty_per_pcid_per_day'] = pcid_df['platform_max_qty_per_pcid_per_day'].apply(pd.to_numeric, errors='coerce').fillna(0)
#pcid_df['avg_sold_price'] = pcid_df['avg_sold_price'].apply(pd.to_numeric, errors='coerce').fillna(0)

pcid_df['direct_avg_qty_per_pcid_per_day'] = pcid_df['direct_avg_qty_per_pcid_per_day'].apply(pd.to_numeric, errors='coerce').fillna(0)
pcid_df['direct_max_qty_per_pcid_per_day'] = pcid_df['direct_max_qty_per_pcid_per_day'].apply(pd.to_numeric, errors='coerce').fillna(0)
#pcid_df['avg_direct_sold_price'] = pcid_df['avg_direct_sold_price'].apply(pd.to_numeric, errors='coerce').fillna(0)

pcid_df = pcid_df.loc[pcid_df['platform_avg_qty_per_pcid_per_day'] > 0]

##Math
pcid_df["safety_stock"] = ((pcid_df['platform_max_qty_per_pcid_per_day'].astype('float64') * pcid_df['max_total_lead_time'].astype('float64')) - (pcid_df['platform_avg_qty_per_pcid_per_day'].astype('float64') * pcid_df['avg_total_lead_time'].astype('float64'))) / 2

pcid_df["desired_stock_level"] = (direct_share * pcid_df['platform_avg_qty_per_pcid_per_day'].astype('float64') * pcid_df['avg_total_lead_time'].astype('float64')) + pcid_df['safety_stock'].astype('float64')

pcid_df.sort_values(by=['pcid'], ascending=[True], inplace=True)

pcid_df["stock_level_delta"] = pcid_df['desired_stock_level'].astype('float64') - pcid_df['avg_qty_on_hand'].astype('float64')

pcid_df["stock_level_delta_%"] = 0.0

pcid_df.loc[pcid_df['desired_stock_level'].astype('float64') != 0, 'stock_level_delta_%'] =  pcid_df['stock_level_delta'].astype('float64') / pcid_df['desired_stock_level'].astype('float64')

##Make final dataframe
pcid_df = pcid_df[['pcid', 'stock_level_delta', 'stock_level_delta_%', 'desired_stock_level', 'avg_qty_on_hand', 'platform_avg_qty_per_pcid_per_day', 'platform_max_qty_per_pcid_per_day', 'direct_avg_qty_per_pcid_per_day', 'direct_max_qty_per_pcid_per_day', 'avg_market_price', 'safety_stock']]

pcid_df.rename(columns={'platform_avg_qty_per_pcid_per_day':'Avg ADD (Marketplace)', 'platform_max_qty_per_pcid_per_day':'Max ADD (Marketplace)', 'direct_avg_qty_per_pcid_per_day':'Avg ADD (Direct)', 'direct_max_qty_per_pcid_per_day':'Max ADD (Direct)', 'avg_market_price':'Avg Market Price ($)'}, inplace=True)

decimals = pd.Series([0, 0, 2, 0, 0, 2, 0, 2, 0, 2], index=['pcid', 'stock_level_delta', 'stock_level_delta_%', 'desired_stock_level', 'avg_qty_on_hand', 'Avg ADD (Marketplace)', 'Max ADD (Marketplace)', 'Avg ADD (Direct)', 'Max ADD (Direct)', 'Avg Market Price ($)'])

pcid_df.round(decimals)

##Write data to sheet
report_tab = gc.open_by_key('1m2aIUUpQ7dWpuosuFdO2xjsPM5k-2hNviWcI7S3iiwc').worksheet('Data')
report_tab.clear()

pcid_df_2 = pcid_df.copy()

pcid_df_2 = pcid_df_2[['Avg ADD (Marketplace)', 'Max ADD (Marketplace)', 'Avg ADD (Direct)', 'Max ADD (Direct)', 'Avg Market Price ($)']]
pcid_df = pcid_df[['pcid', 'stock_level_delta', 'stock_level_delta_%', 'desired_stock_level', 'avg_qty_on_hand']]

gd.set_with_dataframe(report_tab, pcid_df, row=1, col=1)

time.sleep(15)

gd.set_with_dataframe(report_tab, pcid_df_2, row=1, col=6)





