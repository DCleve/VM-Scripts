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

##Import RI stocking information, market price and last sale information
ri_sql = ("""
with
    max_date as (
      select
        distinct order_items.product_condition_id as pcid
        , max(reimbursement_invoices.shelved_at_et::date) as newest_shelved_date

      from analytics.core.reimbursement_invoices
        left outer join hvr_tcgstore_production.tcgd.ReimOrderSellerOrderProduct on hvr_tcgstore_production.tcgd.ReimOrderSellerOrderProduct.ReimOrderId = analytics.core.reimbursement_invoices.id
        left outer join analytics.core.order_items on analytics.core.order_items.id = hvr_tcgstore_production.tcgd.ReimOrderSellerOrderProduct.SellerOrderProductId and hvr_tcgstore_production.tcgd.ReimOrderSellerOrderProduct.hvr_deleted = 0
        inner join analytics.core.seller_orders on analytics.core.seller_orders.id = analytics.core.order_items.seller_order_id
        inner join analytics.core.orders on analytics.core.orders.id = analytics.core.seller_orders.order_id
        left outer join analytics.core.refunds on seller_orders.id = refunds.seller_order_id
        left outer join analytics.core.sellers on seller_orders.seller_id = sellers.id

      where
        reimbursement_invoices.shelved_at_et::date >= cast(dateadd(dd, -365, getdate()) as date)
        and seller_orders.is_direct_order = true
        and seller_orders.seller_id != 249
        and sellers.is_store_your_products = false
        and reimbursement_invoices.shelved_at_et::date is not null

      group by 1
      )

      , all_dates as (
          select
            order_items.product_condition_id as pcid
            , reimbursement_invoices.shelved_at_et::date as shelved_date
            , order_items.total_usd as sold_product_amount
            , order_items.quantity as sold_product_qty

          from analytics.core.reimbursement_invoices
            left outer join hvr_tcgstore_production.tcgd.ReimOrderSellerOrderProduct on hvr_tcgstore_production.tcgd.ReimOrderSellerOrderProduct.ReimOrderId = analytics.core.reimbursement_invoices.id
            left outer join analytics.core.order_items on analytics.core.order_items.id = hvr_tcgstore_production.tcgd.ReimOrderSellerOrderProduct.SellerOrderProductId and hvr_tcgstore_production.tcgd.ReimOrderSellerOrderProduct.hvr_deleted = 0
            inner join analytics.core.seller_orders on analytics.core.seller_orders.id = analytics.core.order_items.seller_order_id
            inner join analytics.core.orders on analytics.core.orders.id = analytics.core.seller_orders.order_id
            left outer join analytics.core.refunds on seller_orders.id = refunds.seller_order_id
            left outer join analytics.core.sellers on seller_orders.seller_id = sellers.id

          where
            reimbursement_invoices.shelved_at_et::date >= cast(dateadd(dd, -365, getdate()) as date)
            and seller_orders.is_direct_order = true
            and seller_orders.seller_id != 249
            and sellers.is_store_your_products = false
            and reimbursement_invoices.shelved_at_et::date is not null
        )

        , market_price as (
            select
              distinct daily_sku_market_prices.sku as pcid
              , max(daily_sku_market_prices.market_price_usd) as market_price

            from analytics.core.daily_sku_market_prices

            where
              daily_sku_market_prices.date::date >= cast(dateadd(dd, -365, getdate()) as date)
              and daily_sku_market_prices.market_price_usd > 0

            group by 1
          )

        , last_direct_sale as (
            select
                distinct order_items.product_condition_id as pcid
                , max(orders.ordered_at_et) as order_timestamp

            from analytics.core.orders
                inner join analytics.core.order_items on orders.id = order_items.order_id
                inner join analytics.core.seller_orders on order_items.seller_order_id = seller_orders.id

            where
                orders.ordered_at_et::date >= cast(dateadd(dd, -365, getdate()) as date)
                and seller_orders.is_direct_order = True

            group by 1
        )

        , last_non_direct_sale as (
            select
                distinct order_items.product_condition_id as pcid
                , max(orders.ordered_at_et) as order_timestamp

            from analytics.core.orders
                inner join analytics.core.order_items on orders.id = order_items.order_id
                inner join analytics.core.seller_orders on order_items.seller_order_id = seller_orders.id

            where
                orders.ordered_at_et::date >= cast(dateadd(dd, -365, getdate()) as date)
                and seller_orders.is_direct_order = False

            group by 1
        )


        select
            all_dates.pcid as "pcid"
            , all_dates.shelved_date as "shelved_date"
            , all_dates.sold_product_amount as "Sold Product Amount USD"
            , all_dates.sold_product_qty as "Sold Product Quantity"
            , max_date.newest_shelved_date as "newest_shelved_date"
            , market_price.market_price as "most_recent_market_price (last 180)"
            , last_direct_sale.order_timestamp as "direct_order_timestamp"
            , last_non_direct_sale.order_timestamp as "non_direct_order_timestamp"

        from all_dates
            left outer join max_date on all_dates.pcid = max_date.pcid
            left outer join market_price on all_dates.pcid = market_price.pcid
            left outer join last_direct_sale on all_dates.pcid = last_direct_sale.pcid
            left outer join last_non_direct_sale on all_dates.pcid = last_non_direct_sale.pcid

""")

cursor = snowflake_pull.cursor()
cursor.execute(ri_sql)
ri_df = cursor.fetch_pandas_all()
ri_df.drop(ri_df.filter(like='Unnamed'), axis=1, inplace=True)

##Calculate days since events
ri_df['newest_shelved_date'] = pd.to_datetime(ri_df['newest_shelved_date'])
ri_df['direct_order_timestamp'] = pd.to_datetime(ri_df['direct_order_timestamp'])
ri_df['non_direct_order_timestamp'] = pd.to_datetime(ri_df['non_direct_order_timestamp'])

ri_df["now_max"] = pd.Timestamp.now()
ri_df['now_max'] = pd.to_datetime(ri_df['now_max'])

ri_df["days_since_last_shelving_event"] = (ri_df['now_max'] - ri_df['newest_shelved_date']).dt.days

ri_df["days_since_last_direct_sale"] = (ri_df['now_max'] - ri_df['direct_order_timestamp']).dt.days

ri_df["days_since_last_non_direct_sale"] = (ri_df['now_max'] - ri_df['non_direct_order_timestamp']).dt.days

##Calculate day intervals
ri_df['shelved_date'] = pd.to_datetime(ri_df['shelved_date'], format='%m-%d-%Y').dt.date

ri_df["now"] = pd.Timestamp.now()
ri_df['now'] = ri_df['now'].dt.date

ri_df["30_days"] = ri_df['now'] - timedelta(days = 30)
ri_df["60_days"] = ri_df['now'] - timedelta(days = 60)
ri_df["90_days"] = ri_df['now'] - timedelta(days = 90)
ri_df["180_days"] = ri_df['now'] - timedelta(days = 180)
ri_df["365_days"] = ri_df['now'] - timedelta(days = 365)

ri_df["usd_0_to_30"] = 0.0
ri_df["usd_30_to_60"] = 0.0
ri_df["usd_60_to_90"] = 0.0
ri_df["usd_90_to_180"] = 0.0
ri_df["usd_180_to_365"] = 0.0
ri_df["usd_365_plus"] = 0.0

ri_df["qty_0_to_30"] = 0.0
ri_df["qty_30_to_60"] = 0.0
ri_df["qty_60_to_90"] = 0.0
ri_df["qty_90_to_180"] = 0.0
ri_df["qty_180_to_365"] = 0.0
ri_df["qty_365_plus"] = 0.0


ri_df.loc[(ri_df['shelved_date'] > ri_df['30_days']) & (ri_df['shelved_date'] <= ri_df['now']), 'usd_0_to_30'] = ri_df['Sold Product Amount USD']
ri_df.loc[(ri_df['shelved_date'] > ri_df['60_days']) & (ri_df['shelved_date'] <= ri_df['30_days']), 'usd_30_to_60'] = ri_df['Sold Product Amount USD']
ri_df.loc[(ri_df['shelved_date'] > ri_df['90_days']) & (ri_df['shelved_date'] <= ri_df['60_days']), 'usd_60_to_90'] = ri_df['Sold Product Amount USD']
ri_df.loc[(ri_df['shelved_date'] > ri_df['180_days']) & (ri_df['shelved_date'] <= ri_df['90_days']), 'usd_90_to_180'] = ri_df['Sold Product Amount USD']
ri_df.loc[(ri_df['shelved_date'] > ri_df['365_days']) & (ri_df['shelved_date'] <= ri_df['180_days']), 'usd_180_to_365'] = ri_df['Sold Product Amount USD']
#ri_df.loc[ri_df['shelved_date'] <= ri_df['365_days'], 'usd_365_plus'] = ri_df['Sold Product Amount USD']

ri_df.loc[(ri_df['shelved_date'] > ri_df['30_days']) & (ri_df['shelved_date'] <= ri_df['now']), 'qty_0_to_30'] = ri_df['Sold Product Quantity']
ri_df.loc[(ri_df['shelved_date'] > ri_df['60_days']) & (ri_df['shelved_date'] <= ri_df['30_days']), 'qty_30_to_60'] = ri_df['Sold Product Quantity']
ri_df.loc[(ri_df['shelved_date'] > ri_df['90_days']) & (ri_df['shelved_date'] <= ri_df['60_days']), 'qty_60_to_90'] = ri_df['Sold Product Quantity']
ri_df.loc[(ri_df['shelved_date'] > ri_df['180_days']) & (ri_df['shelved_date'] <= ri_df['90_days']), 'qty_90_to_180'] = ri_df['Sold Product Quantity']
ri_df.loc[(ri_df['shelved_date'] > ri_df['365_days']) & (ri_df['shelved_date'] <= ri_df['180_days']), 'qty_180_to_365'] = ri_df['Sold Product Quantity']
#ri_df.loc[ri_df['shelved_date'] <= ri_df['365_days'], 'qty_365_plus'] = ri_df['Sold Product Quantity']

##Aggragate per PCID
usd_per_pcid_0_to_30 = ri_df.groupby('pcid')['usd_0_to_30'].sum()
ri_df = pd.merge(ri_df, usd_per_pcid_0_to_30, how='right', on='pcid')
ri_df.rename(columns={'usd_0_to_30_y':'usd_total_0_to_30'}, inplace=True)
ri_df.drop('usd_0_to_30_x', axis=1, inplace=True)

usd_per_pcid_30_to_60 = ri_df.groupby('pcid')['usd_30_to_60'].sum()
ri_df = pd.merge(ri_df, usd_per_pcid_30_to_60, how='right', on='pcid')
ri_df.rename(columns={'usd_30_to_60_y':'usd_total_30_to_60'}, inplace=True)
ri_df.drop('usd_30_to_60_x', axis=1, inplace=True)

usd_per_pcid_60_to_90 = ri_df.groupby('pcid')['usd_60_to_90'].sum()
ri_df = pd.merge(ri_df, usd_per_pcid_60_to_90, how='right', on='pcid')
ri_df.rename(columns={'usd_60_to_90_y':'usd_total_60_to_90'}, inplace=True)
ri_df.drop('usd_60_to_90_x', axis=1, inplace=True)

usd_per_pcid_90_to_180 = ri_df.groupby('pcid')['usd_90_to_180'].sum()
ri_df = pd.merge(ri_df, usd_per_pcid_90_to_180, how='right', on='pcid')
ri_df.rename(columns={'usd_90_to_180_y':'usd_total_90_to_180'}, inplace=True)
ri_df.drop('usd_90_to_180_x', axis=1, inplace=True)

usd_per_pcid_180_to_365 = ri_df.groupby('pcid')['usd_180_to_365'].sum()
ri_df = pd.merge(ri_df, usd_per_pcid_180_to_365, how='right', on='pcid')
ri_df.rename(columns={'usd_180_to_365_y':'usd_total_180_to_365'}, inplace=True)
ri_df.drop('usd_180_to_365_x', axis=1, inplace=True)

#usd_per_pcid_365_plus = ri_df.groupby('pcid')['usd_365_plus'].sum()
#ri_df = pd.merge(ri_df, usd_per_pcid_365_plus, how='right', on='pcid')
#ri_df.rename(columns={'usd_365_plus_y':'usd_total_365_plus'}, inplace=True)
#ri_df.drop('usd_365_plus_x', axis=1, inplace=True)


qty_per_pcid_0_to_30 = ri_df.groupby('pcid')['qty_0_to_30'].sum()
ri_df = pd.merge(ri_df, qty_per_pcid_0_to_30, how='right', on='pcid')
ri_df.rename(columns={'qty_0_to_30_y':'qty_total_0_to_30'}, inplace=True)
ri_df.drop('qty_0_to_30_x', axis=1, inplace=True)

qty_per_pcid_30_to_60 = ri_df.groupby('pcid')['qty_30_to_60'].sum()
ri_df = pd.merge(ri_df, qty_per_pcid_30_to_60, how='right', on='pcid')
ri_df.rename(columns={'qty_30_to_60_y':'qty_total_30_to_60'}, inplace=True)
ri_df.drop('qty_30_to_60_x', axis=1, inplace=True)

qty_per_pcid_60_to_90 = ri_df.groupby('pcid')['qty_60_to_90'].sum()
ri_df = pd.merge(ri_df, qty_per_pcid_60_to_90, how='right', on='pcid')
ri_df.rename(columns={'qty_60_to_90_y':'qty_total_60_to_90'}, inplace=True)
ri_df.drop('qty_60_to_90_x', axis=1, inplace=True)

qty_per_pcid_90_to_180 = ri_df.groupby('pcid')['qty_90_to_180'].sum()
ri_df = pd.merge(ri_df, qty_per_pcid_90_to_180, how='right', on='pcid')
ri_df.rename(columns={'qty_90_to_180_y':'qty_total_90_to_180'}, inplace=True)
ri_df.drop('qty_90_to_180_x', axis=1, inplace=True)

qty_per_pcid_180_to_365 = ri_df.groupby('pcid')['qty_180_to_365'].sum()
ri_df = pd.merge(ri_df, qty_per_pcid_180_to_365, how='right', on='pcid')
ri_df.rename(columns={'qty_180_to_365_y':'qty_total_180_to_365'}, inplace=True)
ri_df.drop('qty_180_to_365_x', axis=1, inplace=True)

#qty_per_pcid_365_plus = ri_df.groupby('pcid')['qty_365_plus'].sum()
#ri_df = pd.merge(ri_df, qty_per_pcid_365_plus, how='right', on='pcid')
#ri_df.rename(columns={'qty_365_plus_y':'qty_total_365_plus'}, inplace=True)
#ri_df.drop('qty_365_plus_x', axis=1, inplace=True)

##Parse down dataframe
ri_df.drop_duplicates(subset='pcid', inplace=True)

##Calculate ACVs
ri_df["acv_0_to_30"] = 0.0
ri_df["acv_30_to_60"] = 0.0
ri_df["acv_60_to_90"] = 0.0
ri_df["acv_90_to_180"] = 0.0
ri_df["acv_180_to_365"] = 0.0
#ri_df["acv_365_plus"] = 0.0

ri_df.loc[ri_df['usd_total_0_to_30'].astype('float64') > 0, 'acv_0_to_30'] =  ri_df['usd_total_0_to_30'].astype('float64') / ri_df['qty_total_0_to_30'].astype('float64')
ri_df.loc[ri_df['usd_total_30_to_60'].astype('float64') > 0, 'acv_30_to_60'] =  ri_df['usd_total_30_to_60'].astype('float64') / ri_df['qty_total_30_to_60'].astype('float64')
ri_df.loc[ri_df['usd_total_60_to_90'].astype('float64') > 0, 'acv_60_to_90'] =  ri_df['usd_total_60_to_90'].astype('float64') / ri_df['qty_total_60_to_90'].astype('float64')
ri_df.loc[ri_df['usd_total_90_to_180'].astype('float64') > 0, 'acv_90_to_180'] =  ri_df['usd_total_90_to_180'].astype('float64') / ri_df['qty_total_90_to_180'].astype('float64')
ri_df.loc[ri_df['usd_total_180_to_365'].astype('float64') > 0, 'acv_180_to_365'] =  ri_df['usd_total_180_to_365'].astype('float64') / ri_df['qty_total_180_to_365'].astype('float64')
#ri_df.loc[ri_df['usd_total_365_plus'].astype('float64') > 0, 'acv_365_plus'] =  ri_df['qty_total_365_plus'].astype('float64') / ri_df['usd_total_365_plus'].astype('float64')

##Import current on hand info
sql_inv = ("""
with cte as (
    select
        distinct direct_inventory_history.product_condition_id as pcid
        , direct_inventory_history.quantity_available as current_qty_on_hand
        , direct_inventory_history.valid_from_et as timestamp
        , Row_Number() over (partition by pcid order by timestamp desc)  RN
    from analytics.core.direct_inventory_history

    where
      direct_inventory_history.valid_from_et::date >= cast(dateadd(dd, -365, getdate()) as date)
      and (direct_inventory_history.product_line = 'Magic' or direct_inventory_history.product_line = 'Pokemon' or direct_inventory_history.product_line = 'YuGiOh')
  )

  select
    cte.pcid as "pcid"
    , cte.current_qty_on_hand as "current_qty_on_hand"

  from cte

  where RN <= 1
""")

cursor = snowflake_pull.cursor()
cursor.execute(sql_inv)
inv_df = cursor.fetch_pandas_all()
inv_df.drop(inv_df.filter(like='Unnamed'), axis=1, inplace=True)

##Combine current on hand frame
ri_df = pd.merge(ri_df, inv_df, how='left', on='pcid')

##Write data to sheet
report_tab = gc.open_by_key('17L8fhu76VuaGDYqBKnnu11kEGbLgdMCBkMJcwoiMi7c').worksheet('Data')

ri_df.sort_values(by=['pcid'], ascending=[True], inplace=True)

ri_df = ri_df [['pcid', 'days_since_last_shelving_event', 'current_qty_on_hand', 'days_since_last_direct_sale', 'days_since_last_non_direct_sale', 'most_recent_market_price (last 180)', 'qty_total_0_to_30', 'acv_0_to_30', 'qty_total_30_to_60', 'acv_30_to_60', 'qty_total_60_to_90', 'acv_60_to_90', 'qty_total_90_to_180', 'acv_90_to_180','qty_total_180_to_365','acv_180_to_365']]


decimals = pd.Series([0, 0, 0, 0, 0, 2, 0, 2, 0, 2, 0, 2, 0, 2, 0, 2], index=['pcid', 'days_since_last_shelving_event', 'current_qty_on_hand', 'days_since_last_direct_sale', 'days_since_last_non_direct_sale', 'most_recent_market_price (last 180)', 'qty_total_0_to_30', 'acv_0_to_30', 'qty_total_30_to_60', 'acv_30_to_60', 'qty_total_60_to_90', 'acv_60_to_90', 'qty_total_90_to_180', 'acv_90_to_180','qty_total_180_to_365','acv_180_to_365'])
ri_df.round(decimals)

ri_df_2 = ri_df.copy()

ri_df = ri_df[['pcid', 'days_since_last_shelving_event', 'current_qty_on_hand', 'days_since_last_direct_sale', 'days_since_last_non_direct_sale', 'most_recent_market_price (last 180)', 'qty_total_0_to_30', 'acv_0_to_30']]
ri_df_2 = ri_df_2[['qty_total_30_to_60', 'acv_30_to_60', 'qty_total_60_to_90', 'acv_60_to_90', 'qty_total_90_to_180', 'acv_90_to_180', 'qty_total_180_to_365', 'acv_180_to_365']]

report_tab.clear()

gd.set_with_dataframe(report_tab, ri_df, row=1, col=1)

time.sleep(30)

gd.set_with_dataframe(report_tab, ri_df_2, row=1, col=9)
