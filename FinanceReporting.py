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

##Import RI Data
sql_ri = ("""
with
    ri_whole as (
      select
        seller_orders.order_number as seller_order_number
        , order_items.product_condition_id as pcid
        , order_items.total_usd as "Sold Product Amount USD"
        , reimbursement_invoices.reimbursement_invoice_number as ri_number
        , concat(pcid, ri_number) as identifier
        , seller_orders.seller_paid_date::date as "Seller Paid Date"
        , order_items.quantity as "Sold Product Quantity"
        , reimbursement_invoices.received_at_et::date as "receipt_date"
        , reimbursement_invoices.processing_ended_at_et::date as "processing_date"
        , reimbursement_invoices.shelved_at_et::date as "shelving_date"
        , case
          when seller_orders.seller_paid_date is null then 'Payment to be Scheduled'
          when (seller_orders.seller_paid_date <= reimbursement_invoices.created_at_et) or (reimbursement_invoices.created_at_et is null) then 'Before RI Creation'
          when (seller_orders.seller_paid_date < reimbursement_invoices.received_at_et) or (reimbursement_invoices.received_at_et is null) then 'Before RI Receipt'
          when (seller_orders.seller_paid_date < reimbursement_invoices.processing_ended_at_et) or (reimbursement_invoices.processing_ended_at_et is null) then 'Before RI Processing'
          when seller_orders.seller_paid_date is not null then 'After RI Finalization'
          else null
        end as "When Was Seller Paid?"
        , reimbursement_invoices.was_marked_missing as "was_marked_missing"

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
        and sellers.is_store_your_products = false
        and len(reimbursement_invoices.reimbursement_invoice_number) = 26
        and split_part(reimbursement_invoices.reimbursement_invoice_number, '-', 3) in('PRF', 'STD')
    )

    , ri_discreps as (
      select
        reimbursement_invoice_products.product_condition_id as pcid
        , reimbursement_invoice_products.total_discrepancies as total_discreps
        , reimbursement_invoice_products.reimbursement_invoice_number as ri_number
        , concat(pcid, ri_number) as identifier

      from analytics.core.reimbursement_invoice_products
        inner join analytics.core.reimbursement_invoices on reimbursement_invoice_products.reimbursement_invoice_number = reimbursement_invoices.reimbursement_invoice_number
        inner join analytics.core.sellers on sellers.id = reimbursement_invoices.seller_id

      where
         split_part(reimbursement_invoice_products.reimbursement_invoice_number, '-', 3) in('PRF', 'STD')
         and len(reimbursement_invoice_products.reimbursement_invoice_number) = 26
         and sellers.id != 249
         and sellers.is_store_your_products = false
    )

    select
        ri_whole.*
        , ri_discreps.total_discreps as "Total Discreps"

    from ri_whole
        left outer join ri_discreps on ri_discreps.identifier = ri_whole.identifier

""")

cursor = snowflake_pull.cursor()
cursor.execute(sql_ri)

ri_df = cursor.fetch_pandas_all()
ri_df.drop(ri_df.filter(like='Unnamed'), axis=1, inplace=True)

ri_df = ri_df[['Sold Product Amount USD', 'When Was Seller Paid?', 'Seller Paid Date', 'Sold Product Quantity', 'Total Discreps', 'receipt_date', 'processing_date', 'shelving_date', 'was_marked_missing']]

ri_df["lost_to_discreps"] = ri_df['Sold Product Amount USD'].astype('float64') * ri_df['Total Discreps'].astype('float64')

ri_df["cards_awaiting_receipt"] = 0.0
ri_df["value_awaiting_receipt"] = 0.0

ri_df["cards_awaiting_processing"] = 0.0
ri_df["value_awaiting_processing"] = 0.0

ri_df["cards_awaiting_shelving"] = 0.0
ri_df["value_awaiting_shelving"] = 0.0

ri_df.loc[(ri_df['When Was Seller Paid?'] == 'Before RI Creation') & (pd.isnull(ri_df['receipt_date']) == True) & (ri_df['was_marked_missing'] == False), 'cards_awaiting_receipt'] = ri_df['Sold Product Quantity']
ri_df.loc[(ri_df['When Was Seller Paid?'] == 'Before RI Creation') & (pd.isnull(ri_df['receipt_date']) == True) & (ri_df['was_marked_missing'] == False), 'value_awaiting_receipt'] = ri_df['Sold Product Amount USD']

ri_df.loc[(ri_df['When Was Seller Paid?'] == 'Before RI Receipt') & (pd.isnull(ri_df['processing_date']) == True) & (ri_df['was_marked_missing'] == False), 'cards_awaiting_processing'] = ri_df['Sold Product Quantity']
ri_df.loc[(ri_df['When Was Seller Paid?'] == 'Before RI Receipt') & (pd.isnull(ri_df['processing_date']) == True) & (ri_df['was_marked_missing'] == False), 'value_awaiting_processing'] = ri_df['Sold Product Amount USD']

ri_df.loc[(ri_df['When Was Seller Paid?'] == 'Before RI Processing') & (pd.isnull(ri_df['shelving_date']) == True) & (ri_df['was_marked_missing'] == False), 'cards_awaiting_shelving'] = ri_df['Sold Product Quantity']
ri_df.loc[(ri_df['When Was Seller Paid?'] == 'Before RI Processing') & (pd.isnull(ri_df['shelving_date']) == True) & (ri_df['was_marked_missing'] == False), 'value_awaiting_shelving'] = ri_df['Sold Product Amount USD']

##Aggragate data
ri_df["combined"] = ri_df['Seller Paid Date'].astype(str) + ri_df['When Was Seller Paid?'].astype(str)

dollar_amount = ri_df.groupby('combined')['Sold Product Amount USD'].sum()
ri_df = pd.merge(ri_df, dollar_amount, how='right', on='combined')
ri_df.rename(columns={'Sold Product Amount USD_x':'Sold Product Amount USD', 'Sold Product Amount USD_y':'dollar_amount_per_day'}, inplace=True)

product_quantity = ri_df.groupby('combined')['Sold Product Quantity'].sum()
ri_df = pd.merge(ri_df, product_quantity, how='right', on='combined')
ri_df.rename(columns={'Sold Product Quantity_x':'Sold Product Quantity', 'Sold Product Quantity_y':'product_amount_per_day'}, inplace=True)

lost_to_discreps = ri_df.groupby('combined')['lost_to_discreps'].sum()
ri_df = pd.merge(ri_df, lost_to_discreps, how='right', on='combined')
ri_df.rename(columns={'lost_to_discreps_x':'lost_to_discreps', 'lost_to_discreps_y':'dollars_recouped_from_discreps_per_day_per_category'}, inplace=True)

total_discreps = ri_df.groupby('combined')['Total Discreps'].sum()
ri_df = pd.merge(ri_df, total_discreps, how='right', on='combined')
ri_df.rename(columns={'Total Discreps_x':'Total Discreps', 'Total Discreps_y':'total_discreps_per_day_per_category'}, inplace=True)

cards_awaiting_receipt = ri_df.groupby('combined')['cards_awaiting_receipt'].sum()
ri_df = pd.merge(ri_df, cards_awaiting_receipt, how='right', on='combined')
ri_df.rename(columns={'cards_awaiting_receipt_x':'cards_awaiting_receipt', 'cards_awaiting_receipt_y':'total_cards_awaiting_receipt'}, inplace=True)

value_awaiting_receipt = ri_df.groupby('combined')['value_awaiting_receipt'].sum()
ri_df = pd.merge(ri_df, value_awaiting_receipt, how='right', on='combined')
ri_df.rename(columns={'value_awaiting_receipt_x':'value_awaiting_receipt', 'value_awaiting_receipt_y':'total_value_awaiting_receipt'}, inplace=True)

cards_awaiting_processing = ri_df.groupby('combined')['cards_awaiting_processing'].sum()
ri_df = pd.merge(ri_df, cards_awaiting_processing, how='right', on='combined')
ri_df.rename(columns={'cards_awaiting_processing_x':'cards_awaiting_processing', 'cards_awaiting_processing_y':'total_cards_awaiting_processing'}, inplace=True)

value_awaiting_processing = ri_df.groupby('combined')['value_awaiting_processing'].sum()
ri_df = pd.merge(ri_df, value_awaiting_processing, how='right', on='combined')
ri_df.rename(columns={'value_awaiting_processing_x':'value_awaiting_processing', 'value_awaiting_processing_y':'total_value_awaiting_processing'}, inplace=True)

cards_awaiting_shelving = ri_df.groupby('combined')['cards_awaiting_shelving'].sum()
ri_df = pd.merge(ri_df, cards_awaiting_shelving, how='right', on='combined')
ri_df.rename(columns={'cards_awaiting_shelving_x':'cards_awaiting_shelving', 'cards_awaiting_shelving_y':'total_cards_awaiting_shelving'}, inplace=True)

value_awaiting_shelving = ri_df.groupby('combined')['value_awaiting_shelving'].sum()
ri_df = pd.merge(ri_df, value_awaiting_shelving, how='right', on='combined')
ri_df.rename(columns={'value_awaiting_shelving_x':'value_awaiting_shelving', 'value_awaiting_shelving_y':'total_value_awaiting_shelving'}, inplace=True)

ri_df.drop_duplicates(subset='combined', inplace=True)

ri_df = ri_df[['Seller Paid Date', 'When Was Seller Paid?', 'dollar_amount_per_day', 'product_amount_per_day', 'total_discreps_per_day_per_category', 'dollars_recouped_from_discreps_per_day_per_category', 'total_cards_awaiting_receipt', 'total_value_awaiting_receipt', 'total_cards_awaiting_processing', 'total_value_awaiting_processing', 'total_cards_awaiting_shelving', 'total_value_awaiting_shelving']]

##Write Data to sheet
finTab = gc.open_by_key('10uGqyQKL8Igby5hFa6fH9sfY4t_eqztK3qodLMJZrrU').worksheet('Data')
finTab.clear()
gd.set_with_dataframe(finTab, ri_df)