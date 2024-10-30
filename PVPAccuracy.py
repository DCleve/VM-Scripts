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

pvpDoc = gc.open_by_key('1TcNVdMuxdCJ1d7SdP7fF-daqbQydOfXmZO6OICXYTFM')
pvpDataTab = pvpDoc.worksheet('Data')

##Import Staffing Data
staffing = gc.open_by_key('1sBVK5vjiB72JuKePpht2R4vZ4ShxuZKjQreE8FE7068').worksheet('Current Staff')
staffing_df = pd.DataFrame.from_dict(staffing.get_all_records())
staffing_df.dropna(subset=["Preferred Name"], inplace=True)
staffing_df.drop(staffing_df.filter(like='Unnamed'), axis=1, inplace=True)
staffing_df = staffing_df[['Preferred Name', 'Email', 'Shift Name', 'Start Date', 'Supervisor', 'OPs Lead', 'Last, First (Formatting)', 'Role']]

staffing_df.rename(columns={'Preferred Name':'Puncher'}, inplace=True)

staffing_df = staffing_df[['Puncher', 'Start Date', 'Supervisor']]

##Import nuway data
pvp_string = ["C:", "Users", login, "OneDrive - eBay Inc", "AC-Scripting", "Data CSVs", "NuWay", "Data.csv"]
pvp_result = separator.join(pvp_string)
pvp_df = pd.read_csv(pvp_result)

pvp_df["Punch"] = pvp_df['Data'].str.split('|').str[0]
pvp_df["First_Offset"] = pvp_df['Data'].str.split('|').str[1]
pvp_df["Puncher"] = pvp_df['Data'].str.split('|').str[2]
pvp_df["Units"] = pvp_df['Data'].str.split('|').str[3]
pvp_df["Subtask"] = pvp_df['Data'].str.split('|').str[4]
pvp_df["Task"] = pvp_df['Data'].str.split('|').str[7]
pvp_df["Orders"] = pvp_df['Data'].str.split('|').str[14]

pvp_df = pd.merge(pvp_df, staffing_df, how='left', on='Puncher')

pvp_df = pvp_df[['Punch', 'First_Offset', 'Puncher',  'Units', 'Subtask', 'Task', 'Orders', 'Start Date', 'Supervisor']]

pvp_df.loc[pvp_df['Task'] != 'PVP', 'Punch'] = None
pvp_df.dropna(subset=['Punch'], inplace=True)

##Import SQ Data
sq_string = ["C:", "Users", login, "OneDrive - eBay Inc", "AC-Scripting", "Data CSVs", "Snowflake", "SQGameData.csv"]
sq_result = separator.join(sq_string)
sq_df = pd.read_csv(sq_result)

##Merge punch data with sq data
pvp_df = pd.merge(pvp_df, sq_df, how='left', on='Subtask')

pvp_df.dropna(subset=["ORDER_NUMBER"], inplace=True)

##Import ticket data
tix_data_string = ["C:", "Users", login, "OneDrive - eBay Inc", "AC-Scripting", "Data CSVs", "Snowflake", "TixData.csv"]
tix_data_result = separator.join(tix_data_string)
tix_data_df = pd.read_csv(tix_data_result)

##Split tix df
tix_data_df.loc[tix_data_df['TICKET_ORDER_NUMBER_1'] == '0', 'TICKET_ORDER_NUMBER_1'] = None
tix_data_df.loc[tix_data_df['TICKET_ORDER_NUMBER_2'] == '0', 'TICKET_ORDER_NUMBER_2'] = None
tix_data_df.loc[tix_data_df['COMMENT_BODY'] == '0', 'COMMENT_BODY'] = None
tix_data_df.loc[tix_data_df['TITLE'] == '0', 'TITLE'] = None

tix_data_df.loc[tix_data_df['CARD_TAG'] == '0', 'CARD_TAG'] = None
tix_data_df.loc[tix_data_df['QTY_TAG'] == 0, 'QTY_TAG'] = None
tix_data_df.loc[tix_data_df['PKG_TAG'] == '0', 'PKG_TAG'] = None
tix_data_df.loc[tix_data_df['CTF_TAG'] == '0', 'CTF_TAG'] = None
tix_data_df.loc[tix_data_df['CND_TAG'] == '0', 'CND_TAG'] = None

tix_data_1_df = tix_data_df.copy()
tix_data_1_df.dropna(subset=['TICKET_ORDER_NUMBER_1'], inplace=True)
tix_data_1_df = tix_data_1_df[['TIX_ID', 'TICKET_ORDER_NUMBER_1', 'CARD_TAG', 'QTY_TAG', 'PKG_TAG', 'CTF_TAG', 'CND_TAG']]
tix_data_1_df.rename(columns={'TICKET_ORDER_NUMBER_1':'order_number'}, inplace=True)

tix_data_2_df = tix_data_df.copy()
tix_data_2_df.dropna(subset=['TICKET_ORDER_NUMBER_2'], inplace=True)
tix_data_2_df = tix_data_2_df[['TIX_ID', 'TICKET_ORDER_NUMBER_2', 'CARD_TAG', 'QTY_TAG', 'PKG_TAG', 'CTF_TAG', 'CND_TAG']]
tix_data_2_df.rename(columns={'TICKET_ORDER_NUMBER_2':'order_number'}, inplace=True)

tix_data_3_df = tix_data_df.copy()
tix_data_3_df.dropna(subset=['COMMENT_BODY'], inplace=True)
tix_data_3_df = tix_data_3_df[['TIX_ID', 'COMMENT_BODY', 'CARD_TAG', 'QTY_TAG', 'PKG_TAG', 'CTF_TAG', 'CND_TAG']]
tix_data_3_df.rename(columns={'COMMENT_BODY':'order_number'}, inplace=True)

tix_data_4_df = tix_data_df.copy()
tix_data_4_df.dropna(subset=['TITLE'], inplace=True)
tix_data_4_df = tix_data_4_df[['TIX_ID', 'TITLE', 'CARD_TAG', 'QTY_TAG', 'PKG_TAG', 'CTF_TAG', 'CND_TAG']]
tix_data_4_df.rename(columns={'TITLE':'order_number'}, inplace=True)

tix_data_1_df = pd.concat([tix_data_1_df, tix_data_2_df])
tix_data_1_df = pd.concat([tix_data_1_df, tix_data_3_df])
tix_data_1_df = pd.concat([tix_data_1_df, tix_data_4_df])

tix_data_1_df["dupe"] = tix_data_1_df['TIX_ID'].astype(str) + tix_data_1_df['order_number'].astype(str)

tix_data_1_df.drop_duplicates(subset=['dupe'], inplace=True)
tix_data_1_df.drop('dupe', axis=1, inplace=True)


tix_data_1_df.loc[tix_data_1_df['CARD_TAG'] == 'card', 'CARD_TAG'] = 1
tix_data_1_df.loc[tix_data_1_df['QTY_TAG'] == 'qty', 'QTY_TAG'] = 1
tix_data_1_df.loc[tix_data_1_df['PKG_TAG'] == 'pkg', 'PKG_TAG'] = 1
tix_data_1_df.loc[tix_data_1_df['CTF_TAG'] == 'ctf', 'CTF_TAG'] = 1
tix_data_1_df.loc[tix_data_1_df['CND_TAG'] == 'cnd', 'CND_TAG'] = 1

##merge ticket data with sq data
pvp_df.rename(columns={'ORDER_NUMBER':'order_number'}, inplace=True)
pvp_df = pd.merge(pvp_df, tix_data_1_df, how='left', on='order_number')

##ticket types
pvp_df["mtg"] = 0
pvp_df["pkm"] = 0
pvp_df["ygo"] = 0

pvp_df.loc[pvp_df['GAME_NAME'] == 'Magic', 'mtg'] = 1
pvp_df.loc[pvp_df['GAME_NAME'] == 'Pokemon', 'pkm'] = 1
pvp_df.loc[pvp_df['GAME_NAME'] == 'YuGiOh', 'ygo'] = 1

##Month and week
pvp_df['Punch'] = pd.to_datetime(pvp_df['Punch'])
pvp_df["pvp_day_of_week"] = pvp_df['Punch'].dt.dayofweek
pvp_df["pvp_week_of"] = pvp_df['Punch'] - pd.to_timedelta(pvp_df['pvp_day_of_week'], unit='D')

pvp_df["punch_normalized"] = pd.to_datetime(pvp_df['Punch']).dt.date
pvp_df['pvp_week_of'] = pd.to_datetime(pvp_df['pvp_week_of']).dt.date

pvp_df["pvp_month_of"] = pvp_df['punch_normalized'].apply(lambda x: x.strftime('%Y-%m-01'))
pvp_df["pvp_operator_tenure"] = (pvp_df['punch_normalized'] - pd.to_datetime(pvp_df['Start Date']).dt.date)/np.timedelta64(1, 'D')/30

pvp_df['month_count'] = pvp_df['Puncher'].astype(str) + pvp_df['pvp_month_of'].astype(str)
pvp_df['week_count'] = pvp_df['Puncher'].astype(str) + pvp_df['pvp_week_of'].astype(str)

pvp_df["mtg_card_tag"] = None
pvp_df["mtg_qty_tag"] = None
pvp_df["mtg_pkg_tag"] = None
pvp_df["mtg_ctf_tag"] = None
pvp_df["mtg_cnd_tag"] = None

pvp_df["pkm_card_tag"] = None
pvp_df["pkm_qty_tag"] = None
pvp_df["pkm_pkg_tag"] = None
pvp_df["pkm_ctf_tag"] = None
pvp_df["pkm_cnd_tag"] = None

pvp_df["ygo_card_tag"] = None
pvp_df["ygo_qty_tag"] = None
pvp_df["ygo_pkg_tag"] = None
pvp_df["ygo_ctf_tag"] = None
pvp_df["ygo_cnd_tag"] = None


pvp_df.loc[(pvp_df['CARD_TAG'] == 1) & (pvp_df['mtg'] == 1), 'mtg_card_tag'] = 1
pvp_df.loc[(pvp_df['QTY_TAG'] == 1) & (pvp_df['mtg'] == 1), 'mtg_qty_tag'] = 1
pvp_df.loc[(pvp_df['PKG_TAG'] == 1) & (pvp_df['mtg'] == 1), 'mtg_pkg_tag'] = 1
pvp_df.loc[(pvp_df['CTF_TAG'] == 1) & (pvp_df['mtg'] == 1), 'mtg_ctf_tag'] = 1
pvp_df.loc[(pvp_df['CND_TAG'] == 1) & (pvp_df['mtg'] == 1), 'mtg_cnd_tag'] = 1


pvp_df.loc[(pvp_df['CARD_TAG'] == 1) & (pvp_df['pkm'] == 1), 'pkm_card_tag'] = 1
pvp_df.loc[(pvp_df['QTY_TAG'] == 1) & (pvp_df['pkm'] == 1), 'pkm_qty_tag'] = 1
pvp_df.loc[(pvp_df['PKG_TAG'] == 1) & (pvp_df['pkm'] == 1), 'pkm_pkg_tag'] = 1
pvp_df.loc[(pvp_df['CTF_TAG'] == 1) & (pvp_df['pkm'] == 1), 'pkm_ctf_tag'] = 1
pvp_df.loc[(pvp_df['CND_TAG'] == 1) & (pvp_df['pkm'] == 1), 'pkm_cnd_tag'] = 1


pvp_df.loc[(pvp_df['CARD_TAG'] == 1) & (pvp_df['ygo'] == 1), 'ygo_card_tag'] = 1
pvp_df.loc[(pvp_df['QTY_TAG'] == 1) & (pvp_df['ygo'] == 1), 'ygo_qty_tag'] = 1
pvp_df.loc[(pvp_df['PKG_TAG'] == 1) & (pvp_df['ygo'] == 1), 'ygo_pkg_tag'] = 1
pvp_df.loc[(pvp_df['CTF_TAG'] == 1) & (pvp_df['ygo'] == 1), 'ygo_ctf_tag'] = 1
pvp_df.loc[(pvp_df['CND_TAG'] == 1) & (pvp_df['ygo'] == 1), 'ygo_cnd_tag'] = 1

##Aggragate monthly counts
pvp_monthly_df = pvp_df.copy()

mtg_monthly_card_tags = pvp_monthly_df.groupby('month_count')['mtg_card_tag'].sum()
pvp_monthly_df = pd.merge(pvp_monthly_df, mtg_monthly_card_tags, how='right', on='month_count')

mtg_monthly_qty_tags = pvp_monthly_df.groupby('month_count')['mtg_qty_tag'].sum()
pvp_monthly_df = pd.merge(pvp_monthly_df, mtg_monthly_qty_tags, how='right', on='month_count')

mtg_monthly_pkg_tags = pvp_monthly_df.groupby('month_count')['mtg_pkg_tag'].sum()
pvp_monthly_df = pd.merge(pvp_monthly_df, mtg_monthly_pkg_tags, how='right', on='month_count')

mtg_monthly_ctf_tags = pvp_monthly_df.groupby('month_count')['mtg_ctf_tag'].sum()
pvp_monthly_df = pd.merge(pvp_monthly_df, mtg_monthly_ctf_tags, how='right', on='month_count')

mtg_monthly_cnd_tags = pvp_monthly_df.groupby('month_count')['mtg_cnd_tag'].sum()
pvp_monthly_df = pd.merge(pvp_monthly_df, mtg_monthly_cnd_tags, how='right', on='month_count')

pkm_monthly_card_tags = pvp_monthly_df.groupby('month_count')['pkm_card_tag'].sum()
pvp_monthly_df = pd.merge(pvp_monthly_df, pkm_monthly_card_tags, how='right', on='month_count')

pkm_monthly_qty_tags = pvp_monthly_df.groupby('month_count')['pkm_qty_tag'].sum()
pvp_monthly_df = pd.merge(pvp_monthly_df, pkm_monthly_qty_tags, how='right', on='month_count')

pkm_monthly_pkg_tags = pvp_monthly_df.groupby('month_count')['pkm_pkg_tag'].sum()
pvp_monthly_df = pd.merge(pvp_monthly_df, pkm_monthly_pkg_tags, how='right', on='month_count')

pkm_monthly_ctf_tags = pvp_monthly_df.groupby('month_count')['pkm_ctf_tag'].sum()
pvp_monthly_df = pd.merge(pvp_monthly_df, pkm_monthly_ctf_tags, how='right', on='month_count')

pkm_monthly_cnd_tags = pvp_monthly_df.groupby('month_count')['pkm_cnd_tag'].sum()
pvp_monthly_df = pd.merge(pvp_monthly_df, pkm_monthly_cnd_tags, how='right', on='month_count')

ygo_monthly_card_tags = pvp_monthly_df.groupby('month_count')['ygo_card_tag'].sum()
pvp_monthly_df = pd.merge(pvp_monthly_df, ygo_monthly_card_tags, how='right', on='month_count')

ygo_monthly_qty_tags = pvp_monthly_df.groupby('month_count')['ygo_qty_tag'].sum()
pvp_monthly_df = pd.merge(pvp_monthly_df, ygo_monthly_qty_tags, how='right', on='month_count')

ygo_monthly_pkg_tags = pvp_monthly_df.groupby('month_count')['ygo_pkg_tag'].sum()
pvp_monthly_df = pd.merge(pvp_monthly_df, ygo_monthly_pkg_tags, how='right', on='month_count')

ygo_monthly_ctf_tags = pvp_monthly_df.groupby('month_count')['ygo_ctf_tag'].sum()
pvp_monthly_df = pd.merge(pvp_monthly_df, ygo_monthly_ctf_tags, how='right', on='month_count')

ygo_monthly_cnd_tags = pvp_monthly_df.groupby('month_count')['ygo_cnd_tag'].sum()
pvp_monthly_df = pd.merge(pvp_monthly_df, ygo_monthly_cnd_tags, how='right', on='month_count')

##Parse down to punch + puncher + month_of combos
pvp_monthly_df["dupe"] = pvp_monthly_df['Punch'].astype(str) + pvp_monthly_df['Puncher'].astype(str) + pvp_monthly_df['pvp_month_of'].astype(str)

pvp_monthly_df.drop_duplicates(subset=['dupe'], inplace=True)

##Aggragete monthly orders, sqs, and cards
pvp_monthly_df['Units'] = pvp_monthly_df['Units'].astype('float64')
pvp_monthly_df['Orders'] = pvp_monthly_df['Orders'].astype('float64')

pvp_monthly_cards = pvp_monthly_df.groupby('month_count')['Units'].sum()
pvp_monthly_df = pd.merge(pvp_monthly_df, pvp_monthly_cards, how='right', on='month_count')

pvp_monthly_orders = pvp_monthly_df.groupby('month_count')['Orders'].sum()
pvp_monthly_df = pd.merge(pvp_monthly_df, pvp_monthly_orders, how='right', on='month_count')

pvp_monthly_sqs = pvp_monthly_df.groupby('month_count')['Subtask'].nunique()
pvp_monthly_df = pd.merge(pvp_monthly_df, pvp_monthly_sqs, how='right', on='month_count')

##Clean up dataframe
pvp_monthly_df.drop(['mtg_card_tag_x', 'mtg_qty_tag_x', 'mtg_pkg_tag_x', 'mtg_ctf_tag_x', 'mtg_cnd_tag_x', 'pkm_card_tag_x', 'pkm_qty_tag_x', 'pkm_pkg_tag_x', 'pkm_ctf_tag_x', 'pkm_cnd_tag_x', 'ygo_card_tag_x', 'ygo_qty_tag_x', 'ygo_pkg_tag_x', 'ygo_ctf_tag_x', 'ygo_cnd_tag_x', 'dupe', 'Units_x', 'Orders_x', 'Task', 'GAME_NAME', 'order_number', 'TIX_ID', 'CARD_TAG', 'QTY_TAG', 'PKG_TAG', 'CTF_TAG', 'CND_TAG', 'mtg', 'pkm', 'ygo', 'pvp_day_of_week', 'punch_normalized', 'Subtask_x'],
 axis=1, inplace=True)

pvp_monthly_df.rename(columns={'mtg_card_tag_y':'mtg_monthly_card_tags', 'mtg_qty_tag_y':'mtg_monthly_qty_tags', 'mtg_pkg_tag_y':'mtg_monthly_pkg_tags', 'mtg_ctf_tag_y':'mtg_monthly_ctf_tags', 'mtg_cnd_tag_y':'mtg_monthly_cnd_tags', 'pkm_card_tag_y':'pkm_monthly_card_tags', 'pkm_qty_tag_y':'pkm_monthly_qty_tags', 'pkm_pkg_tag_y':'pkm_monthly_pkg_tags', 'pkm_ctf_tag_y':'pkm_monthly_ctf_tags', 'pkm_cnd_tag_y':'pkm_monthly_cnd_tags', 'ygo_card_tag_y':'ygo_monthly_card_tags', 'ygo_qty_tag_y':'ygo_monthly_qty_tags', 'ygo_pkg_tag_y':'ygo_monthly_pkg_tags', 'ygo_ctf_tag_y':'ygo_monthly_ctf_tags', 'ygo_cnd_tag_y':'ygo_monthly_cnd_tags', 'Units_y':'monthly_cards', 'Orders_y':'monthly_orders', 'Subtask_y':'monthly_sqs'
}, inplace=True)

##Aggragate weekly counts
pvp_weekly_df = pvp_df.copy()

mtg_weekly_card_tags = pvp_weekly_df.groupby('week_count')['mtg_card_tag'].sum()
pvp_weekly_df = pd.merge(pvp_weekly_df, mtg_weekly_card_tags, how='right', on='week_count')

mtg_weekly_qty_tags = pvp_weekly_df.groupby('week_count')['mtg_qty_tag'].sum()
pvp_weekly_df = pd.merge(pvp_weekly_df, mtg_weekly_qty_tags, how='right', on='week_count')

mtg_weekly_pkg_tags = pvp_weekly_df.groupby('week_count')['mtg_pkg_tag'].sum()
pvp_weekly_df = pd.merge(pvp_weekly_df, mtg_weekly_pkg_tags, how='right', on='week_count')

mtg_weekly_ctf_tags = pvp_weekly_df.groupby('week_count')['mtg_ctf_tag'].sum()
pvp_weekly_df = pd.merge(pvp_weekly_df, mtg_weekly_ctf_tags, how='right', on='week_count')

mtg_weekly_cnd_tags = pvp_weekly_df.groupby('week_count')['mtg_cnd_tag'].sum()
pvp_weekly_df = pd.merge(pvp_weekly_df, mtg_weekly_cnd_tags, how='right', on='week_count')

pkm_weekly_card_tags = pvp_weekly_df.groupby('week_count')['pkm_card_tag'].sum()
pvp_weekly_df = pd.merge(pvp_weekly_df, pkm_weekly_card_tags, how='right', on='week_count')

pkm_weekly_qty_tags = pvp_weekly_df.groupby('week_count')['pkm_qty_tag'].sum()
pvp_weekly_df = pd.merge(pvp_weekly_df, pkm_weekly_qty_tags, how='right', on='week_count')

pkm_weekly_pkg_tags = pvp_weekly_df.groupby('week_count')['pkm_pkg_tag'].sum()
pvp_weekly_df = pd.merge(pvp_weekly_df, pkm_weekly_pkg_tags, how='right', on='week_count')

pkm_weekly_ctf_tags = pvp_weekly_df.groupby('week_count')['pkm_ctf_tag'].sum()
pvp_weekly_df = pd.merge(pvp_weekly_df, pkm_weekly_ctf_tags, how='right', on='week_count')

pkm_weekly_cnd_tags = pvp_weekly_df.groupby('week_count')['pkm_cnd_tag'].sum()
pvp_weekly_df = pd.merge(pvp_weekly_df, pkm_weekly_cnd_tags, how='right', on='week_count')

ygo_weekly_card_tags = pvp_weekly_df.groupby('week_count')['ygo_card_tag'].sum()
pvp_weekly_df = pd.merge(pvp_weekly_df, ygo_weekly_card_tags, how='right', on='week_count')

ygo_weekly_qty_tags = pvp_weekly_df.groupby('week_count')['ygo_qty_tag'].sum()
pvp_weekly_df = pd.merge(pvp_weekly_df, ygo_weekly_qty_tags, how='right', on='week_count')

ygo_weekly_pkg_tags = pvp_weekly_df.groupby('week_count')['ygo_pkg_tag'].sum()
pvp_weekly_df = pd.merge(pvp_weekly_df, ygo_weekly_pkg_tags, how='right', on='week_count')

ygo_weekly_ctf_tags = pvp_weekly_df.groupby('week_count')['ygo_ctf_tag'].sum()
pvp_weekly_df = pd.merge(pvp_weekly_df, ygo_weekly_ctf_tags, how='right', on='week_count')

ygo_weekly_cnd_tags = pvp_weekly_df.groupby('week_count')['ygo_cnd_tag'].sum()
pvp_weekly_df = pd.merge(pvp_weekly_df, ygo_weekly_cnd_tags, how='right', on='week_count')

##Parse down to punch + puncher + week_of combos
pvp_weekly_df["dupe"] = pvp_weekly_df['Punch'].astype(str) + pvp_weekly_df['Puncher'].astype(str) + pvp_weekly_df['pvp_week_of'].astype(str)

pvp_weekly_df.drop_duplicates(subset=['dupe'], inplace=True)

##Aggragete weekly orders and cards and sqs
pvp_weekly_df['Units'] = pvp_weekly_df['Units'].astype('float64')
pvp_weekly_df['Orders'] = pvp_weekly_df['Orders'].astype('float64')

pvp_weekly_cards = pvp_weekly_df.groupby('week_count')['Units'].sum()
pvp_weekly_df = pd.merge(pvp_weekly_df, pvp_weekly_cards, how='right', on='week_count')

pvp_weekly_orders = pvp_weekly_df.groupby('week_count')['Orders'].sum()
pvp_weekly_df = pd.merge(pvp_weekly_df, pvp_weekly_orders, how='right', on='week_count')

pvp_weekly_sqs = pvp_weekly_df.groupby('week_count')['Subtask'].nunique()
pvp_weekly_df = pd.merge(pvp_weekly_df, pvp_weekly_sqs, how='right', on='week_count')

##Clean up dataframe
pvp_weekly_df.drop(['mtg_card_tag_x', 'mtg_qty_tag_x', 'mtg_pkg_tag_x', 'mtg_ctf_tag_x', 'mtg_cnd_tag_x', 'pkm_card_tag_x', 'pkm_qty_tag_x', 'pkm_pkg_tag_x', 'pkm_ctf_tag_x', 'pkm_cnd_tag_x', 'ygo_card_tag_x', 'ygo_qty_tag_x', 'ygo_pkg_tag_x', 'ygo_ctf_tag_x', 'ygo_cnd_tag_x', 'dupe', 'Units_x', 'Orders_x', 'Task', 'GAME_NAME', 'order_number', 'TIX_ID', 'CARD_TAG', 'QTY_TAG', 'PKG_TAG', 'CTF_TAG', 'CND_TAG', 'mtg', 'pkm', 'ygo', 'pvp_day_of_week', 'punch_normalized', 'Subtask_x'],
 axis=1, inplace=True)

pvp_weekly_df.rename(columns={'mtg_card_tag_y':'mtg_weekly_card_tags', 'mtg_qty_tag_y':'mtg_weekly_qty_tags', 'mtg_pkg_tag_y':'mtg_weekly_pkg_tags', 'mtg_ctf_tag_y':'mtg_weekly_ctf_tags', 'mtg_cnd_tag_y':'mtg_weekly_cnd_tags', 'pkm_card_tag_y':'pkm_weekly_card_tags', 'pkm_qty_tag_y':'pkm_weekly_qty_tags', 'pkm_pkg_tag_y':'pkm_weekly_pkg_tags', 'pkm_ctf_tag_y':'pkm_weekly_ctf_tags', 'pkm_cnd_tag_y':'pkm_weekly_cnd_tags', 'ygo_card_tag_y':'ygo_weekly_card_tags', 'ygo_qty_tag_y':'ygo_weekly_qty_tags', 'ygo_pkg_tag_y':'ygo_weekly_pkg_tags', 'ygo_ctf_tag_y':'ygo_weekly_ctf_tags', 'ygo_cnd_tag_y':'ygo_weekly_cnd_tags', 'Units_y':'weekly_cards', 'Orders_y':'weekly_orders', 'Subtask_y':'weekly_sqs'
}, inplace=True)

##Combine frames
pvp_weekly_df["combined"] = pvp_weekly_df['Punch'].astype(str) + pvp_weekly_df['Puncher'].astype(str)
pvp_monthly_df["combined"] = pvp_monthly_df['Punch'].astype(str) + pvp_monthly_df['Puncher'].astype(str)

pvp_combined_df = pd.DataFrame()

pvp_combined_df = pd.merge(pvp_weekly_df, pvp_monthly_df, how='right', on='combined')

##Aggragate errors
pvp_combined_df['mtg_monthly_card_tags'] = pvp_combined_df['mtg_monthly_card_tags'].astype('float64')
pvp_combined_df['mtg_monthly_qty_tags'] = pvp_combined_df['mtg_monthly_qty_tags'].astype('float64')
pvp_combined_df['mtg_monthly_pkg_tags'] = pvp_combined_df['mtg_monthly_pkg_tags'].astype('float64')
pvp_combined_df['mtg_monthly_ctf_tags'] = pvp_combined_df['mtg_monthly_ctf_tags'].astype('float64')
pvp_combined_df['mtg_monthly_cnd_tags'] = pvp_combined_df['mtg_monthly_cnd_tags'].astype('float64')

pvp_combined_df['pkm_monthly_card_tags'] = pvp_combined_df['pkm_monthly_card_tags'].astype('float64')
pvp_combined_df['pkm_monthly_qty_tags'] = pvp_combined_df['pkm_monthly_qty_tags'].astype('float64')
pvp_combined_df['pkm_monthly_pkg_tags'] = pvp_combined_df['pkm_monthly_pkg_tags'].astype('float64')
pvp_combined_df['pkm_monthly_ctf_tags'] = pvp_combined_df['pkm_monthly_ctf_tags'].astype('float64')
pvp_combined_df['pkm_monthly_cnd_tags'] = pvp_combined_df['pkm_monthly_cnd_tags'].astype('float64')

pvp_combined_df['ygo_monthly_card_tags'] = pvp_combined_df['ygo_monthly_card_tags'].astype('float64')
pvp_combined_df['ygo_monthly_qty_tags'] = pvp_combined_df['ygo_monthly_qty_tags'].astype('float64')
pvp_combined_df['ygo_monthly_pkg_tags'] = pvp_combined_df['ygo_monthly_pkg_tags'].astype('float64')
pvp_combined_df['ygo_monthly_ctf_tags'] = pvp_combined_df['ygo_monthly_ctf_tags'].astype('float64')
pvp_combined_df['ygo_monthly_cnd_tags'] = pvp_combined_df['ygo_monthly_cnd_tags'].astype('float64')

pvp_combined_df['mtg_weekly_card_tags'] = pvp_combined_df['mtg_weekly_card_tags'].astype('float64')
pvp_combined_df['mtg_weekly_qty_tags'] = pvp_combined_df['mtg_weekly_qty_tags'].astype('float64')
pvp_combined_df['mtg_weekly_pkg_tags'] = pvp_combined_df['mtg_weekly_pkg_tags'].astype('float64')
pvp_combined_df['mtg_weekly_ctf_tags'] = pvp_combined_df['mtg_weekly_ctf_tags'].astype('float64')
pvp_combined_df['mtg_weekly_cnd_tags'] = pvp_combined_df['mtg_weekly_cnd_tags'].astype('float64')

pvp_combined_df['pkm_weekly_card_tags'] = pvp_combined_df['pkm_weekly_card_tags'].astype('float64')
pvp_combined_df['pkm_weekly_qty_tags'] = pvp_combined_df['pkm_weekly_qty_tags'].astype('float64')
pvp_combined_df['pkm_weekly_pkg_tags'] = pvp_combined_df['pkm_weekly_pkg_tags'].astype('float64')
pvp_combined_df['pkm_weekly_ctf_tags'] = pvp_combined_df['pkm_weekly_ctf_tags'].astype('float64')
pvp_combined_df['pkm_weekly_cnd_tags'] = pvp_combined_df['pkm_weekly_cnd_tags'].astype('float64')

pvp_combined_df['ygo_weekly_card_tags'] = pvp_combined_df['ygo_weekly_card_tags'].astype('float64')
pvp_combined_df['ygo_weekly_qty_tags'] = pvp_combined_df['ygo_weekly_qty_tags'].astype('float64')
pvp_combined_df['ygo_weekly_pkg_tags'] = pvp_combined_df['ygo_weekly_pkg_tags'].astype('float64')
pvp_combined_df['ygo_weekly_ctf_tags'] = pvp_combined_df['ygo_weekly_ctf_tags'].astype('float64')
pvp_combined_df['ygo_weekly_cnd_tags'] = pvp_combined_df['ygo_weekly_cnd_tags'].astype('float64')

pvp_combined_df['monthly_card_tags'] = pvp_combined_df['mtg_monthly_card_tags'] + pvp_combined_df['pkm_monthly_card_tags'] + pvp_combined_df['ygo_monthly_card_tags']
pvp_combined_df['monthly_qty_tags'] = pvp_combined_df['mtg_monthly_qty_tags'] + pvp_combined_df['pkm_monthly_qty_tags'] + pvp_combined_df['ygo_monthly_qty_tags']
pvp_combined_df['monthly_pkg_tags'] = pvp_combined_df['mtg_monthly_pkg_tags'] + pvp_combined_df['pkm_monthly_pkg_tags'] + pvp_combined_df['ygo_monthly_pkg_tags']
pvp_combined_df['monthly_ctf_tags'] = pvp_combined_df['mtg_monthly_ctf_tags'] + pvp_combined_df['pkm_monthly_ctf_tags'] + pvp_combined_df['ygo_monthly_ctf_tags']
pvp_combined_df['monthly_cnd_tags'] = pvp_combined_df['mtg_monthly_cnd_tags'] + pvp_combined_df['pkm_monthly_cnd_tags'] + pvp_combined_df['ygo_monthly_cnd_tags']

pvp_combined_df['weekly_card_tags'] = pvp_combined_df['mtg_weekly_card_tags'] + pvp_combined_df['pkm_weekly_card_tags'] + pvp_combined_df['ygo_weekly_card_tags']
pvp_combined_df['weekly_qty_tags'] = pvp_combined_df['mtg_weekly_qty_tags'] + pvp_combined_df['pkm_weekly_qty_tags'] + pvp_combined_df['ygo_weekly_qty_tags']
pvp_combined_df['weekly_pkg_tags'] = pvp_combined_df['mtg_weekly_pkg_tags'] + pvp_combined_df['pkm_weekly_pkg_tags'] + pvp_combined_df['ygo_weekly_pkg_tags']
pvp_combined_df['weekly_ctf_tags'] = pvp_combined_df['mtg_weekly_ctf_tags'] + pvp_combined_df['pkm_weekly_ctf_tags'] + pvp_combined_df['ygo_weekly_ctf_tags']
pvp_combined_df['weekly_cnd_tags'] = pvp_combined_df['mtg_weekly_cnd_tags'] + pvp_combined_df['pkm_weekly_cnd_tags'] + pvp_combined_df['ygo_weekly_cnd_tags']

##Clean up final frame
pvp_combined_df.drop(['Punch_x', 'First_Offset_x', 'Puncher_x', 'Start Date_x', 'Supervisor_x', 'pvp_week_of_x', 'pvp_month_of_x', 'pvp_operator_tenure_x', 'week_count_x'
], axis=1, inplace=True)

pvp_combined_df.rename(columns={'Punch_y':'Punch', 'First_Offset_y':'First_Offset', 'Puncher_y':'Puncher', 'Start Date_y':'Start Date', 'Supervisor_y':'Supervisor', 'pvp_week_of_y':'pvp_week_of', 'pvp_month_of_y':'pvp_month_of', 'pvp_operator_tenure_y':'pvp_operator_tenure', 'week_count_y':'week_count'
}, inplace=True)

##Reduce to individual week counts and make final dataframe
pvp_combined_df.drop_duplicates(subset=['week_count'], inplace=True)

pvp_combined_df = pvp_combined_df[[
'Puncher',
'Start Date',
'Supervisor',
'pvp_month_of',
'pvp_week_of',
'pvp_operator_tenure',
'monthly_cards',
'monthly_orders',
'monthly_sqs',
'monthly_card_tags',
'monthly_qty_tags',
'monthly_pkg_tags',
'monthly_ctf_tags',
'monthly_cnd_tags',
'mtg_monthly_card_tags',
'mtg_monthly_qty_tags',
'mtg_monthly_pkg_tags',
'mtg_monthly_ctf_tags',
'mtg_monthly_cnd_tags',
'pkm_monthly_card_tags',
'pkm_monthly_qty_tags',
'pkm_monthly_pkg_tags',
'pkm_monthly_ctf_tags',
'pkm_monthly_cnd_tags',
'ygo_monthly_card_tags',
'ygo_monthly_qty_tags',
'ygo_monthly_pkg_tags',
'ygo_monthly_ctf_tags',
'ygo_monthly_cnd_tags',
'weekly_cards',
'weekly_orders',
'weekly_sqs',
'weekly_card_tags',
'weekly_qty_tags',
'weekly_pkg_tags',
'weekly_ctf_tags',
'weekly_cnd_tags',
'mtg_weekly_card_tags',
'mtg_weekly_qty_tags',
'mtg_weekly_pkg_tags',
'mtg_weekly_ctf_tags',
'mtg_weekly_cnd_tags',
'pkm_weekly_card_tags',
'pkm_weekly_qty_tags',
'pkm_weekly_pkg_tags',
'pkm_weekly_ctf_tags',
'pkm_weekly_cnd_tags',
'ygo_weekly_card_tags',
'ygo_weekly_qty_tags',
'ygo_weekly_pkg_tags',
'ygo_weekly_ctf_tags',
'ygo_weekly_cnd_tags']]

##Write data to sheet
pvpDataTab.clear()
gd.set_with_dataframe(pvpDataTab, pvp_combined_df)

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