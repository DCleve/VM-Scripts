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

pullverDoc = gc.open_by_key('1vyTesfxBD9h_WNCVI4b2oH7gvHYZ9vEfkawCNanTCr0')
pullverDataTab = pullverDoc.worksheet('Data')

pullDoc = gc.open_by_key('1Sjb_L3yn8qem5QdwBxP4GRJu-0U4F8S3PiWXh4--iGc')
pullDataTab = pullDoc.worksheet('Data')

##Import Staffing Data
staffing = gc.open_by_key('1sBVK5vjiB72JuKePpht2R4vZ4ShxuZKjQreE8FE7068').worksheet('Current Staff')
staffing_df = pd.DataFrame.from_dict(staffing.get_all_records())
staffing_df.dropna(subset=["Preferred Name"], inplace=True)
staffing_df.drop(staffing_df.filter(like='Unnamed'), axis=1, inplace=True)
staffing_df = staffing_df[['Preferred Name', 'Email', 'Shift Name', 'Start Date', 'Supervisor', 'OPs Lead', 'Last, First (Formatting)', 'Role']]

staffing_df.rename(columns={'Preferred Name':'Puncher'}, inplace=True)

staffing_df = staffing_df[['Puncher', 'Start Date', 'Supervisor']]

##Import nuway data
punch_string = ["C:", "Users", login, "OneDrive - eBay Inc", "AC-Scripting", "Data CSVs", "NuWay", "Data.csv"]
punch_result = separator.join(punch_string)
punch_df = pd.read_csv(punch_result)

punch_df["Punch"] = punch_df['Data'].str.split('|').str[0]
punch_df["First_Offset"] = punch_df['Data'].str.split('|').str[1]
punch_df["Puncher"] = punch_df['Data'].str.split('|').str[2]
punch_df["Units"] = punch_df['Data'].str.split('|').str[3]
punch_df["SQ/POQ"] = punch_df['Data'].str.split('|').str[4]
punch_df["Task"] = punch_df['Data'].str.split('|').str[7]
punch_df["Orders"] = punch_df['Data'].str.split('|').str[13]
punch_df["Extra"] = punch_df['Data'].str.split('|').str[14]
punch_df["Missing"] = punch_df['Data'].str.split('|').str[15]
punch_df["Similar"] = punch_df['Data'].str.split('|').str[16]
punch_df["Unrecorded"] = punch_df['Data'].str.split('|').str[17]
punch_df["Other"] = punch_df['Data'].str.split('|').str[18]

punch_df = pd.merge(punch_df, staffing_df, how='left', on='Puncher')

punch_df = punch_df[['Punch', 'First_Offset', 'Puncher',  'Units', 'SQ/POQ', 'Task', 'Orders', 'Start Date', 'Supervisor', 'Extra', 'Missing', 'Similar', 'Unrecorded', 'Other']]

punch_df['Task'] = punch_df['Task'].str.lower()

punch_df["clean_sq"] = punch_df['SQ/POQ']
punch_df.loc[punch_df['SQ/POQ'].str[-3:] != 'poq', 'clean_sq'] = punch_df['SQ/POQ'].str[:10]

##Create task frames
error_df = punch_df.copy()
pull_df = punch_df.copy()
pullver_df = punch_df.copy()

pull_df = pull_df.loc[(pull_df['Task'] == 'pull') | (pull_df['Task'] == 'paperless pull')]

pullver_df = pullver_df.loc[pullver_df['Task'] == 'pull ver']

error_df = error_df.loc[(error_df['Task'] == 'pvp') | (error_df['Task'] == 'sq error resolution') | (error_df['Task'] == 'pull ver')]

##Split into task error frames
errors_from_pvp = error_df.copy()
errors_from_pvp = errors_from_pvp.loc[errors_from_pvp['Task'] == 'pvp']
errors_from_pvp.rename(columns={'SQ/POQ':'pvp_sq', 'Extra':'pvp_extra', 'Missing':'pvp_miss', 'Similar':'pvp_sim', 'Unrecorded':'pvp_unrec', 'Other':'pvp_other'}, inplace=True)
errors_from_pvp = errors_from_pvp[['pvp_sq', 'pvp_extra', 'pvp_miss', 'pvp_sim', 'pvp_unrec', 'pvp_other', 'clean_sq']]

errors_from_er_res = error_df.copy()
errors_from_er_res = errors_from_er_res.loc[errors_from_er_res['Task'] == 'sq error resolution']
errors_from_er_res.rename(columns={'SQ/POQ':'er_res_sq', 'Missing':'er_res_extra', 'Similar':'er_res_miss', 'Unrecorded':'er_res_sim'}, inplace=True)
errors_from_er_res = errors_from_er_res[['er_res_sq', 'er_res_extra', 'er_res_miss', 'er_res_sim', 'clean_sq']]

errors_from_pullver = error_df.copy()
errors_from_pullver = errors_from_pullver.loc[errors_from_pullver['Task'] == 'pull ver']
errors_from_pullver.rename(columns={'SQ/POQ':'pullver_sq', 'Extra':'pullver_extra', 'Missing':'pullver_miss', 'Similar':'pullver_sim', 'Unrecorded':'pullver_unrec', 'Other':'pullver_other'}, inplace=True)
errors_from_pullver = errors_from_pullver[['pullver_sq', 'pullver_extra', 'pullver_miss', 'pullver_sim', 'pullver_unrec', 'pullver_other', 'clean_sq']]

##Fix variable types
errors_from_pullver['pullver_extra'] = errors_from_pullver['pullver_extra'].apply(pd.to_numeric, errors='coerce').fillna(0)
errors_from_pullver['pullver_miss'] = errors_from_pullver['pullver_miss'].apply(pd.to_numeric, errors='coerce').fillna(0)
errors_from_pullver['pullver_sim'] = errors_from_pullver['pullver_sim'].apply(pd.to_numeric, errors='coerce').fillna(0)
errors_from_pullver['pullver_unrec'] = errors_from_pullver['pullver_unrec'].apply(pd.to_numeric, errors='coerce').fillna(0)
errors_from_pullver['pullver_other'] = errors_from_pullver['pullver_other'].apply(pd.to_numeric, errors='coerce').fillna(0)

errors_from_er_res['er_res_extra'] = errors_from_er_res['er_res_extra'].apply(pd.to_numeric, errors='coerce').fillna(0)
errors_from_er_res['er_res_miss'] = errors_from_er_res['er_res_miss'].apply(pd.to_numeric, errors='coerce').fillna(0)
errors_from_er_res['er_res_sim'] = errors_from_er_res['er_res_sim'].apply(pd.to_numeric, errors='coerce').fillna(0)

errors_from_pvp['pvp_extra'] = errors_from_pvp['pvp_extra'].apply(pd.to_numeric, errors='coerce').fillna(0)
errors_from_pvp['pvp_miss'] = errors_from_pvp['pvp_miss'].apply(pd.to_numeric, errors='coerce').fillna(0)
errors_from_pvp['pvp_sim'] = errors_from_pvp['pvp_sim'].apply(pd.to_numeric, errors='coerce').fillna(0)
errors_from_pvp['pvp_unrec'] = errors_from_pvp['pvp_unrec'].apply(pd.to_numeric, errors='coerce').fillna(0)
errors_from_pvp['pvp_other'] = errors_from_pvp['pvp_other'].apply(pd.to_numeric, errors='coerce').fillna(0)

##Aggragate errors per sq in each error frame
#From pvp
total_pvp_extra = errors_from_pvp.groupby('pvp_sq')['pvp_extra'].sum()
errors_from_pvp = pd.merge(errors_from_pvp, total_pvp_extra, how='right', on='pvp_sq')

total_pvp_miss = errors_from_pvp.groupby('pvp_sq')['pvp_miss'].sum()
errors_from_pvp = pd.merge(errors_from_pvp, total_pvp_miss, how='right', on='pvp_sq')

total_pvp_sim = errors_from_pvp.groupby('pvp_sq')['pvp_sim'].sum()
errors_from_pvp = pd.merge(errors_from_pvp, total_pvp_sim, how='right', on='pvp_sq')

total_pvp_unrec = errors_from_pvp.groupby('pvp_sq')['pvp_unrec'].sum()
errors_from_pvp = pd.merge(errors_from_pvp, total_pvp_unrec, how='right', on='pvp_sq')

total_pvp_other = errors_from_pvp.groupby('pvp_sq')['pvp_other'].sum()
errors_from_pvp = pd.merge(errors_from_pvp, total_pvp_other, how='right', on='pvp_sq')

errors_from_pvp.drop(['pvp_extra_x', 'pvp_miss_x', 'pvp_sim_x', 'pvp_unrec_x', 'pvp_other_x'], axis=1, inplace=True)

errors_from_pvp.rename(columns={'pvp_extra_y':'total_pvp_extra', 'pvp_miss_y':'total_pvp_miss', 'pvp_sim_y':'total_pvp_sim', 'pvp_unrec_y':'total_pvp_unrec', 'pvp_other_y':'total_pvp_other'}, inplace=True)

errors_from_pvp.drop_duplicates(subset=['pvp_sq'], inplace=True)

#From error res
total_er_res_extra = errors_from_er_res.groupby('er_res_sq')['er_res_extra'].sum()
errors_from_er_res = pd.merge(errors_from_er_res, total_er_res_extra, how='right', on='er_res_sq')

total_er_res_miss = errors_from_er_res.groupby('er_res_sq')['er_res_miss'].sum()
errors_from_er_res = pd.merge(errors_from_er_res, total_er_res_miss, how='right', on='er_res_sq')

total_er_res_sim = errors_from_er_res.groupby('er_res_sq')['er_res_sim'].sum()
errors_from_er_res = pd.merge(errors_from_er_res, total_er_res_sim, how='right', on='er_res_sq')

errors_from_er_res.drop(['er_res_extra_x', 'er_res_miss_x', 'er_res_sim_x'], axis=1, inplace=True)

errors_from_er_res.rename(columns={'er_res_extra_y':'total_er_res_extra', 'er_res_miss_y':'total_er_res_miss', 'er_res_sim_y':'total_er_res_sim'}, inplace=True)

errors_from_er_res.drop_duplicates(subset=['er_res_sq'], inplace=True)

#From pull ver
total_pullver_extra = errors_from_pullver.groupby('pullver_sq')['pullver_extra'].sum()
errors_from_pullver = pd.merge(errors_from_pullver, total_pullver_extra, how='right', on='pullver_sq')

total_pullver_miss = errors_from_pullver.groupby('pullver_sq')['pullver_miss'].sum()
errors_from_pullver = pd.merge(errors_from_pullver, total_pullver_miss, how='right', on='pullver_sq')

total_pullver_sim = errors_from_pullver.groupby('pullver_sq')['pullver_sim'].sum()
errors_from_pullver = pd.merge(errors_from_pullver, total_pullver_sim, how='right', on='pullver_sq')

total_pullver_unrec = errors_from_pullver.groupby('pullver_sq')['pullver_unrec'].sum()
errors_from_pullver = pd.merge(errors_from_pullver, total_pullver_unrec, how='right', on='pullver_sq')

total_pullver_other = errors_from_pullver.groupby('pullver_sq')['pullver_other'].sum()
errors_from_pullver = pd.merge(errors_from_pullver, total_pullver_other, how='right', on='pullver_sq')

errors_from_pullver.drop(['pullver_extra_x', 'pullver_miss_x', 'pullver_sim_x', 'pullver_unrec_x', 'pullver_other_x'], axis=1, inplace=True)

errors_from_pullver.rename(columns={'pullver_extra_y':'total_pullver_extra', 'pullver_miss_y':'total_pullver_miss', 'pullver_sim_y':'total_pullver_sim', 'pullver_unrec_y':'total_pullver_unrec', 'pullver_other_y':'total_pullver_other'}, inplace=True)

errors_from_pullver.drop_duplicates(subset=['pullver_sq'], inplace=True)

##Combine punch and appropriate error frames
pull_df = pd.merge(pull_df, errors_from_pullver, how='left', on='clean_sq')
pull_df = pd.merge(pull_df, errors_from_er_res, how='left', on='clean_sq')
pull_df = pd.merge(pull_df, errors_from_pvp, how='left', on='clean_sq')
pull_df = pull_df.fillna(0)

pullver_df = pd.merge(pullver_df, errors_from_er_res, how='left', on='clean_sq')
pullver_df = pd.merge(pullver_df, errors_from_pvp, how='left', on='clean_sq')
pullver_df = pullver_df.fillna(0)

##Import sq data
sq_string = ["C:", "Users", login, "OneDrive - eBay Inc", "AC-Scripting", "Data CSVs", "Snowflake", "sqacc.csv"]
sq_result = separator.join(sq_string)
sq_df = pd.read_csv(sq_result)

sq_df['SHIPPINGQUEUENUMBER'] = sq_df['SHIPPINGQUEUENUMBER'].str.lower()

sq_df.rename(columns={'SHIPPINGQUEUENUMBER':'SQ/POQ'}, inplace=True)

##Combine game info with pull/ver frames
pull_df = pd.merge(pull_df, sq_df, left_on='SQ/POQ', right_on='SQ/POQ')
pullver_df = pd.merge(pullver_df, sq_df, left_on='SQ/POQ', right_on='SQ/POQ')

##SQ Game Types (Pull)
pull_df["count"] = pull_df['Punch'].astype(str) + pull_df['Puncher'].astype(str) + pull_df['GAME_NAME'].astype(str)
pull_df.drop_duplicates(subset=['count'], inplace=True)
pull_df.drop('count', axis=1, inplace=True)
pull_df["count"] = pull_df['Punch'].astype(str) + pull_df['Puncher'].astype(str)

pull_df["mtg"] = 0
pull_df["pkm"] = 0
pull_df["ygo"] = 0
pull_df.loc[pull_df['GAME_NAME'] == 'Magic', 'mtg'] = 1
pull_df.loc[pull_df['GAME_NAME'] == 'Pokemon', 'pkm'] = 1
pull_df.loc[pull_df['GAME_NAME'] == 'YuGiOh', 'ygo'] = 1

pull_df['mtg'] = pull_df['mtg'].astype('float64')
pull_df['pkm'] = pull_df['pkm'].astype('float64')
pull_df['ygo'] = pull_df['ygo'].astype('float64')

pull_mtg = pull_df.groupby('count')['mtg'].sum()
pull_df = pd.merge(pull_df, pull_mtg, how='right', on='count')
pull_df.drop('mtg_x', axis=1, inplace=True)
pull_df.rename(columns={'mtg_y':'mtg'}, inplace=True)

pull_pkm = pull_df.groupby('count')['pkm'].sum()
pull_df = pd.merge(pull_df, pull_pkm, how='right', on='count')
pull_df.drop('pkm_x', axis=1, inplace=True)
pull_df.rename(columns={'pkm_y':'pkm'}, inplace=True)

pull_ygo = pull_df.groupby('count')['ygo'].sum()
pull_df = pd.merge(pull_df, pull_ygo, how='right', on='count')
pull_df.drop('ygo_x', axis=1, inplace=True)
pull_df.rename(columns={'ygo_y':'ygo'}, inplace=True)

pull_df.drop_duplicates(subset=['count'], inplace=True)

pull_df["mixed"] = pull_df['mtg'] + pull_df['pkm'] + pull_df['ygo']

pull_df["sq_type"] = ""

pull_df.loc[pull_df['mtg'] == 1, 'sq_type'] = "mtg"
pull_df.loc[pull_df['pkm'] == 1, 'sq_type'] = "pkm"
pull_df.loc[pull_df['ygo'] == 1, 'sq_type'] = "ygo"
pull_df.loc[pull_df['mixed'] > 1, 'sq_type'] = "mixed"

pull_df.drop(['GAME_NAME', 'count', 'PCID', 'SLOT', 'slot_parse', 'Orders', 'sq_card_quantity', 'QUEUE_NUMBER', 'ORDER_COUNT', 'CREATED_AT', 'CARD_QUANTITY', 'Extra', 'Missing', 'Similar', 'Unrecorded', 'Other'], axis=1, inplace=True)

##Remove dupes and aggragate remaining dupes of SQ Numbers
pull_df["dupe"] = pull_df['Punch'].astype(str) + pull_df['Puncher'].astype(str) + pull_df['SQ/POQ'].astype(str)
pull_df.drop_duplicates(subset=['dupe'], inplace=True)
pull_df.drop('dupe', axis=1, inplace=True)
pull_df["dupe"] = pull_df['Puncher'].astype(str) + pull_df['SQ/POQ'].astype(str)

pull_df['Units'] = pull_df['Units'].astype('float64')

sq_cards = pull_df.groupby('dupe')['Units'].sum()
pull_df = pd.merge(pull_df, sq_cards, how='right', on='dupe')
pull_df.drop('Units_x', axis=1, inplace=True)
pull_df.rename(columns={'Units_y':'Units'}, inplace=True)
pull_df.drop_duplicates(subset=['dupe'], inplace=True)

##Aggragate errors per sq and puncher combo (Pull)
pull_df["count"] = pull_df['Puncher'].astype(str) + pull_df['SQ/POQ'].astype(str)

#Pullver
pullver_extra_count_pull = pull_df.groupby('count')['total_pullver_extra'].sum()
pull_df = pd.merge(pull_df, pullver_extra_count_pull, how='right', on='count')

pullver_miss_count_pull = pull_df.groupby('count')['total_pullver_miss'].sum()
pull_df = pd.merge(pull_df, pullver_miss_count_pull, how='right', on='count')

pullver_sim_count_pull = pull_df.groupby('count')['total_pullver_sim'].sum()
pull_df = pd.merge(pull_df, pullver_sim_count_pull, how='right', on='count')

pullver_unrec_count_pull = pull_df.groupby('count')['total_pullver_unrec'].sum()
pull_df = pd.merge(pull_df, pullver_unrec_count_pull, how='right', on='count')

pullver_other_count_pull = pull_df.groupby('count')['total_pullver_other'].sum()
pull_df = pd.merge(pull_df, pullver_other_count_pull, how='right', on='count')

#Err res
er_res_extra_count_pull = pull_df.groupby('count')['total_er_res_extra'].sum()
pull_df = pd.merge(pull_df, er_res_extra_count_pull, how='right', on='count')

er_res_miss_count_pull = pull_df.groupby('count')['total_er_res_miss'].sum()
pull_df = pd.merge(pull_df, er_res_miss_count_pull, how='right', on='count')

er_res_sim_count_pull = pull_df.groupby('count')['total_er_res_sim'].sum()
pull_df = pd.merge(pull_df, er_res_sim_count_pull, how='right', on='count')

#pvp
pvp_extra_count_pull = pull_df.groupby('count')['total_pvp_extra'].sum()
pull_df = pd.merge(pull_df, pvp_extra_count_pull, how='right', on='count')

pvp_miss_count_pull = pull_df.groupby('count')['total_pvp_miss'].sum()
pull_df = pd.merge(pull_df, pvp_miss_count_pull, how='right', on='count')

pvp_sim_count_pull = pull_df.groupby('count')['total_pvp_sim'].sum()
pull_df = pd.merge(pull_df, pvp_sim_count_pull, how='right', on='count')

pvp_unrec_count_pull = pull_df.groupby('count')['total_pvp_unrec'].sum()
pull_df = pd.merge(pull_df, pvp_unrec_count_pull, how='right', on='count')

pvp_other_count_pull = pull_df.groupby('count')['total_pvp_other'].sum()
pull_df = pd.merge(pull_df, pvp_other_count_pull, how='right', on='count')

#Pull all the counts together
pull_df["total_extra_count_pull"] = pull_df['total_pullver_extra_y'].astype('float64') + pull_df['total_er_res_extra_y'].astype('float64') + pull_df['total_pvp_extra_y'].astype('float64')

pull_df["total_miss_count_pull"] = pull_df['total_pullver_miss_y'].astype('float64') + pull_df['total_er_res_miss_y'].astype('float64') + pull_df['total_pvp_miss_y'].astype('float64')

pull_df["total_sim_count_pull"] = pull_df['total_pullver_sim_y'].astype('float64') + pull_df['total_er_res_sim_y'].astype('float64') + pull_df['total_pvp_sim_y'].astype('float64')

pull_df["total_unrec_count_pull"] = pull_df['total_pullver_unrec_y'].astype('float64') + pull_df['total_pvp_unrec_y'].astype('float64')

pull_df["total_other_count_pull"] = pull_df['total_pullver_other_y'].astype('float64') + pull_df['total_pvp_other_y'].astype('float64')

pull_df.drop(['total_pullver_extra_x', 'total_pullver_miss_x', 'total_pullver_sim_x', 'total_pullver_unrec_x', 'total_pullver_other_x', 'total_pullver_extra_y', 'total_pullver_miss_y', 'total_pullver_sim_y', 'total_pullver_unrec_y', 'total_pullver_other_y', 'total_er_res_extra_x', 'total_er_res_miss_x', 'total_er_res_sim_x', 'total_er_res_extra_y', 'total_er_res_sim_y', 'total_er_res_miss_y', 'total_pvp_extra_x', 'total_pvp_miss_x', 'total_pvp_sim_x', 'total_pvp_unrec_x', 'total_pvp_other_x', 'total_pvp_extra_y', 'total_pvp_miss_y',  'total_pvp_sim_y', 'total_pvp_unrec_y', 'total_pvp_other_y', 'count', 'dupe'], axis=1, inplace=True)

##Month and week (Pull)
pull_df['Punch'] = pd.to_datetime(pull_df['Punch'])
pull_df["pull_day_of_week"] = pull_df['Punch'].dt.dayofweek
pull_df["pull_week_of"] = pull_df['Punch'] - pd.to_timedelta(pull_df['pull_day_of_week'], unit='D')

pull_df["punch_normalized"] = pd.to_datetime(pull_df['Punch']).dt.date
pull_df['pull_week_of'] = pd.to_datetime(pull_df['pull_week_of']).dt.date

pull_df["pull_month_of"] = pull_df['punch_normalized'].apply(lambda x: x.strftime('%Y-%m-01'))
pull_df["pull_operator_tenure"] = (pull_df['punch_normalized'] - pd.to_datetime(pull_df['Start Date']).dt.date)/np.timedelta64(1, 'D')/30

pull_df['month_count'] = pull_df['Puncher'].astype(str) + pull_df['pull_month_of'].astype(str)
pull_df['week_count'] = pull_df['Puncher'].astype(str) + pull_df['pull_week_of'].astype(str)

##Monthly Errors and output (Pull)
pull_monthly_df = pull_df.copy()
pull_monthly_df['Units'] = pull_monthly_df['Units'] .astype('float64')

pull_monthly_df["mtg_monthly_extra"] = 0
pull_monthly_df["mtg_monthly_sim"] = 0
pull_monthly_df["mtg_monthly_miss"] = 0
pull_monthly_df["mtg_monthly_unrec"] = 0
pull_monthly_df["mtg_monthly_other"] = 0

pull_monthly_df["pkm_monthly_extra"] = 0
pull_monthly_df["pkm_monthly_sim"] = 0
pull_monthly_df["pkm_monthly_miss"] = 0
pull_monthly_df["pkm_monthly_unrec"] = 0
pull_monthly_df["pkm_monthly_other"] = 0

pull_monthly_df["ygo_monthly_extra"] = 0
pull_monthly_df["ygo_monthly_sim"] = 0
pull_monthly_df["ygo_monthly_miss"] = 0
pull_monthly_df["ygo_monthly_unrec"] = 0
pull_monthly_df["ygo_monthly_other"] = 0

pull_monthly_df["mix_monthly_extra"] = 0
pull_monthly_df["mix_monthly_sim"] = 0
pull_monthly_df["mix_monthly_miss"] = 0
pull_monthly_df["mix_monthly_unrec"] = 0
pull_monthly_df["mix_monthly_other"] = 0


pull_monthly_df.loc[pull_monthly_df['sq_type'] == 'mtg', 'mtg_monthly_extra'] = pull_monthly_df['total_extra_count_pull']
pull_monthly_df.loc[pull_monthly_df['sq_type'] == 'pkm', 'pkm_monthly_extra'] = pull_monthly_df['total_extra_count_pull']
pull_monthly_df.loc[pull_monthly_df['sq_type'] == 'ygo', 'ygo_monthly_extra'] = pull_monthly_df['total_extra_count_pull']
pull_monthly_df.loc[pull_monthly_df['sq_type'] == 'mixed', 'mix_monthly_extra'] = pull_monthly_df['total_extra_count_pull']

pull_monthly_df.loc[pull_monthly_df['sq_type'] == 'mtg', 'mtg_monthly_sim'] = pull_monthly_df['total_sim_count_pull']
pull_monthly_df.loc[pull_monthly_df['sq_type'] == 'pkm', 'pkm_monthly_sim'] = pull_monthly_df['total_sim_count_pull']
pull_monthly_df.loc[pull_monthly_df['sq_type'] == 'ygo', 'ygo_monthly_sim'] = pull_monthly_df['total_sim_count_pull']
pull_monthly_df.loc[pull_monthly_df['sq_type'] == 'mixed', 'mix_monthly_sim'] = pull_monthly_df['total_sim_count_pull']

pull_monthly_df.loc[pull_monthly_df['sq_type'] == 'mtg', 'mtg_monthly_miss'] = pull_monthly_df['total_miss_count_pull']
pull_monthly_df.loc[pull_monthly_df['sq_type'] == 'pkm', 'pkm_monthly_miss'] = pull_monthly_df['total_miss_count_pull']
pull_monthly_df.loc[pull_monthly_df['sq_type'] == 'ygo', 'ygo_monthly_miss'] = pull_monthly_df['total_miss_count_pull']
pull_monthly_df.loc[pull_monthly_df['sq_type'] == 'mixed', 'mix_monthly_miss'] = pull_monthly_df['total_miss_count_pull']

pull_monthly_df.loc[pull_monthly_df['sq_type'] == 'mtg', 'mtg_monthly_unrec'] = pull_monthly_df['total_unrec_count_pull']
pull_monthly_df.loc[pull_monthly_df['sq_type'] == 'pkm', 'pkm_monthly_unrec'] = pull_monthly_df['total_unrec_count_pull']
pull_monthly_df.loc[pull_monthly_df['sq_type'] == 'ygo', 'ygo_monthly_unrec'] = pull_monthly_df['total_unrec_count_pull']
pull_monthly_df.loc[pull_monthly_df['sq_type'] == 'mixed', 'mix_monthly_unrec'] = pull_monthly_df['total_unrec_count_pull']

pull_monthly_df.loc[pull_monthly_df['sq_type'] == 'mtg', 'mtg_monthly_other'] = pull_monthly_df['total_other_count_pull']
pull_monthly_df.loc[pull_monthly_df['sq_type'] == 'pkm', 'pkm_monthly_other'] = pull_monthly_df['total_other_count_pull']
pull_monthly_df.loc[pull_monthly_df['sq_type'] == 'ygo', 'ygo_monthly_other'] = pull_monthly_df['total_other_count_pull']
pull_monthly_df.loc[pull_monthly_df['sq_type'] == 'mixed', 'mix_monthly_other'] = pull_monthly_df['total_other_count_pull']

#Mtg errors
pull_monthly_mtg_extra = pull_monthly_df.groupby('month_count')['mtg_monthly_extra'].sum()
pull_monthly_df = pd.merge(pull_monthly_df, pull_monthly_mtg_extra, how='right', on='month_count')

pull_monthly_mtg_sim = pull_monthly_df.groupby('month_count')['mtg_monthly_sim'].sum()
pull_monthly_df = pd.merge(pull_monthly_df, pull_monthly_mtg_sim, how='right', on='month_count')

pull_monthly_mtg_miss = pull_monthly_df.groupby('month_count')['mtg_monthly_miss'].sum()
pull_monthly_df = pd.merge(pull_monthly_df, pull_monthly_mtg_miss, how='right', on='month_count')

pull_monthly_mtg_unrec = pull_monthly_df.groupby('month_count')['mtg_monthly_unrec'].sum()
pull_monthly_df = pd.merge(pull_monthly_df, pull_monthly_mtg_unrec, how='right', on='month_count')

pull_monthly_mtg_other = pull_monthly_df.groupby('month_count')['mtg_monthly_other'].sum()
pull_monthly_df = pd.merge(pull_monthly_df, pull_monthly_mtg_other, how='right', on='month_count')

#Pkm errors
pull_monthly_pkm_extra = pull_monthly_df.groupby('month_count')['pkm_monthly_extra'].sum()
pull_monthly_df = pd.merge(pull_monthly_df, pull_monthly_pkm_extra, how='right', on='month_count')

pull_monthly_pkm_sim = pull_monthly_df.groupby('month_count')['pkm_monthly_sim'].sum()
pull_monthly_df = pd.merge(pull_monthly_df, pull_monthly_pkm_sim, how='right', on='month_count')

pull_monthly_pkm_miss = pull_monthly_df.groupby('month_count')['pkm_monthly_miss'].sum()
pull_monthly_df = pd.merge(pull_monthly_df, pull_monthly_pkm_miss, how='right', on='month_count')

pull_monthly_pkm_unrec = pull_monthly_df.groupby('month_count')['pkm_monthly_unrec'].sum()
pull_monthly_df = pd.merge(pull_monthly_df, pull_monthly_pkm_unrec, how='right', on='month_count')

pull_monthly_pkm_other = pull_monthly_df.groupby('month_count')['pkm_monthly_other'].sum()
pull_monthly_df = pd.merge(pull_monthly_df, pull_monthly_pkm_other, how='right', on='month_count')

#Ygo errors
pull_monthly_ygo_extra = pull_monthly_df.groupby('month_count')['ygo_monthly_extra'].sum()
pull_monthly_df = pd.merge(pull_monthly_df, pull_monthly_ygo_extra, how='right', on='month_count')

pull_monthly_ygo_sim = pull_monthly_df.groupby('month_count')['ygo_monthly_sim'].sum()
pull_monthly_df = pd.merge(pull_monthly_df, pull_monthly_ygo_sim, how='right', on='month_count')

pull_monthly_ygo_miss = pull_monthly_df.groupby('month_count')['ygo_monthly_miss'].sum()
pull_monthly_df = pd.merge(pull_monthly_df, pull_monthly_ygo_miss, how='right', on='month_count')

pull_monthly_ygo_unrec = pull_monthly_df.groupby('month_count')['ygo_monthly_unrec'].sum()
pull_monthly_df = pd.merge(pull_monthly_df, pull_monthly_ygo_unrec, how='right', on='month_count')

pull_monthly_ygo_other = pull_monthly_df.groupby('month_count')['ygo_monthly_other'].sum()
pull_monthly_df = pd.merge(pull_monthly_df, pull_monthly_ygo_other, how='right', on='month_count')

#Mixed errors
pull_monthly_mix_extra = pull_monthly_df.groupby('month_count')['mix_monthly_extra'].sum()
pull_monthly_df = pd.merge(pull_monthly_df, pull_monthly_mix_extra, how='right', on='month_count')

pull_monthly_mix_sim = pull_monthly_df.groupby('month_count')['mix_monthly_sim'].sum()
pull_monthly_df = pd.merge(pull_monthly_df, pull_monthly_mix_sim, how='right', on='month_count')

pull_monthly_mix_miss = pull_monthly_df.groupby('month_count')['mix_monthly_miss'].sum()
pull_monthly_df = pd.merge(pull_monthly_df, pull_monthly_mix_miss, how='right', on='month_count')

pull_monthly_mix_unrec = pull_monthly_df.groupby('month_count')['mix_monthly_unrec'].sum()
pull_monthly_df = pd.merge(pull_monthly_df, pull_monthly_mix_unrec, how='right', on='month_count')

pull_monthly_mix_other = pull_monthly_df.groupby('month_count')['mix_monthly_other'].sum()
pull_monthly_df = pd.merge(pull_monthly_df, pull_monthly_mix_other, how='right', on='month_count')

#Output
pull_monthly_card_output = pull_monthly_df.groupby('month_count')['Units'].sum()
pull_monthly_df = pd.merge(pull_monthly_df, pull_monthly_card_output, how='right', on='month_count')

pull_monthly_sq_output = pull_monthly_df.groupby('month_count')['SQ/POQ'].nunique()
pull_monthly_df = pd.merge(pull_monthly_df, pull_monthly_sq_output, how='right', on='month_count')

#SQ type count
pull_monthly_df["mtg_sq_count"] = 0
pull_monthly_df["pkm_sq_count"] = 0
pull_monthly_df["ygo_sq_count"] = 0
pull_monthly_df["mix_sq_count"] = 0

pull_monthly_df.loc[(pull_monthly_df['mtg'] == 1) & (pull_monthly_df['pkm'] == 0) & (pull_monthly_df['ygo'] == 0), 'mtg_sq_count'] = 1
pull_monthly_df.loc[(pull_monthly_df['mtg'] == 0) & (pull_monthly_df['pkm'] == 1) & (pull_monthly_df['ygo'] == 0), 'pkm_sq_count'] = 1
pull_monthly_df.loc[(pull_monthly_df['mtg'] == 0) & (pull_monthly_df['pkm'] == 0) & (pull_monthly_df['ygo'] == 1), 'ygo_sq_count'] = 1
pull_monthly_df.loc[pull_monthly_df['mixed'] > 1, 'mix_sq_count'] = 1

pull_monthly_mtg_sqs = pull_monthly_df.groupby('month_count')['mtg_sq_count'].sum()
pull_monthly_df = pd.merge(pull_monthly_df, pull_monthly_mtg_sqs, how='right', on='month_count')

pull_monthly_pkm_sqs = pull_monthly_df.groupby('month_count')['pkm_sq_count'].sum()
pull_monthly_df = pd.merge(pull_monthly_df, pull_monthly_pkm_sqs, how='right', on='month_count')

pull_monthly_ygo_sqs = pull_monthly_df.groupby('month_count')['ygo_sq_count'].sum()
pull_monthly_df = pd.merge(pull_monthly_df, pull_monthly_ygo_sqs, how='right', on='month_count')

pull_monthly_mix_sqs = pull_monthly_df.groupby('month_count')['mix_sq_count'].sum()
pull_monthly_df = pd.merge(pull_monthly_df, pull_monthly_mix_sqs, how='right', on='month_count')

#Parse down to single puller + month combinations
pull_monthly_df.drop_duplicates(subset=['month_count'], inplace=True)

#Rename columns
pull_monthly_df.rename(columns={'mtg_monthly_extra_y':'mtg_monthly_extra', 'mtg_monthly_sim_y':'mtg_monthly_sim', 'mtg_monthly_miss_y':'mtg_monthly_miss', 'mtg_monthly_unrec_y':'mtg_monthly_unrec', 'mtg_monthly_other_y':'mtg_monthly_other', 'pkm_monthly_extra_y':'pkm_monthly_extra', 'pkm_monthly_sim_y':'pkm_monthly_sim', 'pkm_monthly_miss_y':'pkm_monthly_miss', 'pkm_monthly_unrec_y':'pkm_monthly_unrec', 'pkm_monthly_other_y':'pkm_monthly_other', 'ygo_monthly_extra_y':'ygo_monthly_extra', 'ygo_monthly_sim_y':'ygo_monthly_sim', 'ygo_monthly_miss_y':'ygo_monthly_miss', 'ygo_monthly_unrec_y':'ygo_monthly_unrec', 'ygo_monthly_other_y':'ygo_monthly_other', 'mix_monthly_extra_y':'mix_monthly_extra', 'mix_monthly_sim_y':'mix_monthly_sim', 'mix_monthly_miss_y':'mix_monthly_miss', 'mix_monthly_unrec_y':'mix_monthly_unrec', 'mix_monthly_other_y':'mix_monthly_other', 'Units_y':'monthly_units', 'SQ/POQ_y':'monthly_sqs_pulled', 'First_Offset':'monthly_first_offset', 'pull_operator_tenure':'monthly_pull_operator_tenure', 'mtg_sq_count_y':'monthly_mtg_sq_count', 'pkm_sq_count_y':'monthly_pkm_sq_count', 'ygo_sq_count_y':'monthly_ygo_sq_count', 'mix_sq_count_y':'monthly_mix_sq_count'}, inplace=True)

#Total error amounts
pull_monthly_df["monthly_extra"] = pull_monthly_df['mtg_monthly_extra'].astype('float64') + pull_monthly_df['pkm_monthly_extra'].astype('float64') + pull_monthly_df['ygo_monthly_extra'].astype('float64') + pull_monthly_df['mix_monthly_extra'].astype('float64')

pull_monthly_df["monthly_sim"] = pull_monthly_df['mtg_monthly_sim'].astype('float64') + pull_monthly_df['pkm_monthly_sim'].astype('float64') + pull_monthly_df['ygo_monthly_sim'].astype('float64') + pull_monthly_df['mix_monthly_sim'].astype('float64')

pull_monthly_df["monthly_miss"] = pull_monthly_df['mtg_monthly_miss'].astype('float64') + pull_monthly_df['pkm_monthly_miss'].astype('float64') + pull_monthly_df['ygo_monthly_miss'].astype('float64') + pull_monthly_df['mix_monthly_miss'].astype('float64')

pull_monthly_df["monthly_unrec"] = pull_monthly_df['mtg_monthly_unrec'].astype('float64') + pull_monthly_df['pkm_monthly_unrec'].astype('float64') + pull_monthly_df['ygo_monthly_unrec'].astype('float64') + pull_monthly_df['mix_monthly_unrec'].astype('float64')

pull_monthly_df["monthly_other"] = pull_monthly_df['mtg_monthly_other'].astype('float64') + pull_monthly_df['pkm_monthly_other'].astype('float64') + pull_monthly_df['ygo_monthly_other'].astype('float64') + pull_monthly_df['mix_monthly_other'].astype('float64')

#Drop extraneous columns
pull_monthly_df.drop(['mtg_monthly_extra_x', 'mtg_monthly_sim_x', 'mtg_monthly_miss_x', 'mtg_monthly_unrec_x', 'mtg_monthly_other_x', 'pkm_monthly_extra_x', 'pkm_monthly_sim_x', 'pkm_monthly_miss_x', 'pkm_monthly_unrec_x', 'pkm_monthly_other_x', 'ygo_monthly_extra_x', 'ygo_monthly_sim_x', 'ygo_monthly_miss_x', 'ygo_monthly_unrec_x', 'ygo_monthly_other_x', 'mix_monthly_extra_x', 'mix_monthly_sim_x', 'mix_monthly_miss_x', 'mix_monthly_unrec_x', 'mix_monthly_other_x', 'Units_x', 'SQ/POQ_x','total_extra_count_pull', 'total_miss_count_pull', 'total_sim_count_pull', 'total_unrec_count_pull', 'total_other_count_pull', 'mtg_sq_count_x', 'pkm_sq_count_x', 'ygo_sq_count_x', 'mix_sq_count_x', 'mtg', 'pkm', 'ygo', 'mixed', 'clean_sq', 'punch_normalized', 'sq_type', 'pull_day_of_week', 'month_count', 'week_count', 'pullver_sq', 'er_res_sq', 'pvp_sq'],axis=1, inplace=True)

##Weekly Errors and output (Pull)
pull_weekly_df = pull_df.copy()
pull_weekly_df['Units'] = pull_weekly_df['Units'] .astype('float64')

pull_weekly_df["mtg_weekly_extra"] = 0
pull_weekly_df["mtg_weekly_sim"] = 0
pull_weekly_df["mtg_weekly_miss"] = 0
pull_weekly_df["mtg_weekly_unrec"] = 0
pull_weekly_df["mtg_weekly_other"] = 0

pull_weekly_df["pkm_weekly_extra"] = 0
pull_weekly_df["pkm_weekly_sim"] = 0
pull_weekly_df["pkm_weekly_miss"] = 0
pull_weekly_df["pkm_weekly_unrec"] = 0
pull_weekly_df["pkm_weekly_other"] = 0

pull_weekly_df["ygo_weekly_extra"] = 0
pull_weekly_df["ygo_weekly_sim"] = 0
pull_weekly_df["ygo_weekly_miss"] = 0
pull_weekly_df["ygo_weekly_unrec"] = 0
pull_weekly_df["ygo_weekly_other"] = 0

pull_weekly_df["mix_weekly_extra"] = 0
pull_weekly_df["mix_weekly_sim"] = 0
pull_weekly_df["mix_weekly_miss"] = 0
pull_weekly_df["mix_weekly_unrec"] = 0
pull_weekly_df["mix_weekly_other"] = 0


pull_weekly_df.loc[pull_weekly_df['sq_type'] == 'mtg', 'mtg_weekly_extra'] = pull_weekly_df['total_extra_count_pull']
pull_weekly_df.loc[pull_weekly_df['sq_type'] == 'pkm', 'pkm_weekly_extra'] = pull_weekly_df['total_extra_count_pull']
pull_weekly_df.loc[pull_weekly_df['sq_type'] == 'ygo', 'ygo_weekly_extra'] = pull_weekly_df['total_extra_count_pull']
pull_weekly_df.loc[pull_weekly_df['sq_type'] == 'mixed', 'mix_weekly_extra'] = pull_weekly_df['total_extra_count_pull']

pull_weekly_df.loc[pull_weekly_df['sq_type'] == 'mtg', 'mtg_weekly_sim'] = pull_weekly_df['total_sim_count_pull']
pull_weekly_df.loc[pull_weekly_df['sq_type'] == 'pkm', 'pkm_weekly_sim'] = pull_weekly_df['total_sim_count_pull']
pull_weekly_df.loc[pull_weekly_df['sq_type'] == 'ygo', 'ygo_weekly_sim'] = pull_weekly_df['total_sim_count_pull']
pull_weekly_df.loc[pull_weekly_df['sq_type'] == 'mixed', 'mix_weekly_sim'] = pull_weekly_df['total_sim_count_pull']

pull_weekly_df.loc[pull_weekly_df['sq_type'] == 'mtg', 'mtg_weekly_miss'] = pull_weekly_df['total_miss_count_pull']
pull_weekly_df.loc[pull_weekly_df['sq_type'] == 'pkm', 'pkm_weekly_miss'] = pull_weekly_df['total_miss_count_pull']
pull_weekly_df.loc[pull_weekly_df['sq_type'] == 'ygo', 'ygo_weekly_miss'] = pull_weekly_df['total_miss_count_pull']
pull_weekly_df.loc[pull_weekly_df['sq_type'] == 'mixed', 'mix_weekly_miss'] = pull_weekly_df['total_miss_count_pull']

pull_weekly_df.loc[pull_weekly_df['sq_type'] == 'mtg', 'mtg_weekly_unrec'] = pull_weekly_df['total_unrec_count_pull']
pull_weekly_df.loc[pull_weekly_df['sq_type'] == 'pkm', 'pkm_weekly_unrec'] = pull_weekly_df['total_unrec_count_pull']
pull_weekly_df.loc[pull_weekly_df['sq_type'] == 'ygo', 'ygo_weekly_unrec'] = pull_weekly_df['total_unrec_count_pull']
pull_weekly_df.loc[pull_weekly_df['sq_type'] == 'mixed', 'mix_weekly_unrec'] = pull_weekly_df['total_unrec_count_pull']

pull_weekly_df.loc[pull_weekly_df['sq_type'] == 'mtg', 'mtg_weekly_other'] = pull_weekly_df['total_other_count_pull']
pull_weekly_df.loc[pull_weekly_df['sq_type'] == 'pkm', 'pkm_weekly_other'] = pull_weekly_df['total_other_count_pull']
pull_weekly_df.loc[pull_weekly_df['sq_type'] == 'ygo', 'ygo_weekly_other'] = pull_weekly_df['total_other_count_pull']
pull_weekly_df.loc[pull_weekly_df['sq_type'] == 'mixed', 'mix_weekly_other'] = pull_weekly_df['total_other_count_pull']

#Mtg errors
pull_weekly_mtg_extra = pull_weekly_df.groupby('week_count')['mtg_weekly_extra'].sum()
pull_weekly_df = pd.merge(pull_weekly_df, pull_weekly_mtg_extra, how='right', on='week_count')

pull_weekly_mtg_sim = pull_weekly_df.groupby('week_count')['mtg_weekly_sim'].sum()
pull_weekly_df = pd.merge(pull_weekly_df, pull_weekly_mtg_sim, how='right', on='week_count')

pull_weekly_mtg_miss = pull_weekly_df.groupby('week_count')['mtg_weekly_miss'].sum()
pull_weekly_df = pd.merge(pull_weekly_df, pull_weekly_mtg_miss, how='right', on='week_count')

pull_weekly_mtg_unrec = pull_weekly_df.groupby('week_count')['mtg_weekly_unrec'].sum()
pull_weekly_df = pd.merge(pull_weekly_df, pull_weekly_mtg_unrec, how='right', on='week_count')

pull_weekly_mtg_other = pull_weekly_df.groupby('week_count')['mtg_weekly_other'].sum()
pull_weekly_df = pd.merge(pull_weekly_df, pull_weekly_mtg_other, how='right', on='week_count')

#Pkm errors
pull_weekly_pkm_extra = pull_weekly_df.groupby('week_count')['pkm_weekly_extra'].sum()
pull_weekly_df = pd.merge(pull_weekly_df, pull_weekly_pkm_extra, how='right', on='week_count')

pull_weekly_pkm_sim = pull_weekly_df.groupby('week_count')['pkm_weekly_sim'].sum()
pull_weekly_df = pd.merge(pull_weekly_df, pull_weekly_pkm_sim, how='right', on='week_count')

pull_weekly_pkm_miss = pull_weekly_df.groupby('week_count')['pkm_weekly_miss'].sum()
pull_weekly_df = pd.merge(pull_weekly_df, pull_weekly_pkm_miss, how='right', on='week_count')

pull_weekly_pkm_unrec = pull_weekly_df.groupby('week_count')['pkm_weekly_unrec'].sum()
pull_weekly_df = pd.merge(pull_weekly_df, pull_weekly_pkm_unrec, how='right', on='week_count')

pull_weekly_pkm_other = pull_weekly_df.groupby('week_count')['pkm_weekly_other'].sum()
pull_weekly_df = pd.merge(pull_weekly_df, pull_weekly_pkm_other, how='right', on='week_count')

#Ygo errors
pull_weekly_ygo_extra = pull_weekly_df.groupby('week_count')['ygo_weekly_extra'].sum()
pull_weekly_df = pd.merge(pull_weekly_df, pull_weekly_ygo_extra, how='right', on='week_count')

pull_weekly_ygo_sim = pull_weekly_df.groupby('week_count')['ygo_weekly_sim'].sum()
pull_weekly_df = pd.merge(pull_weekly_df, pull_weekly_ygo_sim, how='right', on='week_count')

pull_weekly_ygo_miss = pull_weekly_df.groupby('week_count')['ygo_weekly_miss'].sum()
pull_weekly_df = pd.merge(pull_weekly_df, pull_weekly_ygo_miss, how='right', on='week_count')

pull_weekly_ygo_unrec = pull_weekly_df.groupby('week_count')['ygo_weekly_unrec'].sum()
pull_weekly_df = pd.merge(pull_weekly_df, pull_weekly_ygo_unrec, how='right', on='week_count')

pull_weekly_ygo_other = pull_weekly_df.groupby('week_count')['ygo_weekly_other'].sum()
pull_weekly_df = pd.merge(pull_weekly_df, pull_weekly_ygo_other, how='right', on='week_count')

#Mixed errors
pull_weekly_mix_extra = pull_weekly_df.groupby('week_count')['mix_weekly_extra'].sum()
pull_weekly_df = pd.merge(pull_weekly_df, pull_weekly_mix_extra, how='right', on='week_count')

pull_weekly_mix_sim = pull_weekly_df.groupby('week_count')['mix_weekly_sim'].sum()
pull_weekly_df = pd.merge(pull_weekly_df, pull_weekly_mix_sim, how='right', on='week_count')

pull_weekly_mix_miss = pull_weekly_df.groupby('week_count')['mix_weekly_miss'].sum()
pull_weekly_df = pd.merge(pull_weekly_df, pull_weekly_mix_miss, how='right', on='week_count')

pull_weekly_mix_unrec = pull_weekly_df.groupby('week_count')['mix_weekly_unrec'].sum()
pull_weekly_df = pd.merge(pull_weekly_df, pull_weekly_mix_unrec, how='right', on='week_count')

pull_weekly_mix_other = pull_weekly_df.groupby('week_count')['mix_weekly_other'].sum()
pull_weekly_df = pd.merge(pull_weekly_df, pull_weekly_mix_other, how='right', on='week_count')

#Output
pull_weekly_card_output = pull_weekly_df.groupby('week_count')['Units'].sum()
pull_weekly_df = pd.merge(pull_weekly_df, pull_weekly_card_output, how='right', on='week_count')

pull_weekly_sq_output = pull_weekly_df.groupby('week_count')['SQ/POQ'].nunique()
pull_weekly_df = pd.merge(pull_weekly_df, pull_weekly_sq_output, how='right', on='week_count')

#SQ type count
pull_weekly_df["mtg_sq_count"] = 0
pull_weekly_df["pkm_sq_count"] = 0
pull_weekly_df["ygo_sq_count"] = 0
pull_weekly_df["mix_sq_count"] = 0

pull_weekly_df.loc[(pull_weekly_df['mtg'] == 1) & (pull_weekly_df['pkm'] == 0) & (pull_weekly_df['ygo'] == 0), 'mtg_sq_count'] = 1
pull_weekly_df.loc[(pull_weekly_df['mtg'] == 0) & (pull_weekly_df['pkm'] == 1) & (pull_weekly_df['ygo'] == 0), 'pkm_sq_count'] = 1
pull_weekly_df.loc[(pull_weekly_df['mtg'] == 0) & (pull_weekly_df['pkm'] == 0) & (pull_weekly_df['ygo'] == 1), 'ygo_sq_count'] = 1
pull_weekly_df.loc[pull_weekly_df['mixed'] > 1, 'mix_sq_count'] = 1

pull_weekly_mtg_sqs = pull_weekly_df.groupby('week_count')['mtg_sq_count'].sum()
pull_weekly_df = pd.merge(pull_weekly_df, pull_weekly_mtg_sqs, how='right', on='week_count')

pull_weekly_pkm_sqs = pull_weekly_df.groupby('week_count')['pkm_sq_count'].sum()
pull_weekly_df = pd.merge(pull_weekly_df, pull_weekly_pkm_sqs, how='right', on='week_count')

pull_weekly_ygo_sqs = pull_weekly_df.groupby('week_count')['ygo_sq_count'].sum()
pull_weekly_df = pd.merge(pull_weekly_df, pull_weekly_ygo_sqs, how='right', on='week_count')

pull_weekly_mix_sqs = pull_weekly_df.groupby('week_count')['mix_sq_count'].sum()
pull_weekly_df = pd.merge(pull_weekly_df, pull_weekly_mix_sqs, how='right', on='week_count')

#Parse down to single puller + week combinations
pull_weekly_df.drop_duplicates(subset=['week_count'], inplace=True)

#Rename columns
pull_weekly_df.rename(columns={'mtg_weekly_extra_y':'mtg_weekly_extra', 'mtg_weekly_sim_y':'mtg_weekly_sim', 'mtg_weekly_miss_y':'mtg_weekly_miss', 'mtg_weekly_unrec_y':'mtg_weekly_unrec', 'mtg_weekly_other_y':'mtg_weekly_other', 'pkm_weekly_extra_y':'pkm_weekly_extra', 'pkm_weekly_sim_y':'pkm_weekly_sim', 'pkm_weekly_miss_y':'pkm_weekly_miss', 'pkm_weekly_unrec_y':'pkm_weekly_unrec', 'pkm_weekly_other_y':'pkm_weekly_other', 'ygo_weekly_extra_y':'ygo_weekly_extra', 'ygo_weekly_sim_y':'ygo_weekly_sim', 'ygo_weekly_miss_y':'ygo_weekly_miss', 'ygo_weekly_unrec_y':'ygo_weekly_unrec', 'ygo_weekly_other_y':'ygo_weekly_other', 'mix_weekly_extra_y':'mix_weekly_extra', 'mix_weekly_sim_y':'mix_weekly_sim', 'mix_weekly_miss_y':'mix_weekly_miss', 'mix_weekly_unrec_y':'mix_weekly_unrec', 'mix_weekly_other_y':'mix_weekly_other', 'Units_y':'weekly_units', 'SQ/POQ_y':'weekly_sqs_pulled', 'First_Offset':'weekly_first_offset', 'pull_operator_tenure':'weekly_pull_operator_tenure', 'mtg_sq_count_y':'weekly_mtg_sq_count', 'pkm_sq_count_y':'weekly_pkm_sq_count', 'ygo_sq_count_y':'weekly_ygo_sq_count', 'mix_sq_count_y':'weekly_mix_sq_count'}, inplace=True)

#Total error amounts
pull_weekly_df["weekly_extra"] = pull_weekly_df['mtg_weekly_extra'].astype('float64') + pull_weekly_df['pkm_weekly_extra'].astype('float64') + pull_weekly_df['ygo_weekly_extra'].astype('float64') + pull_weekly_df['mix_weekly_extra'].astype('float64')

pull_weekly_df["weekly_sim"] = pull_weekly_df['mtg_weekly_sim'].astype('float64') + pull_weekly_df['pkm_weekly_sim'].astype('float64') + pull_weekly_df['ygo_weekly_sim'].astype('float64') + pull_weekly_df['mix_weekly_sim'].astype('float64')

pull_weekly_df["weekly_miss"] = pull_weekly_df['mtg_weekly_miss'].astype('float64') + pull_weekly_df['pkm_weekly_miss'].astype('float64') + pull_weekly_df['ygo_weekly_miss'].astype('float64') + pull_weekly_df['mix_weekly_miss'].astype('float64')

pull_weekly_df["weekly_unrec"] = pull_weekly_df['mtg_weekly_unrec'].astype('float64') + pull_weekly_df['pkm_weekly_unrec'].astype('float64') + pull_weekly_df['ygo_weekly_unrec'].astype('float64') + pull_weekly_df['mix_weekly_unrec'].astype('float64')

pull_weekly_df["weekly_other"] = pull_weekly_df['mtg_weekly_other'].astype('float64') + pull_weekly_df['pkm_weekly_other'].astype('float64') + pull_weekly_df['ygo_weekly_other'].astype('float64') + pull_weekly_df['mix_weekly_other'].astype('float64')

#Drop extraneous columns
pull_weekly_df.drop(['mtg_weekly_extra_x', 'mtg_weekly_sim_x', 'mtg_weekly_miss_x', 'mtg_weekly_unrec_x', 'mtg_weekly_other_x', 'pkm_weekly_extra_x', 'pkm_weekly_sim_x', 'pkm_weekly_miss_x', 'pkm_weekly_unrec_x', 'pkm_weekly_other_x', 'ygo_weekly_extra_x', 'ygo_weekly_sim_x', 'ygo_weekly_miss_x', 'ygo_weekly_unrec_x', 'ygo_weekly_other_x', 'mix_weekly_extra_x', 'mix_weekly_sim_x', 'mix_weekly_miss_x', 'mix_weekly_unrec_x', 'mix_weekly_other_x', 'Units_x', 'SQ/POQ_x','total_extra_count_pull', 'total_miss_count_pull', 'total_sim_count_pull', 'total_unrec_count_pull', 'total_other_count_pull', 'mtg_sq_count_x', 'pkm_sq_count_x', 'ygo_sq_count_x', 'mix_sq_count_x', 'mtg', 'pkm', 'ygo', 'mixed', 'clean_sq', 'punch_normalized', 'sq_type', 'pull_day_of_week', 'week_count', 'week_count', 'pullver_sq', 'er_res_sq', 'pvp_sq'],axis=1, inplace=True)

##Combine monthly and weekly frames
pull_weekly_df["combined"] = pull_weekly_df['Punch'].astype(str) + pull_weekly_df['Puncher'].astype(str)
pull_monthly_df["combined"] = pull_monthly_df['Punch'].astype(str) + pull_monthly_df['Puncher'].astype(str)

pull_combined_df = pd.DataFrame()

pull_combined_df = pd.merge(pull_weekly_df, pull_monthly_df, how='left', on='combined')

pull_combined_df.rename(columns={'Puncher_x':'Puncher', 'Supervisor_x':'Supervisor', 'pull_week_of_x':'pull_week_of', 'pull_month_of_y':'pull_month_of'}, inplace=True)

pull_combined_df = pull_combined_df[[
'Puncher',
'Supervisor',
'pull_week_of',
'pull_month_of',
'weekly_pull_operator_tenure',
'weekly_units',
'weekly_sqs_pulled',
'weekly_mtg_sq_count',
'weekly_pkm_sq_count',
'weekly_ygo_sq_count',
'weekly_mix_sq_count',
'mtg_weekly_extra',
'mtg_weekly_sim',
'mtg_weekly_miss',
'mtg_weekly_unrec',
'mtg_weekly_other',
'pkm_weekly_extra',
'pkm_weekly_sim',
'pkm_weekly_miss',
'pkm_weekly_unrec',
'pkm_weekly_other',
'ygo_weekly_extra',
'ygo_weekly_sim',
'ygo_weekly_miss',
'ygo_weekly_unrec',
'ygo_weekly_other',
'mix_weekly_extra',
'mix_weekly_sim',
'mix_weekly_miss',
'mix_weekly_unrec',
'mix_weekly_other',
'monthly_units',
'monthly_sqs_pulled',
'monthly_mtg_sq_count',
'monthly_pkm_sq_count',
'monthly_ygo_sq_count',
'monthly_mix_sq_count',
'mtg_monthly_extra',
'mtg_monthly_sim',
'mtg_monthly_miss',
'mtg_monthly_unrec',
'mtg_monthly_other',
'pkm_monthly_extra',
'pkm_monthly_sim',
'pkm_monthly_miss',
'pkm_monthly_unrec',
'pkm_monthly_other',
'ygo_monthly_extra',
'ygo_monthly_sim',
'ygo_monthly_miss',
'ygo_monthly_unrec',
'ygo_monthly_other',
'mix_monthly_extra',
'mix_monthly_sim',
'mix_monthly_miss',
'mix_monthly_unrec',
'mix_monthly_other']]

##SQ Game Types (PullVer)
pullver_df["count"] = pullver_df['Punch'].astype(str) + pullver_df['Puncher'].astype(str) + pullver_df['GAME_NAME'].astype(str)
pullver_df.drop_duplicates(subset=['count'], inplace=True)
pullver_df.drop('count', axis=1, inplace=True)
pullver_df["count"] = pullver_df['Punch'].astype(str) + pullver_df['Puncher'].astype(str)

pullver_df["mtg"] = 0
pullver_df["pkm"] = 0
pullver_df["ygo"] = 0
pullver_df.loc[pullver_df['GAME_NAME'] == 'Magic', 'mtg'] = 1
pullver_df.loc[pullver_df['GAME_NAME'] == 'Pokemon', 'pkm'] = 1
pullver_df.loc[pullver_df['GAME_NAME'] == 'YuGiOh', 'ygo'] = 1

pullver_df['mtg'] = pullver_df['mtg'].astype('float64')
pullver_df['pkm'] = pullver_df['pkm'].astype('float64')
pullver_df['ygo'] = pullver_df['ygo'].astype('float64')

pullver_mtg = pullver_df.groupby('count')['mtg'].sum()
pullver_df = pd.merge(pullver_df, pullver_mtg, how='right', on='count')
pullver_df.drop('mtg_x', axis=1, inplace=True)
pullver_df.rename(columns={'mtg_y':'mtg'}, inplace=True)

pullver_pkm = pullver_df.groupby('count')['pkm'].sum()
pullver_df = pd.merge(pullver_df, pullver_pkm, how='right', on='count')
pullver_df.drop('pkm_x', axis=1, inplace=True)
pullver_df.rename(columns={'pkm_y':'pkm'}, inplace=True)

pullver_ygo = pullver_df.groupby('count')['ygo'].sum()
pullver_df = pd.merge(pullver_df, pullver_ygo, how='right', on='count')
pullver_df.drop('ygo_x', axis=1, inplace=True)
pullver_df.rename(columns={'ygo_y':'ygo'}, inplace=True)

pullver_df.drop_duplicates(subset=['count'], inplace=True)

pullver_df["mixed"] = pullver_df['mtg'] + pullver_df['pkm'] + pullver_df['ygo']

pullver_df["sq_type"] = ""

pullver_df.loc[pullver_df['mtg'] == 1, 'sq_type'] = "mtg"
pullver_df.loc[pullver_df['pkm'] == 1, 'sq_type'] = "pkm"
pullver_df.loc[pullver_df['ygo'] == 1, 'sq_type'] = "ygo"
pullver_df.loc[pullver_df['mixed'] > 1, 'sq_type'] = "mixed"

pullver_df.drop(['GAME_NAME', 'count', 'PCID', 'SLOT', 'slot_parse', 'Orders', 'sq_card_quantity', 'QUEUE_NUMBER', 'ORDER_COUNT', 'CREATED_AT', 'CARD_QUANTITY', 'Extra', 'Missing', 'Similar', 'Unrecorded', 'Other'], axis=1, inplace=True)

##Remove dupes and aggragate remaining dupes of SQ Numbers
pullver_df["dupe"] = pullver_df['Punch'].astype(str) + pullver_df['Puncher'].astype(str) + pullver_df['SQ/POQ'].astype(str)
pullver_df.drop_duplicates(subset=['dupe'], inplace=True)
pullver_df.drop('dupe', axis=1, inplace=True)
pullver_df["dupe"] = pullver_df['Puncher'].astype(str) + pullver_df['SQ/POQ'].astype(str)

pullver_df['Units'] = pullver_df['Units'].astype('float64')

sq_cards = pullver_df.groupby('dupe')['Units'].sum()
pullver_df = pd.merge(pullver_df, sq_cards, how='right', on='dupe')
pullver_df.drop('Units_x', axis=1, inplace=True)
pullver_df.rename(columns={'Units_y':'Units'}, inplace=True)
pullver_df.drop_duplicates(subset=['dupe'], inplace=True)

##Aggragate errors per sq and puncher combo (PullVer)
pullver_df["count"] = pullver_df['Puncher'].astype(str) + pullver_df['SQ/POQ'].astype(str)

#er res
er_res_extra_count_pullver = pullver_df.groupby('count')['total_er_res_extra'].sum()
pullver_df = pd.merge(pullver_df, er_res_extra_count_pullver, how='right', on='count')

er_res_miss_count_pullver = pullver_df.groupby('count')['total_er_res_miss'].sum()
pullver_df = pd.merge(pullver_df, er_res_miss_count_pullver, how='right', on='count')

er_res_sim_count_pullver = pullver_df.groupby('count')['total_er_res_sim'].sum()
pullver_df = pd.merge(pullver_df, er_res_sim_count_pullver, how='right', on='count')

#pvp
pvp_extra_count_pullver = pullver_df.groupby('count')['total_pvp_extra'].sum()
pullver_df = pd.merge(pullver_df, pvp_extra_count_pullver, how='right', on='count')

pvp_miss_count_pullver = pullver_df.groupby('count')['total_pvp_miss'].sum()
pullver_df = pd.merge(pullver_df, pvp_miss_count_pullver, how='right', on='count')

pvp_sim_count_pullver = pullver_df.groupby('count')['total_pvp_sim'].sum()
pullver_df = pd.merge(pullver_df, pvp_sim_count_pullver, how='right', on='count')

pvp_unrec_count_pullver = pullver_df.groupby('count')['total_pvp_unrec'].sum()
pullver_df = pd.merge(pullver_df, pvp_unrec_count_pullver, how='right', on='count')

pvp_other_count_pullver = pullver_df.groupby('count')['total_pvp_other'].sum()
pullver_df = pd.merge(pullver_df, pvp_other_count_pullver, how='right', on='count')

#total errors
pullver_df["total_extra_count_pullver"] = pullver_df['total_er_res_extra_y'].astype('float64') + pullver_df['total_pvp_extra_y'].astype('float64')

pullver_df["total_miss_count_pullver"] = pullver_df['total_er_res_miss_y'].astype('float64') + pullver_df['total_pvp_miss_y'].astype('float64')

pullver_df["total_sim_count_pullver"] = pullver_df['total_er_res_sim_y'].astype('float64') + pullver_df['total_pvp_sim_y'].astype('float64')

pullver_df["total_unrec_count_pullver"] = pullver_df['total_pvp_unrec_y'].astype('float64')

pullver_df["total_other_count_pullver"] = pullver_df['total_pvp_other_y'].astype('float64')

#drop extraneous columns
pullver_df.drop(['total_er_res_extra_x', 'total_er_res_miss_x', 'total_er_res_sim_x', 'total_er_res_extra_y', 'total_er_res_sim_y', 'total_er_res_miss_y', 'total_pvp_extra_x', 'total_pvp_miss_x', 'total_pvp_sim_x', 'total_pvp_unrec_x', 'total_pvp_other_x', 'total_pvp_extra_y', 'total_pvp_miss_y',  'total_pvp_sim_y', 'total_pvp_unrec_y', 'total_pvp_other_y', 'count', 'dupe'], axis=1, inplace=True)

##Month and week (PullVer)
pullver_df['Punch'] = pd.to_datetime(pullver_df['Punch'])
pullver_df["pullver_day_of_week"] = pullver_df['Punch'].dt.dayofweek
pullver_df["pullver_week_of"] = pullver_df['Punch'] - pd.to_timedelta(pullver_df['pullver_day_of_week'], unit='D')

pullver_df["punch_normalized"] = pd.to_datetime(pullver_df['Punch']).dt.date
pullver_df['pullver_week_of'] = pd.to_datetime(pullver_df['pullver_week_of']).dt.date

pullver_df["pullver_month_of"] = pullver_df['punch_normalized'].apply(lambda x: x.strftime('%Y-%m-01'))
pullver_df["pullver_operator_tenure"] = (pullver_df['punch_normalized'] - pd.to_datetime(pullver_df['Start Date']).dt.date)/np.timedelta64(1, 'D')/30

pullver_df['month_count'] = pullver_df['Puncher'].astype(str) + pullver_df['pullver_month_of'].astype(str)
pullver_df['week_count'] = pullver_df['Puncher'].astype(str) + pullver_df['pullver_week_of'].astype(str)

##Monthly Errors and output (PullVer)
pullver_monthly_df = pullver_df.copy()
pullver_monthly_df['Units'] = pullver_monthly_df['Units'] .astype('float64')

pullver_monthly_df["mtg_monthly_extra"] = 0
pullver_monthly_df["mtg_monthly_sim"] = 0
pullver_monthly_df["mtg_monthly_miss"] = 0
pullver_monthly_df["mtg_monthly_unrec"] = 0
pullver_monthly_df["mtg_monthly_other"] = 0

pullver_monthly_df["pkm_monthly_extra"] = 0
pullver_monthly_df["pkm_monthly_sim"] = 0
pullver_monthly_df["pkm_monthly_miss"] = 0
pullver_monthly_df["pkm_monthly_unrec"] = 0
pullver_monthly_df["pkm_monthly_other"] = 0

pullver_monthly_df["ygo_monthly_extra"] = 0
pullver_monthly_df["ygo_monthly_sim"] = 0
pullver_monthly_df["ygo_monthly_miss"] = 0
pullver_monthly_df["ygo_monthly_unrec"] = 0
pullver_monthly_df["ygo_monthly_other"] = 0

pullver_monthly_df["mix_monthly_extra"] = 0
pullver_monthly_df["mix_monthly_sim"] = 0
pullver_monthly_df["mix_monthly_miss"] = 0
pullver_monthly_df["mix_monthly_unrec"] = 0
pullver_monthly_df["mix_monthly_other"] = 0

pullver_monthly_df.loc[pullver_monthly_df['sq_type'] == 'mtg', 'mtg_monthly_extra'] = pullver_monthly_df['total_extra_count_pullver']
pullver_monthly_df.loc[pullver_monthly_df['sq_type'] == 'pkm', 'pkm_monthly_extra'] = pullver_monthly_df['total_extra_count_pullver']
pullver_monthly_df.loc[pullver_monthly_df['sq_type'] == 'ygo', 'ygo_monthly_extra'] = pullver_monthly_df['total_extra_count_pullver']
pullver_monthly_df.loc[pullver_monthly_df['sq_type'] == 'mixed', 'mix_monthly_extra'] = pullver_monthly_df['total_extra_count_pullver']

pullver_monthly_df.loc[pullver_monthly_df['sq_type'] == 'mtg', 'mtg_monthly_sim'] = pullver_monthly_df['total_sim_count_pullver']
pullver_monthly_df.loc[pullver_monthly_df['sq_type'] == 'pkm', 'pkm_monthly_sim'] = pullver_monthly_df['total_sim_count_pullver']
pullver_monthly_df.loc[pullver_monthly_df['sq_type'] == 'ygo', 'ygo_monthly_sim'] = pullver_monthly_df['total_sim_count_pullver']
pullver_monthly_df.loc[pullver_monthly_df['sq_type'] == 'mixed', 'mix_monthly_sim'] = pullver_monthly_df['total_sim_count_pullver']

pullver_monthly_df.loc[pullver_monthly_df['sq_type'] == 'mtg', 'mtg_monthly_miss'] = pullver_monthly_df['total_miss_count_pullver']
pullver_monthly_df.loc[pullver_monthly_df['sq_type'] == 'pkm', 'pkm_monthly_miss'] = pullver_monthly_df['total_miss_count_pullver']
pullver_monthly_df.loc[pullver_monthly_df['sq_type'] == 'ygo', 'ygo_monthly_miss'] = pullver_monthly_df['total_miss_count_pullver']
pullver_monthly_df.loc[pullver_monthly_df['sq_type'] == 'mixed', 'mix_monthly_miss'] = pullver_monthly_df['total_miss_count_pullver']

pullver_monthly_df.loc[pullver_monthly_df['sq_type'] == 'mtg', 'mtg_monthly_unrec'] = pullver_monthly_df['total_unrec_count_pullver']
pullver_monthly_df.loc[pullver_monthly_df['sq_type'] == 'pkm', 'pkm_monthly_unrec'] = pullver_monthly_df['total_unrec_count_pullver']
pullver_monthly_df.loc[pullver_monthly_df['sq_type'] == 'ygo', 'ygo_monthly_unrec'] = pullver_monthly_df['total_unrec_count_pullver']
pullver_monthly_df.loc[pullver_monthly_df['sq_type'] == 'mixed', 'mix_monthly_unrec'] = pullver_monthly_df['total_unrec_count_pullver']

pullver_monthly_df.loc[pullver_monthly_df['sq_type'] == 'mtg', 'mtg_monthly_other'] = pullver_monthly_df['total_other_count_pullver']
pullver_monthly_df.loc[pullver_monthly_df['sq_type'] == 'pkm', 'pkm_monthly_other'] = pullver_monthly_df['total_other_count_pullver']
pullver_monthly_df.loc[pullver_monthly_df['sq_type'] == 'ygo', 'ygo_monthly_other'] = pullver_monthly_df['total_other_count_pullver']
pullver_monthly_df.loc[pullver_monthly_df['sq_type'] == 'mixed', 'mix_monthly_other'] = pullver_monthly_df['total_other_count_pullver']

#Mtg errors
pullver_monthly_mtg_extra = pullver_monthly_df.groupby('month_count')['mtg_monthly_extra'].sum()
pullver_monthly_df = pd.merge(pullver_monthly_df, pullver_monthly_mtg_extra, how='right', on='month_count')

pullver_monthly_mtg_sim = pullver_monthly_df.groupby('month_count')['mtg_monthly_sim'].sum()
pullver_monthly_df = pd.merge(pullver_monthly_df, pullver_monthly_mtg_sim, how='right', on='month_count')

pullver_monthly_mtg_miss = pullver_monthly_df.groupby('month_count')['mtg_monthly_miss'].sum()
pullver_monthly_df = pd.merge(pullver_monthly_df, pullver_monthly_mtg_miss, how='right', on='month_count')

pullver_monthly_mtg_unrec = pullver_monthly_df.groupby('month_count')['mtg_monthly_unrec'].sum()
pullver_monthly_df = pd.merge(pullver_monthly_df, pullver_monthly_mtg_unrec, how='right', on='month_count')

pullver_monthly_mtg_other = pullver_monthly_df.groupby('month_count')['mtg_monthly_other'].sum()
pullver_monthly_df = pd.merge(pullver_monthly_df, pullver_monthly_mtg_other, how='right', on='month_count')

#Pkm errors
pullver_monthly_pkm_extra = pullver_monthly_df.groupby('month_count')['pkm_monthly_extra'].sum()
pullver_monthly_df = pd.merge(pullver_monthly_df, pullver_monthly_pkm_extra, how='right', on='month_count')

pullver_monthly_pkm_sim = pullver_monthly_df.groupby('month_count')['pkm_monthly_sim'].sum()
pullver_monthly_df = pd.merge(pullver_monthly_df, pullver_monthly_pkm_sim, how='right', on='month_count')

pullver_monthly_pkm_miss = pullver_monthly_df.groupby('month_count')['pkm_monthly_miss'].sum()
pullver_monthly_df = pd.merge(pullver_monthly_df, pullver_monthly_pkm_miss, how='right', on='month_count')

pullver_monthly_pkm_unrec = pullver_monthly_df.groupby('month_count')['pkm_monthly_unrec'].sum()
pullver_monthly_df = pd.merge(pullver_monthly_df, pullver_monthly_pkm_unrec, how='right', on='month_count')

pullver_monthly_pkm_other = pullver_monthly_df.groupby('month_count')['pkm_monthly_other'].sum()
pullver_monthly_df = pd.merge(pullver_monthly_df, pullver_monthly_pkm_other, how='right', on='month_count')

#Ygo errors
pullver_monthly_ygo_extra = pullver_monthly_df.groupby('month_count')['ygo_monthly_extra'].sum()
pullver_monthly_df = pd.merge(pullver_monthly_df, pullver_monthly_ygo_extra, how='right', on='month_count')

pullver_monthly_ygo_sim = pullver_monthly_df.groupby('month_count')['ygo_monthly_sim'].sum()
pullver_monthly_df = pd.merge(pullver_monthly_df, pullver_monthly_ygo_sim, how='right', on='month_count')

pullver_monthly_ygo_miss = pullver_monthly_df.groupby('month_count')['ygo_monthly_miss'].sum()
pullver_monthly_df = pd.merge(pullver_monthly_df, pullver_monthly_ygo_miss, how='right', on='month_count')

pullver_monthly_ygo_unrec = pullver_monthly_df.groupby('month_count')['ygo_monthly_unrec'].sum()
pullver_monthly_df = pd.merge(pullver_monthly_df, pullver_monthly_ygo_unrec, how='right', on='month_count')

pullver_monthly_ygo_other = pullver_monthly_df.groupby('month_count')['ygo_monthly_other'].sum()
pullver_monthly_df = pd.merge(pullver_monthly_df, pullver_monthly_ygo_other, how='right', on='month_count')

#Mixed errors
pullver_monthly_mix_extra = pullver_monthly_df.groupby('month_count')['mix_monthly_extra'].sum()
pullver_monthly_df = pd.merge(pullver_monthly_df, pullver_monthly_mix_extra, how='right', on='month_count')

pullver_monthly_mix_sim = pullver_monthly_df.groupby('month_count')['mix_monthly_sim'].sum()
pullver_monthly_df = pd.merge(pullver_monthly_df, pullver_monthly_mix_sim, how='right', on='month_count')

pullver_monthly_mix_miss = pullver_monthly_df.groupby('month_count')['mix_monthly_miss'].sum()
pullver_monthly_df = pd.merge(pullver_monthly_df, pullver_monthly_mix_miss, how='right', on='month_count')

pullver_monthly_mix_unrec = pullver_monthly_df.groupby('month_count')['mix_monthly_unrec'].sum()
pullver_monthly_df = pd.merge(pullver_monthly_df, pullver_monthly_mix_unrec, how='right', on='month_count')

pullver_monthly_mix_other = pullver_monthly_df.groupby('month_count')['mix_monthly_other'].sum()
pullver_monthly_df = pd.merge(pullver_monthly_df, pullver_monthly_mix_other, how='right', on='month_count')

#Output
pullver_monthly_card_output = pullver_monthly_df.groupby('month_count')['Units'].sum()
pullver_monthly_df = pd.merge(pullver_monthly_df, pullver_monthly_card_output, how='right', on='month_count')

pullver_monthly_sq_output = pullver_monthly_df.groupby('month_count')['SQ/POQ'].nunique()
pullver_monthly_df = pd.merge(pullver_monthly_df, pullver_monthly_sq_output, how='right', on='month_count')

#SQ type count
pullver_monthly_df["mtg_sq_count"] = 0
pullver_monthly_df["pkm_sq_count"] = 0
pullver_monthly_df["ygo_sq_count"] = 0
pullver_monthly_df["mix_sq_count"] = 0

pullver_monthly_df.loc[(pullver_monthly_df['mtg'] == 1) & (pullver_monthly_df['pkm'] == 0) & (pullver_monthly_df['ygo'] == 0), 'mtg_sq_count'] = 1
pullver_monthly_df.loc[(pullver_monthly_df['mtg'] == 0) & (pullver_monthly_df['pkm'] == 1) & (pullver_monthly_df['ygo'] == 0), 'pkm_sq_count'] = 1
pullver_monthly_df.loc[(pullver_monthly_df['mtg'] == 0) & (pullver_monthly_df['pkm'] == 0) & (pullver_monthly_df['ygo'] == 1), 'ygo_sq_count'] = 1
pullver_monthly_df.loc[pullver_monthly_df['mixed'] > 1, 'mix_sq_count'] = 1

pullver_monthly_mtg_sqs = pullver_monthly_df.groupby('month_count')['mtg_sq_count'].sum()
pullver_monthly_df = pd.merge(pullver_monthly_df, pullver_monthly_mtg_sqs, how='right', on='month_count')

pullver_monthly_pkm_sqs = pullver_monthly_df.groupby('month_count')['pkm_sq_count'].sum()
pullver_monthly_df = pd.merge(pullver_monthly_df, pullver_monthly_pkm_sqs, how='right', on='month_count')

pullver_monthly_ygo_sqs = pullver_monthly_df.groupby('month_count')['ygo_sq_count'].sum()
pullver_monthly_df = pd.merge(pullver_monthly_df, pullver_monthly_ygo_sqs, how='right', on='month_count')

pullver_monthly_mix_sqs = pullver_monthly_df.groupby('month_count')['mix_sq_count'].sum()
pullver_monthly_df = pd.merge(pullver_monthly_df, pullver_monthly_mix_sqs, how='right', on='month_count')

#Parse down to single pullverer + month combinations
pullver_monthly_df.drop_duplicates(subset=['month_count'], inplace=True)

#Rename columns
pullver_monthly_df.rename(columns={'mtg_monthly_extra_y':'mtg_monthly_extra', 'mtg_monthly_sim_y':'mtg_monthly_sim', 'mtg_monthly_miss_y':'mtg_monthly_miss', 'mtg_monthly_unrec_y':'mtg_monthly_unrec', 'mtg_monthly_other_y':'mtg_monthly_other', 'pkm_monthly_extra_y':'pkm_monthly_extra', 'pkm_monthly_sim_y':'pkm_monthly_sim', 'pkm_monthly_miss_y':'pkm_monthly_miss', 'pkm_monthly_unrec_y':'pkm_monthly_unrec', 'pkm_monthly_other_y':'pkm_monthly_other', 'ygo_monthly_extra_y':'ygo_monthly_extra', 'ygo_monthly_sim_y':'ygo_monthly_sim', 'ygo_monthly_miss_y':'ygo_monthly_miss', 'ygo_monthly_unrec_y':'ygo_monthly_unrec', 'ygo_monthly_other_y':'ygo_monthly_other', 'mix_monthly_extra_y':'mix_monthly_extra', 'mix_monthly_sim_y':'mix_monthly_sim', 'mix_monthly_miss_y':'mix_monthly_miss', 'mix_monthly_unrec_y':'mix_monthly_unrec', 'mix_monthly_other_y':'mix_monthly_other', 'Units_y':'monthly_units', 'SQ/POQ_y':'monthly_sqs_pullvered', 'First_Offset':'monthly_first_offset', 'pullver_operator_tenure':'monthly_pullver_operator_tenure', 'mtg_sq_count_y':'monthly_mtg_sq_count', 'pkm_sq_count_y':'monthly_pkm_sq_count', 'ygo_sq_count_y':'monthly_ygo_sq_count', 'mix_sq_count_y':'monthly_mix_sq_count'}, inplace=True)

#Total error amounts
pullver_monthly_df["monthly_extra"] = pullver_monthly_df['mtg_monthly_extra'].astype('float64') + pullver_monthly_df['pkm_monthly_extra'].astype('float64') + pullver_monthly_df['ygo_monthly_extra'].astype('float64') + pullver_monthly_df['mix_monthly_extra'].astype('float64')

pullver_monthly_df["monthly_sim"] = pullver_monthly_df['mtg_monthly_sim'].astype('float64') + pullver_monthly_df['pkm_monthly_sim'].astype('float64') + pullver_monthly_df['ygo_monthly_sim'].astype('float64') + pullver_monthly_df['mix_monthly_sim'].astype('float64')

pullver_monthly_df["monthly_miss"] = pullver_monthly_df['mtg_monthly_miss'].astype('float64') + pullver_monthly_df['pkm_monthly_miss'].astype('float64') + pullver_monthly_df['ygo_monthly_miss'].astype('float64') + pullver_monthly_df['mix_monthly_miss'].astype('float64')

pullver_monthly_df["monthly_unrec"] = pullver_monthly_df['mtg_monthly_unrec'].astype('float64') + pullver_monthly_df['pkm_monthly_unrec'].astype('float64') + pullver_monthly_df['ygo_monthly_unrec'].astype('float64') + pullver_monthly_df['mix_monthly_unrec'].astype('float64')

pullver_monthly_df["monthly_other"] = pullver_monthly_df['mtg_monthly_other'].astype('float64') + pullver_monthly_df['pkm_monthly_other'].astype('float64') + pullver_monthly_df['ygo_monthly_other'].astype('float64') + pullver_monthly_df['mix_monthly_other'].astype('float64')

#Drop extraneous columns
pullver_monthly_df.drop(['mtg_monthly_extra_x', 'mtg_monthly_sim_x', 'mtg_monthly_miss_x', 'mtg_monthly_unrec_x', 'mtg_monthly_other_x', 'pkm_monthly_extra_x', 'pkm_monthly_sim_x', 'pkm_monthly_miss_x', 'pkm_monthly_unrec_x', 'pkm_monthly_other_x', 'ygo_monthly_extra_x', 'ygo_monthly_sim_x', 'ygo_monthly_miss_x', 'ygo_monthly_unrec_x', 'ygo_monthly_other_x', 'mix_monthly_extra_x', 'mix_monthly_sim_x', 'mix_monthly_miss_x', 'mix_monthly_unrec_x', 'mix_monthly_other_x', 'Units_x', 'SQ/POQ_x','total_extra_count_pullver', 'total_miss_count_pullver', 'total_sim_count_pullver', 'total_unrec_count_pullver', 'total_other_count_pullver', 'mtg_sq_count_x', 'pkm_sq_count_x', 'ygo_sq_count_x', 'mix_sq_count_x', 'mtg', 'pkm', 'ygo', 'mixed', 'clean_sq', 'punch_normalized', 'sq_type', 'pullver_day_of_week', 'month_count', 'week_count', 'er_res_sq', 'pvp_sq'],axis=1, inplace=True)

##Weekly Errors and output (PullVer)
pullver_weekly_df = pullver_df.copy()
pullver_weekly_df['Units'] = pullver_weekly_df['Units'] .astype('float64')

pullver_weekly_df["mtg_weekly_extra"] = 0
pullver_weekly_df["mtg_weekly_sim"] = 0
pullver_weekly_df["mtg_weekly_miss"] = 0
pullver_weekly_df["mtg_weekly_unrec"] = 0
pullver_weekly_df["mtg_weekly_other"] = 0

pullver_weekly_df["pkm_weekly_extra"] = 0
pullver_weekly_df["pkm_weekly_sim"] = 0
pullver_weekly_df["pkm_weekly_miss"] = 0
pullver_weekly_df["pkm_weekly_unrec"] = 0
pullver_weekly_df["pkm_weekly_other"] = 0

pullver_weekly_df["ygo_weekly_extra"] = 0
pullver_weekly_df["ygo_weekly_sim"] = 0
pullver_weekly_df["ygo_weekly_miss"] = 0
pullver_weekly_df["ygo_weekly_unrec"] = 0
pullver_weekly_df["ygo_weekly_other"] = 0

pullver_weekly_df["mix_weekly_extra"] = 0
pullver_weekly_df["mix_weekly_sim"] = 0
pullver_weekly_df["mix_weekly_miss"] = 0
pullver_weekly_df["mix_weekly_unrec"] = 0
pullver_weekly_df["mix_weekly_other"] = 0

pullver_weekly_df.loc[pullver_weekly_df['sq_type'] == 'mtg', 'mtg_weekly_extra'] = pullver_weekly_df['total_extra_count_pullver']
pullver_weekly_df.loc[pullver_weekly_df['sq_type'] == 'pkm', 'pkm_weekly_extra'] = pullver_weekly_df['total_extra_count_pullver']
pullver_weekly_df.loc[pullver_weekly_df['sq_type'] == 'ygo', 'ygo_weekly_extra'] = pullver_weekly_df['total_extra_count_pullver']
pullver_weekly_df.loc[pullver_weekly_df['sq_type'] == 'mixed', 'mix_weekly_extra'] = pullver_weekly_df['total_extra_count_pullver']

pullver_weekly_df.loc[pullver_weekly_df['sq_type'] == 'mtg', 'mtg_weekly_sim'] = pullver_weekly_df['total_sim_count_pullver']
pullver_weekly_df.loc[pullver_weekly_df['sq_type'] == 'pkm', 'pkm_weekly_sim'] = pullver_weekly_df['total_sim_count_pullver']
pullver_weekly_df.loc[pullver_weekly_df['sq_type'] == 'ygo', 'ygo_weekly_sim'] = pullver_weekly_df['total_sim_count_pullver']
pullver_weekly_df.loc[pullver_weekly_df['sq_type'] == 'mixed', 'mix_weekly_sim'] = pullver_weekly_df['total_sim_count_pullver']

pullver_weekly_df.loc[pullver_weekly_df['sq_type'] == 'mtg', 'mtg_weekly_miss'] = pullver_weekly_df['total_miss_count_pullver']
pullver_weekly_df.loc[pullver_weekly_df['sq_type'] == 'pkm', 'pkm_weekly_miss'] = pullver_weekly_df['total_miss_count_pullver']
pullver_weekly_df.loc[pullver_weekly_df['sq_type'] == 'ygo', 'ygo_weekly_miss'] = pullver_weekly_df['total_miss_count_pullver']
pullver_weekly_df.loc[pullver_weekly_df['sq_type'] == 'mixed', 'mix_weekly_miss'] = pullver_weekly_df['total_miss_count_pullver']

pullver_weekly_df.loc[pullver_weekly_df['sq_type'] == 'mtg', 'mtg_weekly_unrec'] = pullver_weekly_df['total_unrec_count_pullver']
pullver_weekly_df.loc[pullver_weekly_df['sq_type'] == 'pkm', 'pkm_weekly_unrec'] = pullver_weekly_df['total_unrec_count_pullver']
pullver_weekly_df.loc[pullver_weekly_df['sq_type'] == 'ygo', 'ygo_weekly_unrec'] = pullver_weekly_df['total_unrec_count_pullver']
pullver_weekly_df.loc[pullver_weekly_df['sq_type'] == 'mixed', 'mix_weekly_unrec'] = pullver_weekly_df['total_unrec_count_pullver']

pullver_weekly_df.loc[pullver_weekly_df['sq_type'] == 'mtg', 'mtg_weekly_other'] = pullver_weekly_df['total_other_count_pullver']
pullver_weekly_df.loc[pullver_weekly_df['sq_type'] == 'pkm', 'pkm_weekly_other'] = pullver_weekly_df['total_other_count_pullver']
pullver_weekly_df.loc[pullver_weekly_df['sq_type'] == 'ygo', 'ygo_weekly_other'] = pullver_weekly_df['total_other_count_pullver']
pullver_weekly_df.loc[pullver_weekly_df['sq_type'] == 'mixed', 'mix_weekly_other'] = pullver_weekly_df['total_other_count_pullver']

#Mtg errors
pullver_weekly_mtg_extra = pullver_weekly_df.groupby('week_count')['mtg_weekly_extra'].sum()
pullver_weekly_df = pd.merge(pullver_weekly_df, pullver_weekly_mtg_extra, how='right', on='week_count')

pullver_weekly_mtg_sim = pullver_weekly_df.groupby('week_count')['mtg_weekly_sim'].sum()
pullver_weekly_df = pd.merge(pullver_weekly_df, pullver_weekly_mtg_sim, how='right', on='week_count')

pullver_weekly_mtg_miss = pullver_weekly_df.groupby('week_count')['mtg_weekly_miss'].sum()
pullver_weekly_df = pd.merge(pullver_weekly_df, pullver_weekly_mtg_miss, how='right', on='week_count')

pullver_weekly_mtg_unrec = pullver_weekly_df.groupby('week_count')['mtg_weekly_unrec'].sum()
pullver_weekly_df = pd.merge(pullver_weekly_df, pullver_weekly_mtg_unrec, how='right', on='week_count')

pullver_weekly_mtg_other = pullver_weekly_df.groupby('week_count')['mtg_weekly_other'].sum()
pullver_weekly_df = pd.merge(pullver_weekly_df, pullver_weekly_mtg_other, how='right', on='week_count')

#Pkm errors
pullver_weekly_pkm_extra = pullver_weekly_df.groupby('week_count')['pkm_weekly_extra'].sum()
pullver_weekly_df = pd.merge(pullver_weekly_df, pullver_weekly_pkm_extra, how='right', on='week_count')

pullver_weekly_pkm_sim = pullver_weekly_df.groupby('week_count')['pkm_weekly_sim'].sum()
pullver_weekly_df = pd.merge(pullver_weekly_df, pullver_weekly_pkm_sim, how='right', on='week_count')

pullver_weekly_pkm_miss = pullver_weekly_df.groupby('week_count')['pkm_weekly_miss'].sum()
pullver_weekly_df = pd.merge(pullver_weekly_df, pullver_weekly_pkm_miss, how='right', on='week_count')

pullver_weekly_pkm_unrec = pullver_weekly_df.groupby('week_count')['pkm_weekly_unrec'].sum()
pullver_weekly_df = pd.merge(pullver_weekly_df, pullver_weekly_pkm_unrec, how='right', on='week_count')

pullver_weekly_pkm_other = pullver_weekly_df.groupby('week_count')['pkm_weekly_other'].sum()
pullver_weekly_df = pd.merge(pullver_weekly_df, pullver_weekly_pkm_other, how='right', on='week_count')

#Ygo errors
pullver_weekly_ygo_extra = pullver_weekly_df.groupby('week_count')['ygo_weekly_extra'].sum()
pullver_weekly_df = pd.merge(pullver_weekly_df, pullver_weekly_ygo_extra, how='right', on='week_count')

pullver_weekly_ygo_sim = pullver_weekly_df.groupby('week_count')['ygo_weekly_sim'].sum()
pullver_weekly_df = pd.merge(pullver_weekly_df, pullver_weekly_ygo_sim, how='right', on='week_count')

pullver_weekly_ygo_miss = pullver_weekly_df.groupby('week_count')['ygo_weekly_miss'].sum()
pullver_weekly_df = pd.merge(pullver_weekly_df, pullver_weekly_ygo_miss, how='right', on='week_count')

pullver_weekly_ygo_unrec = pullver_weekly_df.groupby('week_count')['ygo_weekly_unrec'].sum()
pullver_weekly_df = pd.merge(pullver_weekly_df, pullver_weekly_ygo_unrec, how='right', on='week_count')

pullver_weekly_ygo_other = pullver_weekly_df.groupby('week_count')['ygo_weekly_other'].sum()
pullver_weekly_df = pd.merge(pullver_weekly_df, pullver_weekly_ygo_other, how='right', on='week_count')

#Mixed errors
pullver_weekly_mix_extra = pullver_weekly_df.groupby('week_count')['mix_weekly_extra'].sum()
pullver_weekly_df = pd.merge(pullver_weekly_df, pullver_weekly_mix_extra, how='right', on='week_count')

pullver_weekly_mix_sim = pullver_weekly_df.groupby('week_count')['mix_weekly_sim'].sum()
pullver_weekly_df = pd.merge(pullver_weekly_df, pullver_weekly_mix_sim, how='right', on='week_count')

pullver_weekly_mix_miss = pullver_weekly_df.groupby('week_count')['mix_weekly_miss'].sum()
pullver_weekly_df = pd.merge(pullver_weekly_df, pullver_weekly_mix_miss, how='right', on='week_count')

pullver_weekly_mix_unrec = pullver_weekly_df.groupby('week_count')['mix_weekly_unrec'].sum()
pullver_weekly_df = pd.merge(pullver_weekly_df, pullver_weekly_mix_unrec, how='right', on='week_count')

pullver_weekly_mix_other = pullver_weekly_df.groupby('week_count')['mix_weekly_other'].sum()
pullver_weekly_df = pd.merge(pullver_weekly_df, pullver_weekly_mix_other, how='right', on='week_count')

#Output
pullver_weekly_card_output = pullver_weekly_df.groupby('week_count')['Units'].sum()
pullver_weekly_df = pd.merge(pullver_weekly_df, pullver_weekly_card_output, how='right', on='week_count')

pullver_weekly_sq_output = pullver_weekly_df.groupby('week_count')['SQ/POQ'].nunique()
pullver_weekly_df = pd.merge(pullver_weekly_df, pullver_weekly_sq_output, how='right', on='week_count')

#SQ type count
pullver_weekly_df["mtg_sq_count"] = 0
pullver_weekly_df["pkm_sq_count"] = 0
pullver_weekly_df["ygo_sq_count"] = 0
pullver_weekly_df["mix_sq_count"] = 0

pullver_weekly_df.loc[(pullver_weekly_df['mtg'] == 1) & (pullver_weekly_df['pkm'] == 0) & (pullver_weekly_df['ygo'] == 0), 'mtg_sq_count'] = 1
pullver_weekly_df.loc[(pullver_weekly_df['mtg'] == 0) & (pullver_weekly_df['pkm'] == 1) & (pullver_weekly_df['ygo'] == 0), 'pkm_sq_count'] = 1
pullver_weekly_df.loc[(pullver_weekly_df['mtg'] == 0) & (pullver_weekly_df['pkm'] == 0) & (pullver_weekly_df['ygo'] == 1), 'ygo_sq_count'] = 1
pullver_weekly_df.loc[pullver_weekly_df['mixed'] > 1, 'mix_sq_count'] = 1

pullver_weekly_mtg_sqs = pullver_weekly_df.groupby('week_count')['mtg_sq_count'].sum()
pullver_weekly_df = pd.merge(pullver_weekly_df, pullver_weekly_mtg_sqs, how='right', on='week_count')

pullver_weekly_pkm_sqs = pullver_weekly_df.groupby('week_count')['pkm_sq_count'].sum()
pullver_weekly_df = pd.merge(pullver_weekly_df, pullver_weekly_pkm_sqs, how='right', on='week_count')

pullver_weekly_ygo_sqs = pullver_weekly_df.groupby('week_count')['ygo_sq_count'].sum()
pullver_weekly_df = pd.merge(pullver_weekly_df, pullver_weekly_ygo_sqs, how='right', on='week_count')

pullver_weekly_mix_sqs = pullver_weekly_df.groupby('week_count')['mix_sq_count'].sum()
pullver_weekly_df = pd.merge(pullver_weekly_df, pullver_weekly_mix_sqs, how='right', on='week_count')

#Parse down to single pullverer + week combinations
pullver_weekly_df.drop_duplicates(subset=['week_count'], inplace=True)

#Rename columns
pullver_weekly_df.rename(columns={'mtg_weekly_extra_y':'mtg_weekly_extra', 'mtg_weekly_sim_y':'mtg_weekly_sim', 'mtg_weekly_miss_y':'mtg_weekly_miss', 'mtg_weekly_unrec_y':'mtg_weekly_unrec', 'mtg_weekly_other_y':'mtg_weekly_other', 'pkm_weekly_extra_y':'pkm_weekly_extra', 'pkm_weekly_sim_y':'pkm_weekly_sim', 'pkm_weekly_miss_y':'pkm_weekly_miss', 'pkm_weekly_unrec_y':'pkm_weekly_unrec', 'pkm_weekly_other_y':'pkm_weekly_other', 'ygo_weekly_extra_y':'ygo_weekly_extra', 'ygo_weekly_sim_y':'ygo_weekly_sim', 'ygo_weekly_miss_y':'ygo_weekly_miss', 'ygo_weekly_unrec_y':'ygo_weekly_unrec', 'ygo_weekly_other_y':'ygo_weekly_other', 'mix_weekly_extra_y':'mix_weekly_extra', 'mix_weekly_sim_y':'mix_weekly_sim', 'mix_weekly_miss_y':'mix_weekly_miss', 'mix_weekly_unrec_y':'mix_weekly_unrec', 'mix_weekly_other_y':'mix_weekly_other', 'Units_y':'weekly_units', 'SQ/POQ_y':'weekly_sqs_pullvered', 'First_Offset':'weekly_first_offset', 'pullver_operator_tenure':'weekly_pullver_operator_tenure', 'mtg_sq_count_y':'weekly_mtg_sq_count', 'pkm_sq_count_y':'weekly_pkm_sq_count', 'ygo_sq_count_y':'weekly_ygo_sq_count', 'mix_sq_count_y':'weekly_mix_sq_count'}, inplace=True)

#Total error amounts
pullver_weekly_df["weekly_extra"] = pullver_weekly_df['mtg_weekly_extra'].astype('float64') + pullver_weekly_df['pkm_weekly_extra'].astype('float64') + pullver_weekly_df['ygo_weekly_extra'].astype('float64') + pullver_weekly_df['mix_weekly_extra'].astype('float64')

pullver_weekly_df["weekly_sim"] = pullver_weekly_df['mtg_weekly_sim'].astype('float64') + pullver_weekly_df['pkm_weekly_sim'].astype('float64') + pullver_weekly_df['ygo_weekly_sim'].astype('float64') + pullver_weekly_df['mix_weekly_sim'].astype('float64')

pullver_weekly_df["weekly_miss"] = pullver_weekly_df['mtg_weekly_miss'].astype('float64') + pullver_weekly_df['pkm_weekly_miss'].astype('float64') + pullver_weekly_df['ygo_weekly_miss'].astype('float64') + pullver_weekly_df['mix_weekly_miss'].astype('float64')

pullver_weekly_df["weekly_unrec"] = pullver_weekly_df['mtg_weekly_unrec'].astype('float64') + pullver_weekly_df['pkm_weekly_unrec'].astype('float64') + pullver_weekly_df['ygo_weekly_unrec'].astype('float64') + pullver_weekly_df['mix_weekly_unrec'].astype('float64')

pullver_weekly_df["weekly_other"] = pullver_weekly_df['mtg_weekly_other'].astype('float64') + pullver_weekly_df['pkm_weekly_other'].astype('float64') + pullver_weekly_df['ygo_weekly_other'].astype('float64') + pullver_weekly_df['mix_weekly_other'].astype('float64')

#Drop extraneous columns
pullver_weekly_df.drop(['mtg_weekly_extra_x', 'mtg_weekly_sim_x', 'mtg_weekly_miss_x', 'mtg_weekly_unrec_x', 'mtg_weekly_other_x', 'pkm_weekly_extra_x', 'pkm_weekly_sim_x', 'pkm_weekly_miss_x', 'pkm_weekly_unrec_x', 'pkm_weekly_other_x', 'ygo_weekly_extra_x', 'ygo_weekly_sim_x', 'ygo_weekly_miss_x', 'ygo_weekly_unrec_x', 'ygo_weekly_other_x', 'mix_weekly_extra_x', 'mix_weekly_sim_x', 'mix_weekly_miss_x', 'mix_weekly_unrec_x', 'mix_weekly_other_x', 'Units_x', 'SQ/POQ_x','total_extra_count_pullver', 'total_miss_count_pullver', 'total_sim_count_pullver', 'total_unrec_count_pullver', 'total_other_count_pullver', 'mtg_sq_count_x', 'pkm_sq_count_x', 'ygo_sq_count_x', 'mix_sq_count_x', 'mtg', 'pkm', 'ygo', 'mixed', 'clean_sq', 'punch_normalized', 'sq_type', 'pullver_day_of_week', 'week_count', 'week_count', 'er_res_sq', 'pvp_sq'],axis=1, inplace=True)

##Combine monthly and weekly frames
pullver_weekly_df["combined"] = pullver_weekly_df['Punch'].astype(str) + pullver_weekly_df['Puncher'].astype(str)
pullver_monthly_df["combined"] = pullver_monthly_df['Punch'].astype(str) + pullver_monthly_df['Puncher'].astype(str)

pullver_combined_df = pd.DataFrame()

pullver_combined_df = pd.merge(pullver_weekly_df, pullver_monthly_df, how='left', on='combined')

pullver_combined_df.rename(columns={'Puncher_x':'Puncher', 'Supervisor_x':'Supervisor', 'pullver_week_of_x':'pullver_week_of', 'pullver_month_of_y':'pullver_month_of'}, inplace=True)

pullver_combined_df = pullver_combined_df[[
'Puncher',
'Supervisor',
'pullver_week_of',
'pullver_month_of',
'weekly_pullver_operator_tenure',
'weekly_units',
'weekly_sqs_pullvered',
'weekly_mtg_sq_count',
'weekly_pkm_sq_count',
'weekly_ygo_sq_count',
'weekly_mix_sq_count',
'mtg_weekly_extra',
'mtg_weekly_sim',
'mtg_weekly_miss',
'mtg_weekly_unrec',
'mtg_weekly_other',
'pkm_weekly_extra',
'pkm_weekly_sim',
'pkm_weekly_miss',
'pkm_weekly_unrec',
'pkm_weekly_other',
'ygo_weekly_extra',
'ygo_weekly_sim',
'ygo_weekly_miss',
'ygo_weekly_unrec',
'ygo_weekly_other',
'mix_weekly_extra',
'mix_weekly_sim',
'mix_weekly_miss',
'mix_weekly_unrec',
'mix_weekly_other',
'monthly_units',
'monthly_sqs_pullvered',
'monthly_mtg_sq_count',
'monthly_pkm_sq_count',
'monthly_ygo_sq_count',
'monthly_mix_sq_count',
'mtg_monthly_extra',
'mtg_monthly_sim',
'mtg_monthly_miss',
'mtg_monthly_unrec',
'mtg_monthly_other',
'pkm_monthly_extra',
'pkm_monthly_sim',
'pkm_monthly_miss',
'pkm_monthly_unrec',
'pkm_monthly_other',
'ygo_monthly_extra',
'ygo_monthly_sim',
'ygo_monthly_miss',
'ygo_monthly_unrec',
'ygo_monthly_other',
'mix_monthly_extra',
'mix_monthly_sim',
'mix_monthly_miss',
'mix_monthly_unrec',
'mix_monthly_other']]

##Write data to sheets
pullverDataTab.clear()
gd.set_with_dataframe(pullverDataTab, pullver_combined_df)

pullDataTab.clear()
gd.set_with_dataframe(pullDataTab, pull_combined_df)

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