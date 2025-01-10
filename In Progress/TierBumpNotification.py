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

##Import Staffing Data
staffing = gc.open_by_key('1sBVK5vjiB72JuKePpht2R4vZ4ShxuZKjQreE8FE7068').worksheet('Current Staff')
staffing_df = pd.DataFrame.from_dict(staffing.get_all_records())
staffing_df.dropna(subset=["Preferred Name"], inplace=True)
staffing_df = staffing_df[['Preferred Name', 'Email','Supervisor', 'OPs Lead', 'Shift Length']]
staffing_df.rename(columns={'Email':'Team Member'}, inplace=True)

staffing_df['Team Member'] = staffing_df['Team Member'].str.lower()

##Import tier information
hoursTab = gc.open_by_key('1Xq6I5LWxUvqRQ3kw8aBHFyPYmTJlajIfeMbWzzmggi4').worksheet('FilteredData')
hours_df = pd.DataFrame.from_dict(hoursTab.get_all_records())

hours_df = hours_df.loc[(hours_df['Role'] != '') & (hours_df['Job'].str[:13] == 'AC Generalist')]
hours_df = hours_df[['Primary Email', 'Date', 'Job']]

hours_df['Primary Email'] = hours_df['Primary Email'].str.lower()
hours_df.rename(columns={'Primary Email':'Team Member'}, inplace=True)

##Combine Staffing and Tier information
hours_df = pd.merge(hours_df, staffing_df, how='left', on='Team Member')
hours_df = hours_df.loc[hours_df['Shift Length'].astype('float64') > 0]

hours_df['Date'] = pd.to_datetime(hours_df['Date'])

##Find most recent job listing from hours data
max_date = hours_df.groupby('Team Member')['Date'].max()
hours_df = pd.merge(hours_df, max_date, how='right', on='Team Member')
hours_df.rename(columns={'Date_x':'Date', 'Date_y':'max_date'}, inplace=True)

hours_df.loc[hours_df['Date'] != hours_df['max_date'], 'Date'] = None

hours_df.dropna(subset=['Date'], inplace=True)

##Import 90 days metrics information
data_string = ["C:", "Users", login, "OneDrive - eBay Inc", "AC-Scripting", "Data CSVs", "NuWay", "ParsedData.csv"]
data_result = separator.join(data_string)
data_df = pd.read_csv(data_result)

##Determine 90 day metrics for each person
data_df['Total Earned Hours'] = data_df['Total Earned Hours'].astype('float64') * 24
data_df['Hours Worked'] = data_df['Hours Worked'].astype('float64')

sum_earned_hours = data_df.groupby('Team Member')['Total Earned Hours'].sum()
data_df = pd.merge(data_df, sum_earned_hours, how='right', on='Team Member')

sum_worked_hours = data_df.groupby('Team Member')['Hours Worked'].sum()
data_df = pd.merge(data_df, sum_worked_hours, how='right', on='Team Member')

data_df.rename(columns={'Hours Worked_x':'daily_hours_worked', 'Hours Worked_y':'90_day_hours_worked', 'Total Earned Hours_x':'daily_earned_hours', 'Total Earned Hours_y':'90_day_earned_hours'}, inplace=True)

data_df.drop_duplicates(subset='Team Member', inplace=True)

data_df = data_df[['Team Member', 'Pillar', '90_day_earned_hours', '90_day_hours_worked']]

data_df["90_day_metric"] = data_df['90_day_earned_hours'].astype('float64') / data_df['90_day_hours_worked'].astype('float64')

data_df = data_df.loc[(data_df['Pillar'] != "Inventory") & (data_df['Pillar'] != "Supervision")]

##Combine hours and data frames
hours_df = pd.merge(hours_df, data_df, left_on='Preferred Name', right_on='Team Member')

##Convert jobs to levels
hours_df["job_compare"] = "AC Generalist"
hours_df["job_mismatch"] = 0

hours_df.loc[(hours_df['90_day_metric'].astype('float64') >= 1.2) & (hours_df['90_day_metric'].astype('float64') < 1.4), 'job_compare'] = 'AC Generalist - Advanced'
hours_df.loc[hours_df['90_day_metric'].astype('float64') >= 1.4, 'job_compare'] = 'AC Generalist - Legend'

hours_df.loc[hours_df['Job'].astype(str) != hours_df['job_compare'].astype(str), 'job_mismatch'] = 1

hours_df = hours_df.loc[hours_df['job_mismatch'] == 1]

##Convert titles with comparison levels
hours_df["current_level"] = 0
hours_df.loc[hours_df['Job'] == 'AC Generalist', 'current_level'] = 1
hours_df.loc[hours_df['Job'] == 'AC Generalist - Advanced', 'current_level'] = 2
hours_df.loc[hours_df['Job'] == 'AC Generalist - Legend', 'current_level'] = 3

hours_df["90_day_level"] = 0
hours_df.loc[hours_df['job_compare'] == 'AC Generalist', '90_day_level'] = 1
hours_df.loc[hours_df['job_compare'] == 'AC Generalist - Advanced', '90_day_level'] = 2
hours_df.loc[hours_df['job_compare'] == 'AC Generalist - Legend', '90_day_level'] = 3
hours_df.loc[hours_df['job_compare'] == '', '90_day_level'] = 0

##Find tier bumps
hours_df["bumps"] = hours_df['90_day_level'].astype('float64') - hours_df['current_level'].astype('float64')
hours_df.rename(columns={'Job':'Current Tier', 'job_compare':'Updated Tier'}, inplace=True)

bumps_df = hours_df.copy()
bumps_df = bumps_df.loc[bumps_df['bumps'].astype('float64') > 0]
bumps_df = bumps_df[['Preferred Name', 'Supervisor', 'OPs Lead', 'Pillar', '90_day_earned_hours', '90_day_hours_worked', '90_day_metric', 'Current Tier',  'Updated Tier']]



dips_df = hours_df.copy()
dips_df = dips_df.loc[dips_df['bumps'].astype('float64') < 0]
dips_df = dips_df[['Preferred Name', 'Supervisor', 'OPs Lead', 'Pillar', '90_day_earned_hours', '90_day_hours_worked', '90_day_metric', 'Current Tier',  'Updated Tier']]

##Write data to sheet
tierDoc = gc.open_by_key('1l9bB4kEO8xnzIw7ac8ihjfKXDQ3_Prq5sbczCC3ECtA')

bumpTab = tierDoc.worksheet('Bumps')
bumpTab.clear()
gd.set_with_dataframe(bumpTab, bumps_df)

dipTab = tierDoc.worksheet('Dips')
dipTab.clear()
gd.set_with_dataframe(dipTab, dips_df)



