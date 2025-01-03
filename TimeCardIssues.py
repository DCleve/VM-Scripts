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

##Import Staffing Data
staffing = gc.open_by_key('1sBVK5vjiB72JuKePpht2R4vZ4ShxuZKjQreE8FE7068').worksheet('Current Staff')
staffing_df = pd.DataFrame.from_dict(staffing.get_all_records())
staffing_df.dropna(subset=["Preferred Name"], inplace=True)
staffing_df.drop(staffing_df.filter(like='Unnamed'), axis=1, inplace=True)
staffing_df = staffing_df[['Preferred Name', 'Email','Shift Name', 'Start Date', 'Supervisor', 'OPs Lead', 'Last, First (Formatting)', 'Role', 'Shift Length']]

staffing_df.rename(columns={'Email':'Puncher'}, inplace=True)
staffing_df.loc[staffing_df['Puncher'] != '', 'Puncher'] = staffing_df['Puncher'].str.lower()

##Import Shift Data
shift = gc.open_by_key('1Xq6I5LWxUvqRQ3kw8aBHFyPYmTJlajIfeMbWzzmggi4').worksheet('FilteredData')
shiftdata_df = pd.DataFrame.from_dict(shift.get_all_records())
shiftdata_df.dropna(subset=["Date"], inplace=True)
shiftdata_df.drop(shiftdata_df.filter(like='Unnamed'), axis=1, inplace=True)
shiftdata_df = shiftdata_df[['Date', 'Primary Email', 'Role', 'Regular Hours']]
shiftdata_df['Date'] = pd.to_datetime(shiftdata_df['Date'])
shiftdata_df['Date'] = shiftdata_df['Date'].dt.date

shiftdata_df.loc[(shiftdata_df['Regular Hours'] == '-') | (shiftdata_df['Regular Hours'] == '0'), 'Regular Hours'] = None
shiftdata_df.dropna(subset=["Regular Hours"], inplace=True)
shiftdata_df['Primary Email'] = shiftdata_df['Primary Email'].str.lower()

##Merge data
shiftdata_df = pd.merge(shiftdata_df, staffing_df, left_on='Primary Email', right_on='Puncher')

shiftdata_df.rename(columns={'Role_x':'Role'}, inplace=True)

shiftdata_df = shiftdata_df[['Date', 'Primary Email', 'Regular Hours', 'Preferred Name', 'Supervisor', 'OPs Lead', 'Shift Length', 'Role']]

##Remove data older than 2 weeks
shiftdata_df['Date'] = pd.to_datetime(shiftdata_df['Date']).dt.date

shiftdata_df["now"] = pd.Timestamp.now()
shiftdata_df['now'] = shiftdata_df['now'].dt.date

shiftdata_df = shiftdata_df.loc[(shiftdata_df['Date'] + timedelta(days = 14)) >= shiftdata_df['now']]

##Ignore punches from today for first team
shiftdata_df.loc[(shiftdata_df['Role'] == 'FC Generalist Overnight') & (shiftdata_df['Date'] == shiftdata_df['now']), 'Date'] = None

shiftdata_df.dropna(subset=['Date'], inplace=True)

##Shift check
shiftdata_df["shift_check"] = shiftdata_df['Regular Hours'].astype('float64') / shiftdata_df['Shift Length'].astype('float64')

shiftdata_df = shiftdata_df.loc[(shiftdata_df['shift_check'] < 0.67) | (shiftdata_df['shift_check'] > 1.1)]

shiftdata_df = shiftdata_df[['Date', 'Primary Email', 'Regular Hours', 'Preferred Name', 'Supervisor', 'OPs Lead', 'Shift Length', 'shift_check']]

##Fill in missing lead
shiftdata_df.loc[shiftdata_df['Supervisor'] == '', 'Supervisor'] = shiftdata_df['OPs Lead']
shiftdata_df.loc[shiftdata_df['OPs Lead'] == '', 'OPs Lead'] = shiftdata_df['Supervisor']

##Merge shift data again on supervisor name
shiftdata_df = pd.merge(shiftdata_df, staffing_df, left_on='Supervisor', right_on='Preferred Name')

shiftdata_df.rename(columns={'Preferred Name_x':'Preferred Name', 'Supervisor_x':'Supervisor', 'OPs Lead_x':'OPs Lead', 'Shift Length_x':'Shift Length', 'Puncher':'Supervisor Email'}, inplace=True)

shiftdata_df = shiftdata_df[['Date', 'Primary Email', 'Regular Hours', 'Preferred Name', 'Supervisor', 'OPs Lead', 'Shift Length', 'shift_check', 'Supervisor Email']]

##Merge shift data again on lead name
shiftdata_df = pd.merge(shiftdata_df, staffing_df, left_on='OPs Lead', right_on='Preferred Name')

shiftdata_df.rename(columns={'Preferred Name_x':'Preferred Name', 'Supervisor_x':'Supervisor', 'OPs Lead_x':'OPs Lead', 'Shift Length_x':'Shift Length', 'Puncher':'OPs Lead Email',}, inplace=True)

shiftdata_df = shiftdata_df[['Date', 'Primary Email', 'Regular Hours', 'Preferred Name', 'Supervisor', 'OPs Lead', 'Shift Length', 'shift_check', 'Supervisor Email', 'OPs Lead Email']]

##Skip entries with supervisor = lead
shiftdata_df.loc[shiftdata_df['Supervisor'] == shiftdata_df['OPs Lead'], 'OPs Lead Email'] = None

##Split and recombine frame
shiftdata_df_2 = shiftdata_df.copy()
shiftdata_df = shiftdata_df[['Date', 'Primary Email', 'Regular Hours', 'Preferred Name', 'Supervisor', 'OPs Lead', 'Shift Length', 'shift_check', 'Supervisor Email']]
shiftdata_df_2 = shiftdata_df_2[['Date', 'Primary Email', 'Regular Hours', 'Preferred Name', 'Supervisor', 'OPs Lead', 'Shift Length', 'shift_check', 'OPs Lead Email']]

shiftdata_df.rename(columns={'Supervisor Email':'send_to_email'}, inplace=True)
shiftdata_df_2.rename(columns={'OPs Lead Email':'send_to_email'}, inplace=True)

shiftdata_df.rename(columns={'Supervisor':'Manager'}, inplace=True)
shiftdata_df_2.rename(columns={'OPs Lead':'Manager'}, inplace=True)

shiftdata_df = pd.concat([shiftdata_df, shiftdata_df_2])

shiftdata_df = shiftdata_df[['Date', 'Primary Email', 'Regular Hours', 'Preferred Name', 'Manager', 'send_to_email', 'Shift Length', 'shift_check']]

##Get list of unique managers
managers_df = shiftdata_df.copy()
managers_df = managers_df[['Manager']]
managers_df.drop_duplicates(subset=['Manager'], inplace=True)

managers_df = pd.merge(managers_df, staffing_df, left_on='Manager', right_on='Preferred Name')

managers_df = managers_df[['Manager', 'Puncher']]

##send emails
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import pandas as pd
import smtplib

def send_email(send_to, subject, df):
    send_from = "ac.data@tcgplayer-acw.com"
    password = "TcGp!TcGp!"

    if (len(df) > 0):
        message = "Greetings " + supervisor_or_lead + "! The potential time card issues for the past 2 weeks are attached."

    multipart = MIMEMultipart()
    multipart["From"] = send_from
    multipart["To"] = send_to_email
    multipart["Subject"] = subject

    if(len(df) > 0):
        attachment = MIMEApplication(send_to_df.to_csv(index=False))
        attachment["Content-Disposition"] = "attachment; filename={}".format(f"TimeCardIssues.csv")
        multipart.attach(attachment)

        multipart.attach(MIMEText(message, "html"))
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login(multipart["From"], password)
        server.sendmail(multipart["From"], multipart["To"], multipart.as_string())
        server.quit()


Subject = "Potential time card issues for the past 2 weeks."

for i in range(len(managers_df)):
    supervisor_or_lead = managers_df.iloc[i, 0]
    send_to_email = managers_df.iloc[i, 1]
    send_to_df = shiftdata_df.loc[shiftdata_df['send_to_email'] == send_to_email]

    send_to_df = send_to_df[['Date', 'Preferred Name', 'Regular Hours', 'Shift Length']]

    send_email(send_to_email, Subject, send_to_df)

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