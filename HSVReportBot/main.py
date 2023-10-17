import telegram
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, InputFile
from telegram.ext import Updater, MessageHandler, Filters, CallbackQueryHandler, CommandHandler
from apscheduler.schedulers.background import BackgroundScheduler
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import pyodbc
import os
import shutil

#Khai báo server odbc
server = 'HSVSGNEDB07'
database = 'LSReport'
username = 'dd'
password = 'Hoahuongduong2908'
driver = 'SQL Server'
engine = create_engine(f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver}', use_setinputsizes=False)

# Tạo đối tượng bot
bot = telegram.Bot(token='6384410860:AAGPi-SJZ7Pnx_BsFeRkTAsHmlXDDE6XRcQ')

def chat(update, context):
        text = update.message.text
        message_text = ''
        folder_path = 'C:\HSV.Services\HSVReportBot'
        if text.startswith('/offercalendar'):
                if len(text) == 14:
                    message_text = '/offercalendarTFS: Kiểm tra CTKM trong tháng brand TFS\n/offercalendarCBX: Kiểm tra CTKM trong tháng brand CBX\n/offercalendarCCL: Kiểm tra CTKM trong tháng brand CCL\n/offercalendarFRB: Kiểm tra CTKM trong tháng brand FRB\n'
                else:
                    brand = text[14:]
                    sql_query = f"EXEC [offer_list] @br = '{brand}'"
                    with pyodbc.connect(f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}', autocommit=True) as cnxn:
                        df = pd.read_sql_query(sql_query, cnxn)  
                        if not df.empty:   
                            # Save the DataFrame to a CSV file
                            folder_name = "OfferCalendar.xlsx"
                            df.to_excel(folder_name, index=False)
                            # clear the DataFrame when done
                            df.drop(index=df.index, inplace=True)  # drops all rows
                            df.drop(columns=df.columns, inplace=True)  # drops all columns     
                            # Send the Excel file via the Telegram chatbot
                            with open(folder_name, "rb") as file:
                                context.bot.send_document(chat_id=update.message.chat_id, document=InputFile(file)) 
                            cnxn.close() 
        if text.startswith('/'):            
                message_text = text
        else:
                message_text = '/offercalendar: Kiểm tra CTKM trong tháng\n/[mã item]: Kiểm tra thông tin item'

        if message_text != '':
            context.bot.send_message(chat_id=update.message.chat_id, text=message_text)

# Khởi tạo updater và thêm các trình xử lý (handler) cho bot
updater = Updater(token='6384410860:AAGPi-SJZ7Pnx_BsFeRkTAsHmlXDDE6XRcQ', use_context=True)
updater.dispatcher.add_handler(MessageHandler(Filters.text, chat))

# Chay bot
updater.start_polling()
updater.idle()