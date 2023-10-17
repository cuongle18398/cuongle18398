import logging
import telegram
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, InputFile
from telegram.ext import Updater, MessageHandler, Filters, CallbackQueryHandler, CommandHandler
from sqlalchemy import create_engine
import pandas as pd
import pyodbc
import time
import openpyxl


#khai báo biến
#Khai báo server odbc
server = 'HSVSGNEDB07'
database = 'LSReport'
username = 'dd'
password = 'Hoahuongduong2908'
driver = 'SQL Server'

#Khai báo Bot
token = '6176024440:AAGQsxQHz53eV9bTAi5MyJFARwTDkzfROMk'
# Tạo đối tượng bot
bot = telegram.Bot(token=token)

# Khởi tạo biến user_interaction là một từ điển trống
user_interaction = {}

engine = create_engine(f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver}', use_setinputsizes=False)

# Initialize logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

# Create a logger for the bot and add a file handler
logger = logging.getLogger(__name__)
file_handler = logging.FileHandler('bot.log')
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)


def voucher(update, context):
    try:
        chat_id = update.message.chat_id
        # Get the user's query from the message text
        query = ' '.join(context.args)
        if query == '':
            update.message.reply_text("Bạn cần nhập mã voucher vào.")
        else:
            sql_query = f"EXEC [sp_Telegram_Check_Evoucher] @code = '{query}'"
        #Khai báo server odbc
        cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
        cursor = cnxn.cursor()
        cursor.execute(sql_query)
        rows = cursor.fetchall()
        # Save the data into a dataframe
        df = pd.DataFrame([tuple(row) for row in rows], columns=[desc[0] for desc in cursor.description])
        for index, row in df.iterrows():
            message = f"{row[0]}\n"
            context.bot.send_message(chat_id=chat_id, text=message)
            # Để tránh việc bot bị chặn hoặc gửi quá nhiều tin nhắn trong thời gian ngắn, thêm một đợi ngắn
            time.sleep(1)
        # df.to_excel("HSV_Voucher.xlsx", index=False)
        # # Send the Excel file via the Telegram chatbot
        # with open("HSV_Voucher.xlsx", "rb") as file:
        #     context.bot.send_document(chat_id=update.message.chat_id, document=InputFile(file))
    except Exception as e:
        logger.error("Error in voucher function: %s", e) 

        
def offer(update, context):
    try:
        chat_id = update.message.chat_id
        # Get the user's query from the message text
        query = ' '.join(context.args)
        if query == '':
            update.message.reply_text("Bạn cần nhập tham số vào (/offer [Brand] [Mã item] hoặc /offer [OfferNo]).")
        else:
            parts = query.split(' ')
            if len(parts) == 2:
                brand, item = parts
                sql_query = f"EXEC [sp_Telegram_Check_Offer] @brand = '{brand}', @item = '{item}'"
            else:
                offer = parts[0]
                sql_query = f"EXEC [sp_Telegram_Check_OfferNo] @offer = '{offer}'"
        #Khai báo server odbc
        cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
        cursor = cnxn.cursor()
        cursor.execute(sql_query)
        rows = cursor.fetchall()
        # Save the data into a dataframe
        df = pd.DataFrame([tuple(row) for row in rows], columns=[desc[0] for desc in cursor.description])
        df.to_excel("Offer.xlsx", index=False)
        # Send the Excel file via the Telegram chatbot
        with open("Offer.xlsx", "rb") as file:
            context.bot.send_document(chat_id=update.message.chat_id, document=InputFile(file))
    except Exception as e:
        logger.error("Error in offer function: %s", e) 


def error(update, context):
    logger.warning('Update "%s" caused error "%s"', update, context.error)


# Khởi tạo updater và thêm các trình xử lý (handler) cho bot
updater = Updater(token=token, use_context=True)
updater.dispatcher.add_handler(CommandHandler('voucher', voucher))
updater.dispatcher.add_handler(CommandHandler('offer', offer))
updater.dispatcher.add_error_handler(error)

# Chạy bot
logger.info('Bot started')
updater.start_polling()
updater.idle()