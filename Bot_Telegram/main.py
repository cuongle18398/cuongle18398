import logging
import telegram
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, InputFile
from telegram.ext import Updater, MessageHandler, Filters, CallbackQueryHandler, CommandHandler
import time
from apscheduler.schedulers.background import BackgroundScheduler
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import pyodbc
import openpyxl

#Khai báo server odbc
server = 'HSVSGNEDB07'
database = 'LSReport'
username = 'dd'
password = 'Hoahuongduong2908'
driver = 'SQL Server'

# Initialize logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

# Create a logger for the bot and add a file handler
logger = logging.getLogger(__name__)
file_handler = logging.FileHandler('bot.log')
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)

# tạo dataframe lựa chọn của user
df = pd.DataFrame(columns=['Datetime', 'Group', 'UserID', 'UserName', 'Message', 'Selection'])

# Tạo đối tượng bot
bot = telegram.Bot(token='6294192554:AAE0B3FSEp139llrPOw2ws0sIj-zTgX5y9A')

# Khởi tạo biến user_interaction là một từ điển trống
user_interaction = {}

engine = create_engine(f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver}', use_setinputsizes=False)

# Xử lý lệnh /start
def start(update, context):
    try:
        # Kiểm tra xem người dùng có phải là quản trị viên hay không
        chat_id = update.message.chat_id
        user_id = update.message.from_user.id
        chat_member = context.bot.get_chat_member(chat_id, user_id)
        chat(update=update,context=context)
        if chat_member.status in ['member']:
            # Tạo các nút lựa chọn cho người dùng
            keyboard = [
                [InlineKeyboardButton("ERP(POS, TO, PO)", callback_data='1'),
                InlineKeyboardButton("PC + Máy Scan", callback_data='2'),
                InlineKeyboardButton("Máy In", callback_data='3')],
                [InlineKeyboardButton("Mạng", callback_data='4'),
                InlineKeyboardButton("Khuyến mãi", callback_data='5'),
                InlineKeyboardButton("Khác (C&C, Mail, ...)", callback_data='6')],
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            # Gửi tin nhắn chứa nút bấm
            message = update.message.reply_text('Xin chào, vấn đề bạn đang gặp phải là gì ? (vui lòng chọn nút tương ứng):', reply_markup=reply_markup)
            user_interaction[user_id] = True
            # # Wait for 1 minute
            # time.sleep(30)
            # # Remove keyboard
            # bot.edit_message_reply_markup(chat_id=message.chat_id,message_id=message.message_id,reply_markup=None)
            # context.bot.delete_message(chat_id=message.chat_id, message_id=message.message_id)
            # del user_interaction[user_id]
            return False
    except Exception as e:
        logger.error("Error in start function: %s", e)


# Xử lý câu trả lời từ người dùng
def button(update, context):
    try:
        query = update.callback_query
        group = update.callback_query.message.chat.title
        user_id = update.callback_query.from_user.id
        user_name = update.callback_query.from_user.first_name
        date_time = pd.to_datetime((update.callback_query.message.date + timedelta(hours=7)).strftime('%Y-%m-%d %H:%M:%S'))
        if user_id in user_interaction:
        # Xử lý tùy chọn được chọn bởi người dùng
            if query.data == '1':
                message_text ="[ERP] Bạn có thể tắt trình duyệt, POS và mở lại để khắc phục lỗi trong lúc chờ bộ phận IT phản hồi thông tin."
                issue_type = "ERP(POS/TO/Vận hành)"
                sql_query = f"EXEC [KillSessions]"
                cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
                cursor = cnxn.cursor()
                cursor.execute(sql_query)
            elif query.data == '2':
                message_text ="[PC+Scan] Bạn có thể khởi động lại thiết bị để khắc phục lỗi trong lúc chờ bộ phận IT phản hồi thông tin."
                issue_type = "PC + Máy scan"
            elif query.data == '3':
                message_text ="[Máy in] Bạn có thể kiểm tra nguồn máy in hoặc khởi động lại máy tính trong lúc chờ bộ phận IT phản hồi thông tin."
                issue_type = "Máy in"
            elif query.data == '4':
                message_text ="[Mạng] Bạn nên kiểm tra Modem mạng có hoạt động không trong lúc chờ bộ phận IT phản hồi thông tin."
                issue_type = "Mạng"
            elif query.data == '5':
                message_text ="[CTKM] Bạn nên kiểm tra các item có nằm trong CTKM không trong lúc chờ bộ phận IT phản hồi thông tin."
                issue_type = "Khuyến mãi"
            elif query.data == '6':
                message_text ="[Khác] Bộ phận IT đã nhận thông tin và sẽ phản hồi lại."
                issue_type = "Khác"
            query.edit_message_text(text=message_text)
            del user_interaction[user_id]

            # Thêm dữ liệu vào dataframe
            df.loc[len(df)] = [date_time, group, user_id, user_name, message_text, issue_type]

            #Ghi du lieu vao db
            df.to_sql('Telegram_ChatBot_History', engine, if_exists='append', index=False)
            # Kiểm tra xem DataFrame có trống không
            if not df.empty:
                # Xóa tất cả các hàng
                df.drop(index=df.index, inplace=True)
        else:
            update.callback_query.answer(text="Bạn không có quyền tương tác với bàn phím này!")
    except Exception as e:
        logger.error("Error in button function: %s", e)


def chat(update, context):
    try:
        group = update.message.chat.title
        user_id = update.message.from_user.id
        user_name = update.message.from_user.first_name
        date_time = pd.to_datetime(update.message.date + timedelta(hours=7)).strftime('%Y-%m-%d %H:%M:%S')
        message_text = update.message.text
        issue_type =  ""
        # Thêm dữ liệu vào dataframe
        df.loc[len(df)] = [date_time, group, user_id, user_name, message_text, issue_type]
        #Ghi du lieu vao db
        df.to_sql('Telegram_ChatBot_History', engine, if_exists='append', index=False)
        # Kiểm tra xem DataFrame có trống không
        if not df.empty:
            # Xóa tất cả các hàng
            df.drop(index=df.index, inplace=True)
    except Exception as e:
        logger.error("Error in chat function: %s", e) 

def chat_history(update, context):
    try:
        # Get the user's query from the message text
        query = ' '.join(context.args)
        if query == '':
            sql_query = f"SELECT * FROM Telegram_ChatBot_History"
        else:
            sql_query = f"SELECT * FROM Telegram_ChatBot_History WHERE MONTH([Datetime]) = '{query}'"
        #Khai báo server odbc
        cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
        cursor = cnxn.cursor()
        cursor.execute(sql_query)
        rows = cursor.fetchall()
        # Save the data into a dataframe
        df = pd.DataFrame([tuple(row) for row in rows], columns=[desc[0] for desc in cursor.description])
        df.to_excel("Telegramchat_data.xlsx", index=False)
        # Send the Excel file via the Telegram chatbot
        with open("Telegramchat_data.xlsx", "rb") as file:
            context.bot.send_document(chat_id=update.message.chat_id, document=InputFile(file))
    except Exception as e:
        logger.error("Error in chat history function: %s", e) 

def remove_inactive_users():
    try:
        for user_id in list(user_interaction.keys()):
            # Check if the user was active within the last 5 minutes
            last_active_time = user_interaction[user_id]
            if time.time() - last_active_time > 300:
                # Remove the user from user_interaction
                del user_interaction[user_id]
    except Exception as e:
        logger.error("Error in remove inactive user function: %s", e) 

# Tạo một scheduler
scheduler = BackgroundScheduler()

# Thêm một job vào scheduler, lên lịch chạy mỗi phút một lần
scheduler.add_job(remove_inactive_users, 'interval', minutes=1)

# Bắt đầu chạy scheduler
scheduler.start()

def error(update, context):
    logger.warning('Update "%s" caused error "%s"', update, context.error)

# Khởi tạo updater và thêm các trình xử lý (handler) cho bot
updater = Updater(token='6294192554:AAE0B3FSEp139llrPOw2ws0sIj-zTgX5y9A', use_context=True)
updater.dispatcher.add_handler(MessageHandler(Filters.text & Filters.regex(r'(?i)\bHi IT\b'), start))
updater.dispatcher.add_handler(CommandHandler('ch', chat_history))
updater.dispatcher.add_handler(MessageHandler(Filters.text, chat))
updater.dispatcher.add_handler(CallbackQueryHandler(button))
updater.dispatcher.add_error_handler(error)

# Chạy bot
logger.info('Bot started')
updater.start_polling()
updater.idle()
