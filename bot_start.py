import aiocron
import logging

from datetime import datetime

from aiogram.types import BotCommand
from aiogram.utils import executor
from aiogram.utils.exceptions import ChatNotFound, BotBlocked

from usefultools.create_bot import bot, dp
from database.user_db_select import UserDBSelect
from database.user_db_insert import UserDBInsert
from reports.report_24h import get_daily_report

from handlers import main

logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=logging.INFO)


@aiocron.crontab('*/1 * * * *')
async def send_daily_report():
    logging.info(f"Send DAILY report at {datetime.now()}")
    report_text_ru = await get_daily_report()
    users_subscribe = await UserDBSelect().select_subscribe_users()
    for user in users_subscribe:
        try:
            await bot.send_message(chat_id=user, text=report_text_ru,
                                   parse_mode='HTML')
        except ChatNotFound as err:
            logging.warning(err)
        except BotBlocked as err:
            logging.warning(err)

#
# @aiocron.crontab('* * * * * */20')
# async def send_weekly_report():
#     logging.info(f"Send WEEKLY report at {datetime.now()}")
#     print('WEEKLY report!')
#
#
# @aiocron.crontab('* * * * * */30')
# async def send_monthly_report():
#     logging.info(f"Send MONTHLY report at {datetime.now()}")
#     print('MONTHLY report!')


async def on_startup(_):
    text_ru = f"#restart_bot\n\n" \
              f"🤖 Бот успешно перезагрузился."
    admins_list = await UserDBSelect().select_admins()
    for admin in admins_list:
        try:
            await bot.send_message(chat_id=admin, text=text_ru)
        except ChatNotFound as err:
            logging.warning(err)
        except BotBlocked as err:
            logging.warning(err)
    menu_commands = [
        BotCommand(command='/start',
                   description='перезапустить бота')
    ]
    await bot.set_my_commands(menu_commands)


async def on_shutdown(_):
    text_ru = f"#shutdown_bot\n\n" \
           f"🤖 Бот упал..."
    admins_list = await UserDBSelect().select_admins()
    for admin in admins_list:
        try:
            await bot.send_message(chat_id=admin, text=text_ru)
        except ChatNotFound as err:
            logging.warning(err)
        except BotBlocked as err:
            logging.warning(err)


if __name__ == "__main__":
    executor.start_polling(dp, timeout=120, skip_updates=True,
                           on_startup=on_startup, on_shutdown=on_shutdown)
