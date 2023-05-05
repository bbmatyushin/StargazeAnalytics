import aiosqlite

import aiocron
import logging

from datetime import datetime

from aiogram.types import BotCommand
from aiogram.utils import executor
from aiogram.utils.exceptions import ChatNotFound, BotBlocked

from usefultools.create_bot import bot, dp
from database.user_db_select import UserDBSelect, UserDB
from database.user_db_insert import UserDBInsert
from reports.report_24h import get_daily_report
from reports.report_blitz import get_blitz_report

from handlers import main, commands


logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=logging.INFO)


@aiocron.crontab('0 */1 * * *')
async def send_blitz_report():
    """Ограничение - только для админов"""
    logging.info(f"Send BLITZ report at {datetime.now()}")
    report_text_ru = await get_blitz_report()
    admins = await UserDBSelect().select_admins()
    for user in admins:
        try:
            await bot.send_message(chat_id=user, text=report_text_ru,
                                   parse_mode='HTML')
        except ChatNotFound as err:
            logging.warning(err)
        except BotBlocked as err:
            logging.warning(err)


@aiocron.crontab('10 1 * * *')
async def send_daily_report():
    logging.info(f"Send DAILY report at {datetime.now()}")
    report_text_ru = await get_daily_report()
    users_subscribe = await UserDBSelect().select_subscribe_users()
    try:  # добавляем отчет в таблицу с ежедневными отчетами
        await UserDBInsert().insert_daily_reports(message=report_text_ru)
    except aiosqlite.IntegrityError as err:
        logging.error(err)
    last_report_id = await UserDBSelect().select_daily_report_id()
    for user in users_subscribe:
        try:
            await bot.send_message(chat_id=user, text=report_text_ru,
                                   parse_mode='HTML')
            try:  # отмечаем флагом, что пользователю отчет отправлен
                await UserDBInsert().insert_send_daily_report(user_id=user, report_id=last_report_id)
            except aiosqlite.IntegrityError as err:
                logging.error(err)
        except ChatNotFound as err:
            logging.warning(err)
            try:  # добавляем пользователя как неактивного
                await UserDBInsert().insert_inactive_users(user_id=user)
            except aiosqlite.IntegrityError as err:
                logging.error(err)
        except BotBlocked as err:
            logging.warning(err)
            try:  # добавляем пользователя как неактивного
                await UserDBInsert().insert_inactive_users(user_id=user)
            except aiosqlite.IntegrityError as err:
                logging.error(err)

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
    try:
        await UserDB().create_tables()  # создаем таблицы для пользоватлей
    except:
        logging.error("Create users tables ERROR!")
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
        BotCommand(command='/get_report',
                   description='получить отчёт 24Ч')
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
