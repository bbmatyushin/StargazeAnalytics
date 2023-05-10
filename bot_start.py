import asyncio

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
from database.db_select import SelectQuery
from reports.report_24h import get_daily_report
from reports.report_blitz import get_blitz_report
from reports.report_wallets import WalletsReport

from handlers import admins, main, commands, addrs_monitoring


logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=logging.INFO)


@aiocron.crontab('1 5,12,20 * * *')
# @aiocron.crontab('*/1 * * * *')
async def send_blitz_report():
    """Ограничение - только для админов"""
    logging.info(f"Send BLITZ report at {datetime.now()}")
    report_text_ru = await get_blitz_report()
    admins = await UserDBSelect().select_admins_subscribe()
    for user in admins:
        try:
            await bot.send_message(chat_id=user, text=report_text_ru,
                                   parse_mode='HTML')
        except ChatNotFound as err:
            logging.warning(err)
        except BotBlocked as err:
            logging.warning(err)


@aiocron.crontab('10 11,23 * * *')
# @aiocron.crontab('*/1 * * * *')
async def send_daily_report():
    """Отправка отчётов изменений за последние 24 часа"""
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


@aiocron.crontab('*/2 * * * *')
# @aiocron.crontab('* * * * * */15')
async def addrs_monitoring():
    """Из БД с собраной инфой переноси в БД Users активности за
    наблюдаемыми кошельками."""
    addrs_monitor_list = await UserDBSelect().select_addrs_monitoring()  # адреса для отслеживания
    if addrs_monitor_list:
        for addr in addrs_monitor_list:
            # получаем данные по ПРОДАЖАМ из БД с собраной инфой
            async for sales_data_insert in SelectQuery().select_for_sales_monitoring(owner_addr=addr):
                if sales_data_insert:  # если что-то есть, то переносим их в БД users
                    try:
                        await UserDBInsert().insert_sales_monitoring(sales_data_insert)
                    except aiosqlite.IntegrityError as err:
                        logging.error(f"SALES data not insert - {err}")
                else:
                    logging.info('No SALES DATA to address monitoring')
            # получаем данные по ПОКУПКАМ из БД с собраной инфой
            async for buys_data_insert in SelectQuery().select_for_buys_monitoring(owner_addr=addr):
                if buys_data_insert:
                    try:
                        await UserDBInsert().insert_buys_monitoring(buys_data_insert)
                    except aiosqlite.IntegrityError as err:
                        logging.warning(f"BUYS data not insert - {err}")
                else:
                    logging.info('No BUYS DATA to address monitoring')


async def send_bot_msg_about_action(user_id: int, msg: str,
                                    action: str, monitor_id: int):
    """Отправление ботом сообщений об активностях на кошельках"""
    try:
        await bot.send_message(chat_id=user_id, text=msg,
                               parse_mode='HTML')  # отправляем каждому сообщение о сделке
        try:  # в таблицу send_monitor_info вставляем данные об отправке сообщения
            await UserDBInsert().insert_send_monitor_info(user_id=user_id, monitor_id=monitor_id,
                                                          action=action)
        except aiosqlite.IntegrityError as err:
            logging.error(err)
    except ChatNotFound as err:
        logging.warning(err)
        try:  # добавляем пользователя как неактивного
            await UserDBInsert().insert_inactive_users(user_id=user_id)
            logging.info(f"Inactive User {user_id}")
        except aiosqlite.IntegrityError as err:
            logging.error(err)
    except BotBlocked as err:
        logging.warning(err)
        try:  # добавляем пользователя как неактивного
            await UserDBInsert().insert_inactive_users(user_id=user_id)
            logging.info(f"User {user_id} block bot")
        except aiosqlite.IntegrityError as err:
            logging.error(err)


@aiocron.crontab('*/2 * * * *')
# @aiocron.crontab('* * * * * */20')
async def send_actions_monitor_info():
    """Задача на отправку пользователям сообщений о ПОКУПКАХ"""
    sales_monitor_ids = await UserDBSelect().select_no_send_sales_monitor_id()  # id неотрпавленных активностей (продаж)
    buys_monitor_ids = await UserDBSelect().select_no_send_buys_monitor_id()  # id неотрпавленных активностей (ПОКУПКИ)
    addrs_monitor_list = await UserDBSelect().select_addrs_monitoring()  # адреса для отслеживания
    for addr in addrs_monitor_list:
        # список пользоватлей, кот.следят за этим адресом
        users = await UserDBSelect().select_users_monitoring(addr_monitor=addr)
        if sales_monitor_ids:
            for monitor_id in sales_monitor_ids:
                # тут формируется сообщение для отправки
                sales_output_msg = await WalletsReport().wallet_sales_report(owner_addr=addr,
                                                                             monitor_id=monitor_id)
                if sales_output_msg:
                    logging.info(f"New sales report from wallet {addr}")
                    for user in users:
                        await send_bot_msg_about_action(user_id=user, msg=sales_output_msg,
                                                        action='sale', monitor_id=monitor_id)
        if buys_monitor_ids:
            for monitor_id in buys_monitor_ids:
                # тут формируется сообщение для отправки
                buys_output_msg = await WalletsReport().wallet_buys_report(owner_addr=addr,
                                                                           monitor_id=monitor_id)
                if buys_output_msg:
                    logging.info(f"New BUYS report from wallet {addr}")
                    for user in users:
                        await send_bot_msg_about_action(user_id=user, msg=buys_output_msg,
                                                        action='buy', monitor_id=monitor_id)


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
        BotCommand(command='/bot_menu',
                   description='меню Бота'),
        BotCommand(command='/cancel',
                   description='отменить действия')
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
