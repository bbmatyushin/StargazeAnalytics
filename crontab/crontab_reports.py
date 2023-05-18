"""Отправка отчетов по расписанию"""
import aiocron
import logging
import aiosqlite

from aiogram.utils.exceptions import ChatNotFound, BotBlocked
from datetime import datetime

from usefultools.create_bot import bot
from database.user_db_select import UserDBSelect
from database.user_db_insert import UserDBInsert
from reports.report_24h import get_daily_report
from reports.report_7d import get_weekly_report
from reports.report_blitz import get_blitz_report
# from reports.report_whales import get_whales_report


logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=logging.INFO)


@aiocron.crontab('1 5,13,21 * * *')
# @aiocron.crontab('*/1 * * * *')
async def send_blitz_report():
    """Ограничение - только для админов"""
    logging.info(f"Send BLITZ report at {datetime.now()}")
    report = await get_blitz_report()
    report_text_ru = "".join(report[:10])
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
    users_subscribe = await UserDBSelect().select_subscribe_users_report(report_name='24h_report')
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


@aiocron.crontab('10 19 * * 7')
# @aiocron.crontab('* * * * * 30')
async def send_weekly_report():
    """ЕЖЕНЕДЕЛЬНАЯ рассылка. Отчёт за 7 дней"""
    logging.info(f"Send WEEKLY report at {datetime.now()}")
    report_text_ru = await get_weekly_report()
    users_subscribe = await UserDBSelect().select_subscribe_users_report(report_name='7d_report')
    try:  # добавляем отчет в таблицу с ЕЖЕНЕДЕЛЬНЫМИ отчетами
        await UserDBInsert().insert_weekly_reports(message=report_text_ru)
    except aiosqlite.IntegrityError as err:
        logging.error(err)
    last_report_id = await UserDBSelect().select_weekly_report_id()
    for user in users_subscribe:
        try:
            await bot.send_message(chat_id=user, text=report_text_ru,
                                   parse_mode='HTML')
            try:  # отмечаем флагом, что пользователю отчет отправлен
                await UserDBInsert().insert_send_weekly_report(user_id=user, report_id=last_report_id)
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
#
# @aiocron.crontab('* * * * * */30')
# async def send_monthly_report():
#     logging.info(f"Send MONTHLY report at {datetime.now()}")
#     print('MONTHLY report!')


# @aiocron.crontab('10 12 * * 7')
# async def send_whales_report():
#     """Отправка отчётов о китах"""
#     #TODO: ДОДЕЛАТЬ
#     logging.info(f"Send WHALES report at {datetime.now()}")
#     report = await get_blitz_report()
#     pass