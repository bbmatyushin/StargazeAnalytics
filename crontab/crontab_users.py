"""Рассылка сообщений пользователям по расписанию"""
import aiocron
import asyncio
import logging
import aiosqlite

from aiogram.utils.exceptions import ChatNotFound, BotBlocked

from usefultools.create_bot import bot
from database.user_db_select import UserDBSelect
from database.user_db_insert import UserDBInsert
from reports.report_wallets import WalletsReport


logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=logging.INFO)


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
    logging.info("Start send_actions_monitor_info()")
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


if __name__ == "__main__":
    # test_func.start()
    # test_func_2.start()
    asyncio.get_event_loop().run_forever()
    # asyncio.run(main())
