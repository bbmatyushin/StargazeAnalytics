"""Работа с БД по расписанию"""
import aiocron
import logging
import aiosqlite
import asyncio

from database.user_db_select import UserDBSelect, UserDB
from database.user_db_insert import UserDBInsert
from database.db_select import SelectQuery


logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=logging.INFO)


@aiocron.crontab('*/2 * * * *')
# @aiocron.crontab('* * * * * */15')
async def addrs_monitoring():
    """Из БД с собраной инфой переноси в БД Users активности за
    наблюдаемыми кошельками."""
    logging.info("Start function addrs_monitoring()")
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


@aiocron.crontab('3 */4 * * *')
async def transfer_owners():
    """Получаем адрес и имя кошелька из основной БД
    для переноса этой инфы в БД Users"""
    async for insert_data in SelectQuery().select_owner_addr_name():
        try:
            await UserDBInsert().insert_owners(insert_data)
            logging.info("Insert NEW OWNER address.")
        except aiosqlite.IntegrityError:
            logging.warning(f"Insert OWNER Error. Not unique addr.")


if __name__ == "__main__":
    asyncio.run(transfer_owners())
