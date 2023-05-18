import aiosqlite
# import schedule
import asyncio
import aiocron

from datetime import datetime
from aiologger import Logger
from aiologger.levels import LogLevel
from aiologger.formatters.base import Formatter

from database.db_insert import InsertIntoDB, MainDB
from database.db_select import SelectQuery
from database.db_owners_tokens import OwnersTokensDB
from database.db_mint import MintDB
from parser.parsing import Parser

# formatter = Formatter('%(asctime)s [%(levelname)s] %(filename)s -> %(funcName)s line:%(lineno)d %(message)s')
formatter = Formatter('%(asctime)s [%(levelname)s] %(message)s')
logger = Logger.with_default_handlers(name='my-logger', formatter=formatter, level=LogLevel.INFO)

class GetDataStargaze:
    async def main_insert(self, coll_addr: str, token_num: int):
        """Вставляем данные в таблицы collections, tokens, rarity"""
        if coll_addr:
            coll_data = await Parser().get_collection_data(coll_addr)  # получаем данные по коллекции через api
            # ==> coll_name, floor_price, tokens_count, creator_addr
            if not isinstance(coll_data, int):  # если там не статус код с ошибкой
                coll_name, floor_price, tokens_count, creator_addr = \
                    coll_data[0], coll_data[1], coll_data[2], coll_data[3]
                floor_price = floor_price if floor_price else None
                insert_coll_data = [coll_name, coll_addr, tokens_count]
                try:  # наполняем таблицу collections
                    await logger.info(f"Try insert collection {coll_name}")
                    await InsertIntoDB().insert_collections(insert_data=insert_coll_data)
                    await logger.info(f'[+] Insert new collection {coll_name}')
                except aiosqlite.IntegrityError as warn:
                    await logger.warning(warn)
                coll_id = await SelectQuery().select_coll_id(coll_addr)
                # coll_id = coll_id[0] if coll_id else None
                token_data = await Parser().get_tokens_data(coll_addr, token_num)  # получаем данные токена
                if not isinstance(token_data, int):  # если там не статус код с ошибкой
                    token_name, rarity = token_data[0], token_data[1]
                    insert_token_data = [coll_id, token_name, int(token_num)]
                    try:  # наполняем таблицу tokens
                        await logger.info(f"Try insert token {token_name}")
                        await InsertIntoDB().insert_tokens(insert_token_data)
                        await logger.info(f"[+] Insert new token {token_name} from {coll_name}")
                    except aiosqlite.IntegrityError as warn:
                        await logger.warning(warn)
                    token_id = await SelectQuery().select_token_id(coll_id, int(token_num))
                    # token_id = token_id[0] if token_id else None
                    rarity_max = await SelectQuery().select_rarity_max(token_id)  # получаем самую большую редкость
                    # rarity_max = rarity_max[0] if rarity_max else None
                    try:
                        # Заполняем таблицу rarity
                        await logger.info(f"Try insert rarity {token_name} Rarity #{rarity}/{rarity_max}")
                        await InsertIntoDB().insert_rarity([token_id, rarity, rarity_max])
                        await logger.info(f"[+] Insert new rarity: {token_name} Rarity #{rarity}/{rarity_max}")
                    except aiosqlite.IntegrityError as warn:
                        await logger.warning(warn)
                    return coll_id, coll_name, token_id, floor_price
        else:
            return None, None, None, None

    async def owners_inser(self, owner_addr: str):
        """Вставляем данные владельцев"""
        if owner_addr:
            owner_name = await Parser().get_owner_name(owner_addr)
            insert_owner_data = [owner_addr, owner_name]
            try:
                # Заполняем таблицу owners
                await InsertIntoDB().insert_owners(insert_owner_data)
                await logger.info(f"Insert new owner 👤 {owner_addr} / {owner_name}")
            except aiosqlite.IntegrityError:
                pass

    async def sales_parsing(self):
        """Получаем данные по продажам.
        Заполняем таблицы collections, tokens, rarity, floors, owners, sales новыми данными"""
        await logger.info(" ############ Get Sales data.. ############ ")
        data_sales = Parser().get_sales_data()
        async for data in data_sales:
            if data:  # Если вдруг вернется None
                block_height, coll_addr, token_num, price_stars, price_usd, seller_addr, buyer_addr, date_create = \
                    data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7]
                coll_id, coll_name, token_id, floor_price = await self.main_insert(coll_addr, token_num)
                if coll_id:
                    await self.owners_inser(seller_addr)
                    await self.owners_inser(buyer_addr)
                    seller_id = await SelectQuery().select_owner_id(seller_addr)
                    # seller_id = seller_id[0] if seller_id else None
                    buyer_id = await SelectQuery().select_owner_id(buyer_addr)
                    # buyer_id = buyer_id[0] if buyer_id else None
                    insert_sales_data = [block_height, coll_id, token_id, seller_id, buyer_id,
                                         price_stars, price_usd, date_create]
                    try:
                        # Заполняем таблицу sales
                        await InsertIntoDB().insert_sales(insert_sales_data)
                        await logger.info(f"Insert new sale 💵 | {block_height} | {coll_name} "
                                     f"{token_num} {price_stars} STARS create at {date_create}")
                    except aiosqlite.IntegrityError as warn:
                        await logger.warning(warn)
                    try:
                        # Заполняем таблицу floors
                        await InsertIntoDB().insert_floors([coll_id, floor_price])
                        await logger.info(f"Insert floor price {floor_price} STARS {coll_name}")
                    except aiosqlite.IntegrityError as warn:
                        await logger.warning(warn)
                    except aiosqlite.OperationalError as err:
                        await logger.error(err)

    async def burns_parsing(self):
        """Получаем данные по сжиганиям токенов.
        Заполняем таблицы collections, tokens, rarity, owners, burns новыми данными"""
        await logger.info(" ############ Get Burns data.. ############ ")
        data_burns = Parser().get_burn_data()
        # block_height, coll_addr, token_num, sender_addr, date_create
        async for data in data_burns:
            if data:
                block_height, coll_addr, token_num, sender_addr, date_create = \
                    data[0], data[1], data[2], data[3], data[4]
                coll_id, coll_name, token_id, floor_price = await self.main_insert(coll_addr, token_num)
                if coll_id:
                    await self.owners_inser(sender_addr)
                    sender_id = await SelectQuery().select_owner_id(sender_addr)
                    # sender_id = sender_id[0] if sender_id else None
                    insert_burns_data = [block_height, coll_id, token_id, sender_id, date_create]
                    try:
                        await InsertIntoDB().insert_burns(insert_burns_data)
                        await logger.info(f"Insert new burn 🔥 | {block_height} | {coll_name} {token_num} "
                                     f"create at {date_create}")
                    except aiosqlite.IntegrityError as warn:
                        await logger.warning(warn)
                    except aiosqlite.OperationalError as err:
                        await logger.error(err)

    async def mints_parsing(self):
        """Получаем данные по минтам.
        Заполняем таблицы collections, tokens, rarity, owners, mints новыми данными"""
        await logger.info(" ############ Get Mints data.. ############ ")
        # data_mints = Parser().get_mints_data()
        async for data in MintDB().select_mints_data():
            if data:
                block_height, coll_addr, token_num, recipient_addr, creator_addr, \
                    price_stars, price_usd, date_create = data[0], data[1], data[2], \
                    data[3], data[4], data[5], data[6], data[7]
                coll_id, coll_name, token_id, floor_price = await self.main_insert(coll_addr, token_num)
                await self.owners_inser(recipient_addr)
                await self.owners_inser(creator_addr)
                recipient_id = await SelectQuery().select_owner_id(recipient_addr)
                # recipient_id = recipient_id[0] if recipient_id else None
                creator_id = await SelectQuery().select_owner_id(creator_addr)
                # creator_id = creator_id[0] if creator_id else None
                insert_mints_data = [block_height, coll_id, token_id, recipient_id,
                                     creator_id, price_stars, price_usd, date_create]
                try:
                    await InsertIntoDB().insert_mints(insert_mints_data)
                    await logger.info(f"Insert new mint 🍃 | {block_height} | {coll_name} {token_num}  "
                                      f"{price_stars} STARS create at {date_create}")
                except aiosqlite.IntegrityError as warn:
                    await logger.warning(warn)
                except aiosqlite.OperationalError as err:
                    await logger.error(err)

    async def listings_parsing(self):
        """Получаем данные по новым размещениям.
        Заполняем таблицы collections, tokens, rarity, owners, mints новыми данными"""
        await logger.info(" ############ Get Listing data.. ############ ")
        data_listing = Parser().get_listing_and_update_price_data()
        async for data in data_listing:
            if data:
                block_height, coll_addr, token_num, seller_addr, price_stars, price_usd, \
                    event_name, date_create = data[0], data[1], data[2], \
                    data[3], data[4], data[5], data[6], data[7]
                coll_id, coll_name, token_id, floor_price = await self.main_insert(coll_addr, token_num)
                if seller_addr:
                    await self.owners_inser(seller_addr)
                    seller_id = await SelectQuery().select_owner_id(seller_addr)
                    # seller_id = seller_id[0] if seller_id else None
                else:
                    seller_id = None
                insert_mints_data = [block_height, coll_id, token_id, seller_id,
                                     price_stars, price_usd, event_name, date_create]
                try:
                    await InsertIntoDB().insert_listings(insert_mints_data)
                    await logger.info(f"Insert new listing 📈 | {block_height} | {coll_name} {token_num} "
                                      f"{price_stars} STARS create at {date_create}")
                except aiosqlite.IntegrityError as warn:
                    await logger.warning(warn)
                except aiosqlite.OperationalError as err:
                    await logger.error(err)

    async def check_owners_tokens(self):
        """Проверяем какие токены есть на кошельке все адреса"""
        # insert_data = [owner_id, coll_id, token_id, for_sale, date_create]
        count_owner = await SelectQuery().select_count_owners()
        count = count_owner
        async for owner_addr in SelectQuery().select_owner_addr():  # вытаскиваем по очереди адреса всех кошельков
            async for data in Parser().owners_parsing(owner_addr):  # получаем ео данные
                await logger.info(f"Addr {count}/{count_owner}. Get data {data}")
                coll_addr, token_name, token_num, for_sale, date_create = \
                    data[0], data[1], data[2], data[3], data[4]
                if await SelectQuery().select_owner_id(owner_addr):  # такой владелец уже есть в БД
                    owner_id = await SelectQuery().select_owner_id(owner_addr)
                else:
                    await self.owners_inser(owner_addr)
                    owner_id = await SelectQuery().select_owner_id(owner_addr)
                if await SelectQuery().select_coll_id(coll_addr):  # если такая коллекция уже есть в БД
                    coll_id = await SelectQuery().select_coll_id(coll_addr)
                    if await SelectQuery().select_token_id(coll_id, int(token_num)):  # если токен есть в БД
                        token_id = await SelectQuery().select_token_id(coll_id, int(token_num))
                    else:  # если токена нет то добавляем его и получаем его id
                        try:
                            insert_token_data = [coll_id, token_name, token_num]
                            await InsertIntoDB().insert_tokens(insert_token_data)
                            await logger.info(f"Addr {count}/{count_owner}. [+] Insert new token {token_name} from Coll_ID {coll_id}")
                        except aiosqlite.IntegrityError:
                            await logger.info(f"Addr {count}/{count_owner}. Insert UNIQUE value.")
                        token_id = await SelectQuery().select_token_id(coll_id, int(token_num))
                else:
                    coll_id, coll_name, token_id, floor_price = await self.main_insert(coll_addr, token_num)
                insert_data = [owner_id, coll_id, token_id, for_sale, date_create]  # получаем данные для вставки
                await logger.info(f"Addr {count}/{count_owner}. Get insert data {insert_data}")
                try:
                    await OwnersTokensDB().insert_owners_tokens_single(insert_data)
                    await logger.info(f"Addr {count}/{count_owner}. Add new token for OWNER {owner_addr[:8]}...{owner_addr[-4:]}")
                except aiosqlite.IntegrityError:
                    await logger.info(f"Addr {count}/{count_owner}. Insert UNIQUE value.")
            count -= 1

    async def insert_owners_tokens_in_main(self):
        """Вставка из БД owners_tokens в основнуб БД в таблицу owners_tokens"""
        count_rows = await OwnersTokensDB().select_count_rows()
        count = count_rows
        async for insert_data in OwnersTokensDB().select_owners_tokens_single():
            try:
                await InsertIntoDB().insert_owners_tokens(insert_data)
                await logger.info(f"Row {count}/{count_rows} insert DATA OWNERS_TOKENS to Main DB - {insert_data}")
            except aiosqlite.IntegrityError as err:
                await logger.warning(f"Row {count}/{count_rows}. Insert UNIQUE values.")
            count -= 1


@aiocron.crontab('*/2 * * * * ')
async def main():
    start = datetime.now()
    await logger.info(f" ======> Start parsing at {start}")
    task0 = asyncio.create_task(MainDB().create_tables())
    task1 = asyncio.create_task(GetDataStargaze().sales_parsing())
    task2 = asyncio.create_task(GetDataStargaze().burns_parsing())
    # task3 = asyncio.create_task(GetDataStargaze().mints_parsing())
    task4 = asyncio.create_task(GetDataStargaze().listings_parsing())
    await task0
    await task1
    await task2
    # await task3
    await task4
    # await asyncio.gather(task0, task1, task2, task3, task4)
    await logger.info(f" ======> Done {datetime.now() - start}")
    # await asyncio.sleep(1)
    return None


@aiocron.crontab('*/5 * * * * ')
async def main_mint_insert():
    """Вставляем в основную БД данные собранные по минту"""
    start = datetime.now()
    await logger.info(f" ======> Start MINT DATA INSERT at {start}")
    task = asyncio.create_task(GetDataStargaze().mints_parsing())
    await task
    await logger.info(f" ======> MINT DATA INSERT Done {datetime.now() - start}")



@aiocron.crontab('0-10 18,19 * * * */3')
async def mint_fast_parsing():
    """Парсим данные по минтам каждые 3 секунды
    с 21:00 до 21:10 и с 22:00 до 22:10 - в это время,
    как правило новые минты.
    В Docker время в UTC, поэтому 18 и 19"""
    create_db = asyncio.create_task(MintDB().create_table())
    await create_db
    data_mints = Parser().get_mints_data()
    async for insert_data in data_mints:
        try:
            await MintDB().insert_mints_data(insert_data)
            await logger.info("Get new MINT data. Work FAST PARSER.")
        except aiosqlite.IntegrityError:
            await logger.info("NO UNIQUE MINT data for insert. Work FAST PARSER.")
            pass


@aiocron.crontab('*/1 * * * *')
async def mint_slow_parsing():
    """Парсим данные по минтам раз в минуту,
    в любое время кроме 20:00-20:10 и 21:00-21:10,
    в это время работает парсер каждые 3 секунды"""
    condition_hours = datetime.now().hour != 18 and datetime.now().hour != 19
    condition_minute = datetime.now().minute > 10
    if condition_hours or condition_minute:
        create_db = asyncio.create_task(MintDB().create_table())
        await create_db
        data_mints = Parser().get_mints_data()
        async for insert_data in data_mints:
            try:
                await MintDB().insert_mints_data(insert_data)
                await logger.info("Get new MINT data. Work SLOW PARSER.")
            except aiosqlite.IntegrityError:
                await logger.info("NO UNIQUE MINT data for insert. Work SLOW PARSER.")
                pass


@aiocron.crontab('35 1 * * 6')
async def check_owners():
    """Запускается ночью в субботу"""
    start1 = datetime.now()
    await logger.info(f" ======> Start CHECK OWNERS parsing at {start1}")
    task0 = asyncio.create_task(MainDB().create_tables())
    task1 = asyncio.create_task(GetDataStargaze().check_owners_tokens())
    await task0
    await task1  # собираем инфу о токенах
    await logger.info(f" ======> CHECK OWNERS Done {datetime.now() - start1}")
    return None


@aiocron.crontab('35 1 * * 7')
async def insert_owners_tokens():
    """Вставляем полученные данные о держателей токенов из
    вспомогательной БД в основную.
    Запускается ночью в вс, чтобы не перегружать БД (будет блокироваться при вставке)"""
    start2 = datetime.now()
    task2 = asyncio.create_task(OwnersTokensDB().create_table())
    task3 = asyncio.create_task(GetDataStargaze().insert_owners_tokens_in_main())
    await logger.info(f" ======> Start INSERT Owners Tokens in MainDB at {start2}")
    await task2
    await task3  # собранную инфу переносим из вспомогательной БД в основную
    await logger.info(f" ======> INSERT Owners Tokens in MainDB Done {datetime.now() - start2}")


if __name__ == "__main__":
    main.start()
    check_owners.start()
    insert_owners_tokens.start()
    mint_fast_parsing.start()
    mint_slow_parsing.start()
    main_mint_insert.start()
    asyncio.get_event_loop().run_forever()
    # asyncio.run(mint_parsing())
