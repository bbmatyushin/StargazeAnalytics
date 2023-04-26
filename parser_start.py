import sqlite3
import logging
import aiocron
import asyncio

from database.db_insert import InsertIntoDB, MainDB
from database.db_select import SelectQuery
from parser.parsing import Parser

from datetime import datetime


class GetDataStargaze:
    def main_insert(self, coll_addr: str, token_num: str):
        """Вставляем данные в таблицы collections, tokens, rarity"""
        if coll_addr:
            coll_data = Parser().get_collection_data(coll_addr)  # получаем данные по коллекции через api
            # ==> coll_name, floor_price, tokens_count, creator_addr
            if not isinstance(coll_data, int):  # если там не статус код с ошибкой
                coll_name, floor_price, tokens_count, creator_addr = \
                    coll_data[0], coll_data[1], coll_data[2], coll_data[3]
                floor_price = int(round(floor_price, 0)) if floor_price else None
                insert_coll_data = [coll_name, coll_addr, tokens_count]
                try:  # наполняем таблицу collections
                    InsertIntoDB().insert_collections(insert_data=insert_coll_data)
                    logging.info(f'Insert new collection {coll_name}')
                except sqlite3.IntegrityError:
                    pass
                coll_id = SelectQuery().select_coll_id(coll_addr)
                coll_id = coll_id[0] if coll_id else None
                token_data = Parser().get_tokens_data(coll_addr, token_num)  # получаем данные токена
                if not isinstance(token_data, int):  # если там не статус код с ошибкой
                    token_name, rarity = token_data[0], token_data[1]
                    insert_token_data = [coll_id, token_name, int(token_num)]
                    try:  # наполняем таблицу tokens
                        InsertIntoDB().insert_tokens(insert_token_data)
                        logging.info(f"Insert new token {token_name} from {coll_name}")
                    except sqlite3.IntegrityError:
                        pass
                    token_id = SelectQuery().select_token_id(coll_id, int(token_num), token_name)
                    token_id = token_id[0] if token_id else None
                    rarity_max = SelectQuery().select_rarity_max(token_id, token_name)  # получаем самую большую редкость
                    rarity_max = rarity_max[0] if rarity_max else None
                    try:
                        # Заполняем таблицу rarity
                        InsertIntoDB().insert_rarity([token_id, rarity, rarity_max])
                        logging.info(f"Insert new rarity: {token_name} Rarity #{rarity}/{rarity_max}")
                    except sqlite3.IntegrityError:
                        pass
                    return coll_id, coll_name, token_id, floor_price
        else:
            return None, None, None, None

    def owners_inser(self, owner_addr: str):
        """Вставляем данные владельцев"""
        if owner_addr:
            owner_name = Parser().get_owner_name(owner_addr)
            insert_owner_data = [owner_addr, owner_name]
            try:
                # Заполняем ьаблицу owners
                InsertIntoDB().insert_owners(insert_owner_data)
                logging.info(f"Insert new owner {owner_addr} / {owner_name}")
            except sqlite3.IntegrityError:
                pass

    def sales_parsing(self):
        """Получаем данные по продажам.
        Заполняем таблицы collections, tokens, rarity, floors, owners, sales новыми данными"""
        logging.info("Get Sales data..")
        data_sales = Parser().get_sales_data()
        for data in data_sales:
            if data:  # Если вдруг вернется None
                block_height, coll_addr, token_num, price_stars, price_usd, seller_addr, buyer_addr, date_create = \
                    data[0], data[1], data[2], int(round(data[3], 0)), data[4], data[5], data[6], data[7]
                coll_id, coll_name, token_id, floor_price = self.main_insert(coll_addr, token_num)
                if coll_id:
                    self.owners_inser(seller_addr)
                    self.owners_inser(buyer_addr)
                    seller_id = SelectQuery().select_owner_id(seller_addr)
                    seller_id = seller_id[0] if seller_id else None
                    buyer_id = SelectQuery().select_owner_id(buyer_addr)
                    buyer_id = buyer_id[0] if buyer_id else None
                    insert_sales_data = [block_height, coll_id, token_id, seller_id, buyer_id,
                                         price_stars, price_usd, date_create]
                    try:
                        # Заполняем таблицу sales
                        InsertIntoDB().insert_sales(insert_sales_data)
                        logging.info(f"Insert new sale 💵 | {block_height} | {coll_name} "
                                     f"{token_num} {price_stars} STARS create at {date_create}")
                        # Заполняем таблицу floors
                        InsertIntoDB().insert_floors([coll_id, floor_price])
                        logging.info(f"Insert floor price {floor_price} STARS {coll_name}")
                    except sqlite3.IntegrityError:
                        pass

    def burns_parsing(self):
        """Получаем данные по сжиганиям токенов.
        Заполняем таблицы collections, tokens, rarity, owners, burns новыми данными"""
        logging.info("Get Burns data..")
        data_burns = Parser().get_burn_data()
        # block_height, coll_addr, token_num, sender_addr, date_create
        for data in data_burns:
            if data:
                block_height, coll_addr, token_num, sender_addr, date_create = \
                    data[0], data[1], data[2], data[3], data[4]
                coll_id, coll_name, token_id, floor_price = self.main_insert(coll_addr, token_num)
                if coll_id:
                    self.owners_inser(sender_addr)
                    sender_id = SelectQuery().select_owner_id(sender_addr)
                    sender_id = sender_id[0] if sender_id else None
                    insert_burns_data = [block_height, coll_id, token_id, sender_id, date_create]
                    try:
                        InsertIntoDB().insert_burns(insert_burns_data)
                        logging.info(f"Insert new burn 🔥 | {block_height} | {coll_name} {token_num} "
                                     f"create at {date_create}")
                    except sqlite3.IntegrityError:
                        pass

    def mints_parsing(self):
        """Получаем данные по минтам.
        Заполняем таблицы collections, tokens, rarity, owners, mints новыми данными"""
        logging.info("Get Mints data..")
        data_mints = Parser().get_mints_data()
        for data in data_mints:
            if data:
                block_height, coll_addr, token_num, recipient_addr, creator_addr, \
                    price_stars, price_usd, date_create = data[0], data[1], data[2], \
                    data[3], data[4], data[5], data[6], data[7]
                coll_id, coll_name, token_id, floor_price = self.main_insert(coll_addr, token_num)
                self.owners_inser(recipient_addr)
                self.owners_inser(creator_addr)
                recipient_id = SelectQuery().select_owner_id(recipient_addr)
                recipient_id = recipient_id[0] if recipient_id else None
                creator_id = SelectQuery().select_owner_id(creator_addr)
                creator_id = creator_id[0] if creator_id else None
                insert_mints_data = [block_height, coll_id, token_id, recipient_id,
                                     creator_id, price_stars, price_usd, date_create]
                try:
                    InsertIntoDB().insert_mints(insert_mints_data)
                    logging.info(f"Insert new mint 🍃 | {block_height} | {coll_name} {token_num}  "
                                 f"{price_stars} STARS create at {date_create}")
                except sqlite3.IntegrityError:
                    pass

    def run(self):
        MainDB().create_tables()
        self.sales_parsing()
        self.burns_parsing()
        self.mints_parsing()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    try:
        @aiocron.crontab('*/2 * * * * 30')
        def main():
            start = datetime.now()
            logging.info(f"Start parsing at {start}")
            GetDataStargaze().run()
            logging.info(f" ======> Done {datetime.now() - start}")
    except Exception as err:
        logging.error(err, exc_info=True)
    asyncio.get_event_loop().run_forever()
