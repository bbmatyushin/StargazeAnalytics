# import logging
import asyncio

from .db_create import MainDB
# from db_create import MainDB


class InsertIntoDB(MainDB):
    async def insert_collections(self, insert_data):
        """Заполняем таблицу collections
        insert_data = [coll_name, coll_addr, tokens_count]"""
        sql = """INSERT INTO collections(
                    coll_name, coll_addr, tokens_count, date_add)
                VALUES(?, ?, ?, DATETIME())"""
        async with self.connector as conn:
            # logging.info(f"{conn}")
            await conn.execute(sql, insert_data)
            await conn.commit()

    async def insert_tokens(self, insert_data):
        """Заполняем таблицу tokens
        insert_data = [coll_id, token_name, token_num]"""
        sql = """INSERT INTO tokens(
                    coll_id, token_name, token_num, date_add)
                VALUES(?, ?, ?, DATETIME())"""
        async with self.connector as conn:
            await conn.execute(sql, insert_data)
            await conn.commit()

    async def insert_owners(self, insert_data):
        """Заполняем таблицу owners
        insert_data = [owner_addr, owner_name]"""
        sql = """INSERT INTO owners(owner_addr, owner_name, date_add)
                VALUES(?, ?, DATETIME())"""
        async with self.connector as conn:
            await conn.execute(sql, insert_data)
            await conn.commit()

    async def insert_owners_tokens(self, insert_data):
        """Заполняем таблицу owners_tokens кто какими токенами владеет
        insert_data = [owner_id, coll_id, token_id, for_sale, date_create]"""
        async with self.connector as conn:
            sql = """INSERT INTO owners_tokens(
                        owner_id, coll_id, token_id, for_sale, date_create, date_add)
                    VALUES(?, ?, ?, ?, ?, DATETIME())"""
            await conn.execute(sql, insert_data)
            await conn.commit()

    async def insert_floors(self, insert_data):
        """Заполняем таблицу floors
        insert_data = [coll_id, floor_price]"""
        sql = """INSERT INTO floors(coll_id, floor_price, date_add)
                VALUES(?, ?, STRFTIME('%Y-%m-%d %H:00:00', DATETIME()))"""
        async with self.connector as conn:
            await conn.execute(sql, insert_data)
            await conn.commit()

    async def insert_rarity(self, insert_data):
        """Заполняем таблицу rarity
        insert_data = [token_id, rarity, rarity_max]"""
        sql = """INSERT INTO rarity(token_id, rarity, rarity_max, date_add)
                VALUES(?, ?, ?, DATETIME())"""
        async with self.connector as conn:
            await conn.execute(sql, insert_data)
            await conn.commit()

    async def insert_sales(self, insert_data):
        """Заполняем таблицу sales
        insert_data = [block_height, coll_id, token_id, seller_id,
                        buyer_id, price_stars, price_usd, date_create]"""
        sql = """INSERT INTO sales(block_height, coll_id, token_id,
                    seller_id, buyer_id, price_stars, price_usd, date_create, date_add)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, DATETIME())"""
        async with self.connector as conn:
            await conn.execute(sql, insert_data)
            await conn.commit()

    async def insert_burns(self, insert_data):
        """Заполняем таблицу burns
        insert_data = [block_height, coll_id, token_id, sender_id, date_create]"""
        sql = """INSERT INTO burns(block_height, coll_id, token_id, sender_id, date_create, date_add)
                VALUES(?, ?, ?, ?, ?, DATETIME())"""
        async with self.connector as conn:
            await conn.execute(sql, insert_data)
            await conn.commit()

    async def insert_mints(self, insert_data):
        """Заполняем таблицу mints
        insert_data = [block_height, coll_id, token_id, recipient_id,
                        creator_id, price_stars, price_usd, date_create]"""
        sql = """INSERT INTO mints(block_height, coll_id, token_id, 
                    recipient_id, creator_id, price_stars, price_usd, date_create, date_add)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, DATETIME())"""
        async with self.connector as conn:
            await conn.execute(sql, insert_data)
            await conn.commit()

    async def insert_listings(self, insert_data):
        """Заполняем таблицу listings
        insert_data = [block_height, coll_id, token_id, seller_id,
                        price_stars, price_usd, event_name, date_create]"""
        sql = """INSERT INTO listings(block_height, coll_id, token_id, seller_id, 
                    price_stars, price_usd, event_name, date_create, date_add)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, DATETIME())"""
        async with self.connector as conn:
            await conn.execute(sql, insert_data)
            await conn.commit()


if __name__ == "__main__":
    data = ['Cosmos Ape Alliance', 'stars19y092mzr4szme4jfd5psnkkkvfayh25a6q5t2y0xshcpay9x6hhsgpqqca', 1111]
    asyncio.run(InsertIntoDB().insert_collections(data))
