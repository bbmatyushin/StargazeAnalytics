"""Создается отдельная БД, т.к. парсинг токенов занимает много времени,
и при этом блокируется основная БД, а значит нельзя будет встаивть
значени по сделкам."""
import aiosqlite
import os
from pathlib import Path


class OwnersTokensDB:
    def __init__(self):
        self.db_dir = os.path.dirname(os.path.realpath(__file__))
        self.connector = aiosqlite.connect(f"{Path(self.db_dir, 'owners_tokens.db')}")
        # self.create_table()

    async def create_table(self):
        async with self.connector as conn:
            sql = """CREATE TABLE IF NOT EXISTS owners_tokens_single(
                        owner_id INTEGER,
                        coll_id INTEGER,
                        token_id INTEGER,
                        for_sale TEXT,
                        date_create TEXT,
                        date_add TEXT,
                        UNIQUE(owner_id, coll_id, token_id)
                        );"""
            await conn.execute(sql)
            await conn.commit()

    async def insert_owners_tokens_single(self, insert_data):
        """Заполняем таблицу owners_tokens кто какими токенами владеет
        insert_data = [owner_id, coll_id, token_id, for_sale, date_create]"""
        async with self.connector as conn:
            sql = """INSERT INTO owners_tokens_single(
                        owner_id, coll_id, token_id, for_sale, date_create, date_add)
                    VALUES(?, ?, ?, ?, ?, DATETIME())"""
            await conn.execute(sql, insert_data)
            await conn.commit()

    async def select_owners_tokens_single(self):
        """Выводим все значения для их вставки в основную БД"""
        async with self.connector as conn:
            sql = """SELECT owner_id, coll_id, token_id, for_sale, date_create
                    FROM owners_tokens_single"""
            async with conn.execute(sql) as cursor:
                result = await cursor.fetchall()
                for res in result:
                    yield res
