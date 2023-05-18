"""Отдельная БД для сохранения новых минтов,
т.к. нужно будет чаще обращаться за получением новой инфы о минтах"""

import aiosqlite
import asyncio
import os

from pathlib import Path


class MintDB:
    def __init__(self):
        self.db_dir = os.path.dirname(os.path.realpath(__file__))
        self.connector = aiosqlite.connect(f"{Path(self.db_dir, '_db', 'mints.db')}")

    async def create_table(self):
        async with self.connector as conn:
            sql = """CREATE TABLE IF NOT EXISTS mints_data(
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        block_height INTEGER,
                        coll_addr TEXT,
                        token_num INTEGER,
                        recipient_addr TEXT,
                        creator_addr TEXT,
                        price_stars INTEGER,
                        price_usd REAL,
                        date_create TEXT,
                        date_add TEXT,
                        UNIQUE(block_height, coll_addr, token_num)
                        )"""
            await conn.execute(sql)
            await conn.commit()

    async def insert_mints_data(self, insert_data):
        async with self.connector as conn:
            sql = """INSERT INTO mints_data(block_height, coll_addr, token_num,
                        recipient_addr, creator_addr, price_stars, price_usd,
                        date_create, date_add)
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?, DATETIME())"""
            await conn.execute(sql, insert_data)
            await conn.commit()

    async def select_mints_data(self):
        async with self.connector as conn:
            sql = """SELECT block_height,
                        coll_addr, token_num,
                        recipient_addr, creator_addr,
                        price_stars, price_usd,
                        date_create
                    FROM mints_data
                    WHERE DATETIME(date_add) >= DATETIME(DATETIME(), '-20 minutes') 
                    ORDER BY date_create DESC"""
            async with conn.execute(sql) as cursor:
                result = await cursor.fetchall()
                if result:
                    for res in result:
                        yield res
                    # return result


async def main():
    task1 = asyncio.create_task(MintDB().create_table())
    await task1
    async for res in MintDB().select_mints_data():
        print(res)


if __name__ == "__main__":
    asyncio.run(main())
