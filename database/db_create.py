import aiosqlite
import asyncio
import os
from pathlib import Path


class MainDB:
    def __init__(self):
        self.db_dir = os.path.dirname(os.path.realpath(__file__))
        self.connector = aiosqlite.connect(f"{Path(self.db_dir, 'stargaze_analytics.db')}")
        # self.cursor = self.connector.cursor()

    async def create_tables(self):
        async with self.connector as conn:
            sql = """CREATE TABLE IF NOT EXISTS collections(
                        coll_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        coll_name TEXT,
                        coll_addr TEXT UNIQUE,
                        tokens_count INTEGER,
                        date_add DATETIME);
                    CREATE TABLE IF NOT EXISTS tokens(
                        token_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        coll_id INTEGER,
                        token_name TEXT,
                        token_num INTEGER,
                        date_add DATETIME,
                        UNIQUE(coll_id, token_num)
                        FOREIGN KEY (coll_id) REFERENCES collections(coll_id)
                        );
                    CREATE TABLE IF NOT EXISTS owners(
                        owner_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        owner_addr TEXT UNIQUE,
                        owner_name TEXT,
                        date_add DATETIME
                        );
                    CREATE TABLE IF NOT EXISTS floors(
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        coll_id INTEGER,
                        floor_price INTEGER,
                        date_add DATETIME,
                        UNIQUE(coll_id, floor_price, date_add),
                        FOREIGN KEY (coll_id) REFERENCES collections(coll_id)
                        );
                    CREATE TABLE IF NOT EXISTS rarity(
                        token_id INTEGER UNIQUE,
                        rarity INTEGER,
                        rarity_max INTEGER,
                        date_add DATETIME,
                        FOREIGN KEY (token_id) REFERENCES tokens(token_id)
                        );
                    CREATE TABLE IF NOT EXISTS sales(
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        block_height INTEGER,
                        coll_id INTEGER,
                        token_id INTEGER,
                        seller_id INTEGER,
                        buyer_id INTEGER,
                        price_stars INTEGER,
                        price_usd REAL,
                        date_create DATETIME,
                        date_add DATETIME,
                        UNIQUE(block_height, token_id, seller_id, buyer_id),
                        FOREIGN KEY (coll_id) REFERENCES collections(coll_id),
                        FOREIGN KEY (token_id) REFERENCES tokens(token_id),
                        FOREIGN KEY (seller_id) REFERENCES owners(owner_id),
                        FOREIGN KEY (buyer_id) REFERENCES owners(owner_id)
                        );
                    CREATE TABLE IF NOT EXISTS burns(
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        block_height INTEGER UNIQUE,
                        coll_id INTEGER,
                        token_id INTEGER,
                        sender_id INTEGER,
                        date_create DATETIME,
                        date_add DATETIME,
                        UNIQUE(block_height, token_id),
                        FOREIGN KEY (coll_id) REFERENCES collections(coll_id),
                        FOREIGN KEY (token_id) REFERENCES tokens(token_id),
                        FOREIGN KEY (sender_id) REFERENCES owners(owner_id)
                        );
                    CREATE TABLE IF NOT EXISTS mints(
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        block_height INTEGER,
                        coll_id INTEGER,
                        token_id INTEGER,
                        recipient_id INTEGER,
                        creator_id INTEGER,
                        price_stars INTEGER,
                        price_usd REAL,
                        date_create DATETIME,
                        date_add DATETIME,
                        UNIQUE(block_height, token_id),
                        FOREIGN KEY (coll_id) REFERENCES collections(coll_id),
                        FOREIGN KEY (token_id) REFERENCES tokens(token_id),
                        FOREIGN KEY (recipient_id) REFERENCES owners(owner_id),
                        FOREIGN KEY (creator_id) REFERENCES owners(owner_id)
                        );
                    CREATE TABLE IF NOT EXISTS listings(
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        block_height INTEGER,
                        coll_id INTEGER,
                        token_id INTEGER,
                        seller_id INTEGER,
                        price_stars INTEGER,
                        price_usd REAL,
                        event_name TEXT,
                        date_create DATETIME,
                        date_add DATETIME,
                        UNIQUE(block_height, date_create, token_id),
                        FOREIGN KEY (coll_id) REFERENCES collections(coll_id),
                        FOREIGN KEY (token_id) REFERENCES tokens(token_id),
                        FOREIGN KEY (seller_id) REFERENCES owners(owner_id)
                        );
                    CREATE TABLE IF NOT EXISTS owners_tokens(
                        owner_id INTEGER,
                        coll_id INTEGER,
                        token_id INTEGER,
                        for_sale TEXT,
                        date_create TEXT,
                        date_add TEXT,
                        UNIQUE(owner_id, coll_id, token_id)
                        FOREIGN KEY (owner_id) REFERENCES owners(owner_id)
                        ON DELETE CASCADE,
                        FOREIGN KEY (coll_id) REFERENCES collections(coll_id)
                        ON DELETE CASCADE,
                        FOREIGN KEY (token_id) REFERENCES tokens(token_id)
                        ON DELETE CASCADE
                        );
                        """
            await conn.executescript(sql)
            await conn.commit()


if __name__ == "__main__":
    asyncio.run(MainDB().create_tables())
