import sqlite3
import os
from pathlib import Path


class MainDB:
    def __init__(self):
        self.connector = sqlite3.connect(f"{Path(os.path.dirname(os.path.realpath(__file__)), 'stargaze_analytics.db')}")
        self.cursor = self.connector.cursor()

    def create_tables(self):
        with self.connector:
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
                        floor_price REAL,
                        date_add DATETIME,
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
                        price_stars REAL,
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
                        price_stars REAL,
                        price_usd REAL,
                        date_create DATETIME,
                        date_add DATETIME,
                        UNIQUE(block_height, token_id),
                        FOREIGN KEY (coll_id) REFERENCES collections(coll_id),
                        FOREIGN KEY (token_id) REFERENCES tokens(token_id),
                        FOREIGN KEY (recipient_id) REFERENCES owners(owner_id),
                        FOREIGN KEY (creator_id) REFERENCES owners(owner_id)
                        );
                        """
            with self.connector:
                self.cursor.executescript(sql)
                self.connector.commit()


if __name__ == "__main__":
    MainDB().create_tables()
