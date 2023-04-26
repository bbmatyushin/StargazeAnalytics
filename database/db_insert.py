import sqlite3

from database.db_create import MainDB


class InsertIntoDB(MainDB):

    def insert_collections(self, insert_data):
        """Заполняем таблицу collections
        insert_data = [coll_name, coll_addr, tokens_count]"""
        sql = """INSERT INTO collections(
                    coll_name, coll_addr, tokens_count, date_add)
                VALUES(?, ?, ?, DATETIME())"""
        with self.connector:
            self.cursor.execute(sql, insert_data)
            self.connector.commit()

    def insert_tokens(self, insert_data):
        """Заполняем таблицу tokens
        insert_data = [coll_id, token_name, token_num]"""
        sql = """INSERT INTO tokens(
                    coll_id, token_name, token_num, date_add)
                VALUES(?, ?, ?, DATETIME())"""
        with self.connector:
            self.cursor.execute(sql, insert_data)
            self.connector.commit()

    def insert_owners(self, insert_data):
        """Заполняем таблицу owners
        insert_data = [owner_addr, owner_name]"""
        sql = """INSERT INTO owners(owner_addr, owner_name, date_add)
                VALUES(?, ?, DATETIME())"""
        with self.connector:
            self.cursor.execute(sql, insert_data)
            self.connector.commit()

    def insert_floors(self, insert_data):
        """Заполняем таблицу floors
        insert_data = [coll_id, floor_price]"""
        sql = """INSERT INTO floors(coll_id, floor_price, date_add)
                VALUES(?, ?, DATETIME())"""
        with self.connector:
            self.cursor.execute(sql, insert_data)
            self.connector.commit()

    def insert_rarity(self, insert_data):
        """Заполняем таблицу rarity
        insert_data = [token_id, rarity, rarity_max]"""
        sql = """INSERT INTO rarity(token_id, rarity, rarity_max, date_add)
                VALUES(?, ?, ?, DATETIME())"""
        with self.connector:
            self.cursor.execute(sql, insert_data)
            self.connector.commit()

    def insert_sales(self, insert_data):
        """Заполняем таблицу sales
        insert_data = [block_height, coll_id, token_id, seller_id,
                        buyer_id, price_stars, price_usd, date_create]"""
        sql = """INSERT INTO sales(block_height, coll_id, token_id,
                    seller_id, buyer_id, price_stars, price_usd, date_create, date_add)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, DATETIME())"""
        with self.connector:
            self.cursor.execute(sql, insert_data)
            self.connector.commit()

    def insert_burns(self, insert_data):
        """Заполняем таблицу burns
        insert_data = [block_height, coll_id, token_id, sender_id, date_create]"""
        sql = """INSERT INTO burns(block_height, coll_id, token_id, sender_id, date_create, date_add)
                VALUES(?, ?, ?, ?, ?, DATETIME())"""
        with self.connector:
            self.cursor.execute(sql, insert_data)
            self.connector.commit()

    def insert_mints(self, insert_data):
        """Заполняем таблицу mints
        insert_data = [block_height, coll_id, token_id, recipient_id,
                        creator_id, price_stars, price_usd, date_create]"""
        sql = """INSERT INTO mints(block_height, coll_id, token_id, 
                    recipient_id, creator_id, price_stars, price_usd, date_create, date_add)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, DATETIME())"""
        with self.connector:
            self.cursor.execute(sql, insert_data)
            self.connector.commit()
