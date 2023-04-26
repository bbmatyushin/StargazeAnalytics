from database.db_create import MainDB


class SelectQuery(MainDB):
    def select_coll_id(self, coll_addr: str):
        """Получаем coll_id"""
        sql = """SELECT coll_id FROM collections
                    WHERE coll_addr = ?"""
        with self.connector:
            result = self.cursor.execute(sql, (coll_addr, ))
        return result.fetchone()

    def select_token_id(self, coll_id: int, token_num: int, token_name: str):
        """Получаем token_id"""
        sql = """SELECT token_id FROM tokens
                    WHERE coll_id = ? AND token_num = ?
                    AND token_name = ?"""
        with self.connector:
            result = self.cursor.execute(sql, (coll_id, token_num, token_name,))
        return result.fetchone()

    def select_owner_id(self, owner_addr: str):
        """Получаем owner_id"""
        sql = """SELECT owner_id FROM owners
                WHERE owner_addr = ?"""
        with self.connector:
            result = self.cursor.execute(sql, (owner_addr, ))
        return result.fetchone()

    def select_rarity_max(self, token_id: int, token_name: str):
        """Получаем rarity_max - максимальное количество предметов в коллекции"""
        sql = """SELECT tokens_count
                FROM collections
                JOIN tokens USING(coll_id)
                WHERE token_id = ? AND token_name = ?"""
        with self.connector:
            result = self.cursor.execute(sql, (token_id, token_name,))
        return result.fetchone()


if __name__ == "__main__":
    # s = SelectQuery().select_owner_id()
    # print(s)
    pass
