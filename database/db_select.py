import asyncio
from database.db_create import MainDB


class SelectQuery(MainDB):
    async def select_coll_id(self, coll_addr: str):
        """Получаем coll_id"""
        async with self.connector as conn:
            sql = """SELECT coll_id FROM collections
                    WHERE coll_addr = ?"""
            async with conn.execute(sql, (coll_addr, )) as cursor:
                result = await cursor.fetchone()
                return result[0] if result else None

    async def select_token_id(self, coll_id: int, token_num: int):
        """Получаем token_id"""
        async with self.connector as conn:
            sql = """SELECT token_id FROM tokens
                        WHERE coll_id = ? 
                            AND token_num = ?"""
            async with conn.execute(sql, (coll_id, token_num,)) as cursor:
                result = await cursor.fetchone()
                return result[0] if result else None

    async def select_rarity_max(self, token_id: int):
        """Получаем rarity_max - максимальное количество предметов в коллекции"""
        async with self.connector as conn:
            sql = """SELECT tokens_count
                    FROM collections
                    JOIN tokens USING(coll_id)
                    WHERE token_id = ?"""
            async with conn.execute(sql, (token_id,)) as cursor:
                result = await cursor.fetchone()
                return result[0] if result else None

    # TODO: ###########  OWNERS  ################

    async def select_owner_id(self, owner_addr: str):
        """Получаем owner_id"""
        async with self.connector as conn:
            sql = """SELECT owner_id FROM owners
                    WHERE owner_addr = ?"""
            async with conn.execute(sql, (owner_addr, )) as cursor:
                result = await cursor.fetchone()
                return result[0] if result else None

    async def select_count_owners(self):
        """Получаем количество всех кошельков в базе"""
        async with self.connector as conn:
            # так быстрее отрабатывает
            sql = """SELECT owner_id FROM owners
                    ORDER BY owner_id DESC LIMIT 1"""
            async with conn.execute(sql) as cursor:
                result = await cursor.fetchone()
                return result[0] if result else None

    async def select_owner_addr(self):
        """Получаем owner_addr"""
        async with self.connector as conn:
            sql = """SELECT owner_addr FROM owners"""
            async with conn.execute(sql) as cursor:
                result = await cursor.fetchall()
                for res in result:
                    yield res[0]
                # return result

    async def select_owner_addr_name(self):
        """Получаем owner_addr и owner_name для переноса в БД Users"""
        async with self.connector as conn:
            sql = """SELECT owner_addr, owner_name FROM owners"""
            async with conn.execute(sql) as cursor:
                result = await cursor.fetchall()
                for res in result:
                    yield res
                # return result

    #TODO: ###########  MONITORING SELECT  ################
    async def select_for_sales_monitoring(self, owner_addr: str):
        """Вытаскиваем данные для таблицы sales_monitoring в Users DB.
        (данные берутся за последние 20 минут, чтобы не перегружать выдачу)"""
        async with self.connector as conn:
            sql = """SELECT o.owner_addr AS addr_monitor,
                        coll_addr, coll_name,
                        token_name, token_num,
                        ob.owner_addr AS buyer_addr,
                        ob.owner_name AS buyer_name,
                        price_stars, price_usd,
                        s.date_create
                    FROM sales s
                    JOIN owners o ON o.owner_id = s.seller_id
                        AND o.owner_addr = ?
                    JOIN owners ob ON ob.owner_id = s.buyer_id
                    JOIN collections USING(coll_id)
                    JOIN tokens USING(token_id)
                    WHERE DATETIME(s.date_add) >= DATETIME(DATETIME(), '-20 minute')"""
            async with conn.execute(sql, (owner_addr,)) as cursor:
                result = await cursor.fetchall()
                for res in result:
                    yield res
                # return result

    async def select_for_buys_monitoring(self, owner_addr: str):
        """Вытаскиваем данные для таблицы buys_monitoring в Users DB.
        (данные берутся за последние 20 минут, чтобы не перегружать выдачу)"""
        async with self.connector as conn:
            sql = """SELECT o.owner_addr AS addr_monitor,
                        coll_addr, coll_name,
                        token_name, token_num,
                        ob.owner_addr AS seller_addr,
                        ob.owner_name AS seller_name,
                        price_stars, price_usd,
                        s.date_create
                    FROM sales s
                    JOIN owners o ON o.owner_id = s.buyer_id
                        AND o.owner_addr = ?
                    JOIN owners ob ON ob.owner_id = s.seller_id
                    JOIN collections USING(coll_id)
                    JOIN tokens USING(token_id)
                    WHERE DATETIME(s.date_add) >= DATETIME(DATETIME(), '-20 minute')"""
            async with conn.execute(sql, (owner_addr,)) as cursor:
                result = await cursor.fetchall()
                for res in result:
                    yield res
                # return result

    #TODO:  ##############  OWNERS TOKENS  ################
    async def select_owners_tokens_single(self):
        """Выводим все значения для их вставки в основную БД"""
        async with self.connector as conn:
            sql = """SELECT owner_id, coll_id, token_id, for_sale, date_create
                    FROM owners_tokens"""
            async with conn.execute(sql) as cursor:
                result = await cursor.fetchall()
                for res in result:
                    yield res

async def main():
    count = await SelectQuery().select_count_owners()
    return count


if __name__ == "__main__":
    c = asyncio.run(main())
    print(c)
