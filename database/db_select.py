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

    async def select_token_id(self, coll_id: int, token_num: int, token_name: str):
        """Получаем token_id"""
        async with self.connector as conn:
            sql = """SELECT token_id FROM tokens
                        WHERE coll_id = ? AND token_num = ?
                        AND token_name = ?"""
            async with conn.execute(sql, (coll_id, token_num, token_name,)) as cursor:
                result = await cursor.fetchone()
                return result if result else None

    async def select_owner_id(self, owner_addr: str):
        """Получаем owner_id"""
        async with self.connector as conn:
            sql = """SELECT owner_id FROM owners
                    WHERE owner_addr = ?"""
            async with conn.execute(sql, (owner_addr, )) as cursor:
                result = await cursor.fetchone()
                return result if result else None

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


if __name__ == "__main__":
    owner_addr = 'stars1654yth3nm628ej2x4tm6farrf0h7wju7c3cyp6'
    s = asyncio.run(SelectQuery().select_for_sales_monitoring(owner_addr))
    for r in s:
        print(r)
    pass
