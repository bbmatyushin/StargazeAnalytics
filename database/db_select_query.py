#TODO: Дубль для тернировок
import asyncio

from database.db_create import MainDB


class SelectQuery(MainDB):
    async def select_coll_id(self, coll_addr: str):
        """Получаем coll_id"""
        sql = """SELECT coll_id FROM collections
                    WHERE coll_addr = ?"""
        async with self.connector as conn:
            async with conn.execute(sql, (coll_addr, )) as cursor:
                result = await cursor.fetchone()
        return result

    async def select_token_id(self, coll_id: int, token_num: int, token_name: str):
        """Получаем token_id"""
        sql = """SELECT token_id FROM tokens
                    WHERE coll_id = ? AND token_num = ?
                    AND token_name = ?"""
        async with self.connector as conn:
            async with conn.execute(sql, (coll_id, token_num, token_name,)) as cursor:
                result = await cursor.fetchone()
        return result

    async def select_owner_id(self, owner_addr: str):
        """Получаем owner_id"""
        sql = """SELECT owner_id FROM owners
                WHERE owner_addr = ?"""
        async with self.connector as conn:
            async with conn.execute(sql, (owner_addr, )) as cursor:
                result = await cursor.fetchone()
        return result

    async def select_rarity_max(self, token_id: int, token_name: str):
        """Получаем rarity_max - максимальное количество предметов в коллекции"""
        sql = """SELECT tokens_count
                FROM collections
                JOIN tokens USING(coll_id)
                WHERE token_id = ? AND token_name = ?"""
        async with self.connector as conn:
            async with conn.execute(sql, (token_id, token_name,)) as cursor:
                result = await cursor.fetchone()
        return result


async def _main():
    owner_addr = 'stars1f87q82cnmzsf3h7pqg0lllrvjp0zhfaceh3yk6'
    task1 = asyncio.create_task(SelectQuery().select_owner_id(owner_addr))
    await asyncio.gather(task1)

if __name__ == "__main__":
    # s = SelectQuery().select_owner_id()
    # print(s)
    res = asyncio.run(SelectQuery().select_owner_id(''))
    print(res)
