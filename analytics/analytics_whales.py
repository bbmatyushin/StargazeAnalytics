import asyncio
from database.db_create import MainDB


class AnalyticsWhales(MainDB):
    """Аналитика китов stargaze"""
    async def top5_whales(self):
        async with self.connector as conn:
            sql = """SELECT owner_addr, owner_name, COUNT(token_id)
                    FROM owners_tokens
                        JOIN owners USING(owner_id)
                    GROUP BY owner_addr, owner_name
                    ORDER BY 3 DESC
                    LIMIT 5"""
            async with conn.execute(sql) as cursor:
                result = await cursor.fetchall()
                for res in result:
                    yield res
                # return result


if __name__ == "__main__":
    pass
