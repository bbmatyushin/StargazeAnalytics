import asyncio

from database.user_db_create import UserDB


class UserDBSelect(UserDB):
    async def select_active_users(self):
        """Все активные пользователи"""
        async with self.connector as conn:
            sql = """SELECT user_id FROM users
                    WHERE user_id NOT IN (
                            SELECT user_id FROM users_block_bot
                            WHERE block_flag = 1
                            )"""
            async with conn.execute(sql) as cursor:
                result = await cursor.fetchall()
            return [el[0] for el in result]

    async def select_subscribe_users(self):
        """Все активные пользователи,
        которые подписанны на рассфлку отчетов"""
        async with self.connector as conn:
            sql = """SELECT u.user_id FROM users u
                    JOIN users_subscribe us ON u.user_id = us.user_id
                        AND us.subscribe_flag = 1
                    WHERE u.user_id NOT IN (
                        SELECT user_id FROM users_block_bot
                        WHERE block_flag = 1
                        )"""
            async with conn.execute(sql) as cursor:
                result = await cursor.fetchall()
            return [el[0] for el in result]

    async def select_admins(self):
        """Выборка админов бота"""
        async with self.connector as conn:
            sql = """SELECT user_id FROM admins
                    WHERE user_id NOT IN (
                            SELECT user_id FROM users_block_bot
                            WHERE block_flag = 1
                            )"""
            async with conn.execute(sql) as cursor:
                result = await cursor.fetchall()
            return [el[0] for el in result]


if __name__ == "__main__":
    result = asyncio.run(UserDBSelect().select_subscribe_users())
    print(result)
