import asyncio

from database.user_db_create import UserDB


class UserDBSelect(UserDB):
    #TODO: ##########  USERS  #############
    async def select_active_users(self):
        """Все активные пользователи"""
        async with self.connector as conn:
            sql = """SELECT user_id FROM users
                    WHERE user_id NOT IN (
                            SELECT user_id FROM inactive_users
                            WHERE inactive_flag = 1
                            )"""
            async with conn.execute(sql) as cursor:
                result = await cursor.fetchall()
            return [el[0] for el in result]

    async def select_subscribe_users(self):
        """Все активные пользователи,
        которые подписанны на рассылку отчетов"""
        async with self.connector as conn:
            sql = """SELECT u.user_id FROM users u
                    JOIN users_subscribe us ON u.user_id = us.user_id
                        AND us.subscribe_flag = 1
                    WHERE u.user_id NOT IN (
                        SELECT user_id FROM inactive_users
                        WHERE inactive_flag = 1
                        )"""
            async with conn.execute(sql) as cursor:
                result = await cursor.fetchall()
            return [el[0] for el in result]

    async def select_admins(self):
        """Выборка админов бота"""
        async with self.connector as conn:
            sql = """SELECT user_id FROM admins
                    WHERE user_id NOT IN (
                            SELECT user_id FROM inactive_users
                            WHERE inactive_flag = 1
                            )"""
            async with conn.execute(sql) as cursor:
                result = await cursor.fetchall()
            return [el[0] for el in result]

    async def select_admins_subscribe(self):
        """Выборка админов бота с подпиской"""
        async with self.connector as conn:
            sql = """SELECT admins.user_id
                    FROM admins
                    LEFT JOIN inactive_users USING(user_id)
                    LEFT JOIN users_subscribe USING(user_id)
                    WHERE inactive_flag IS NULL
                        AND subscribe_flag = 1"""
            async with conn.execute(sql) as cursor:
                result = await cursor.fetchall()
            return [el[0] for el in result]

    #TODO: ##########  REPORTS  ############
    async def select_daily_report_id(self):
        """Забираем последний номер отправленного ежеДНЕВНОГО отчета"""
        async with self.connector as conn:
            sql = "SELECT MAX(report_id) FROM daily_reports"
            async with conn.execute(sql) as cursor:
                result = await cursor.fetchone()
            return result[0]

    async def select_weekly_report_id(self):
        """Забираем последний номер отправленного ежеНЕДЕЛЬНОГО отчета"""
        async with self.connector as conn:
            sql = "SELECT MAX(report_id) FROM weekly_report"
            async with conn.execute(sql) as cursor:
                result = await cursor.fetchone()
            return result[0]

    async def select_monthly_report_id(self):
        """Забираем последний номер отправленного ежеМЕСЯЧНОГО отчета"""
        async with self.connector as conn:
            sql = "SELECT MAX(report_id) FROM monthly_report"
            async with conn.execute(sql) as cursor:
                result = await cursor.fetchone()
            return result[0]


if __name__ == "__main__":
    result = asyncio.run(UserDBSelect().select_admins_subscribe())
    print(result)
