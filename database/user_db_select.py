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

    # TODO: ##########  ADDRS MONITORING  ############
    async def select_addrs_monitoring(self):
        """Все адреса для мониторинга"""
        async with self.connector as conn:
            sql = """SELECT DISTINCT addr_monitor FROM addrs_monitor
                    WHERE monitor_flag = 1"""
            async with conn.execute(sql) as cursor:
                result = await cursor.fetchall()
                return [el[0] for el in result]

    async def select_users_monitoring(self, addr_monitor):
        """Пользователи которые следят за выбранным адресом"""
        async with self.connector as conn:
            sql = """SELECT user_id FROM addrs_monitor
                    WHERE addr_monitor = ?"""
            async with conn.execute(sql, (addr_monitor,)) as cursor:
                result = await cursor.fetchall()
                return [el[0] for el in result]

    async def select_count_addrs_monitor(self, user_id):
        """Кол-во адресов за которыми следит пользователь (МАКС. - 5шт.)"""
        async with self.connector as conn:
            sql = """SELECT COUNT(addr_monitor) FROM addrs_monitor
                    WHERE user_id = ?"""
            async with conn.execute(sql, (user_id,)) as cursor:
                result = await cursor.fetchone()
                return result[0] if result else None

    async def select_monitoring_info(self, addr_monitor):
        """ИНФА для сообщения о свершивсейся сделки"""
        async with self.connector as conn:
            sql = """SELECT addr_monitor, coll_addr, coll_name, token_name, token_num,
                        buyer_addr, buyer_name, price_stars, price_usd, DATETIME(date_create) AS dt
                    FROM sales_monitoring
                    WHERE addr_monitor = ?
                    ORDER BY date_create DESC
                    LIMIT 1"""
            async with conn.execute(sql, (addr_monitor,)) as cursor:
                result = await cursor.fetchone()
                return result if result else None

    async def select_last_monitor_id(self):
        async with self.connector as conn:
            sql = """SELECT monitor_id FROM sales_monitoring
                    ORDER BY date_add DESC LIMIT 1"""
            async with conn.execute(sql) as cursor:
                result = await cursor.fetchone()
                return result[0] if result else None


if __name__ == "__main__":
    result = asyncio.run(UserDBSelect().select_last_monitor_id())
    print(result)
