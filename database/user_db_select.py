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

    async def select_user_addrs_monitoring(self, user_id):
        """Aдреса для мониторинга отдельного пользователя"""
        async with self.connector as conn:
            sql = """SELECT owner_addr, owner_name
                    FROM addrs_monitor a
                     JOIN owners o ON o.owner_addr = a.addr_monitor
                    WHERE user_id = ?
                        AND monitor_flag = 1
                    ORDER BY a.date_add"""
            async with conn.execute(sql, (user_id,)) as cursor:
                result = await cursor.fetchall()
                return [el for el in result]

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
                    WHERE user_id = ?
                        AND monitor_flag = 1"""
            async with conn.execute(sql, (user_id,)) as cursor:
                result = await cursor.fetchone()
                return result[0] if result else None

    async def select_sales_monitoring_info(self, monitor_id: int, addr_monitor: str):
        """ИНФА для сообщения о свершивсейся сделки - ПРОДАЖИ"""
        # monitor_ids.append(addr_monitor)
        # data = monitor_ids
        async with self.connector as conn:
            sql = f"""SELECT owner_addr, owner_name, coll_addr, coll_name, token_name, token_num,
                        buyer_addr, buyer_name, price_stars, price_usd, DATETIME(date_create) AS dt
                    FROM sales_monitoring sm
                        JOIN owners o ON sm.addr_monitor = o.owner_addr
                    WHERE monitor_id = ?
                        AND addr_monitor = ?
                    ORDER BY date_create"""
            async with conn.execute(sql, (monitor_id, addr_monitor,)) as cursor:
                result = await cursor.fetchone()
                return result if result else None

    async def select_buys_monitoring_info(self, monitor_id: int, addr_monitor: str):
        """ИНФА для сообщения о свершивсейся сделки - ПРОДАЖИ"""
        # monitor_ids.append(addr_monitor)
        # data = monitor_ids
        async with self.connector as conn:
            sql = f"""SELECT owner_addr, owner_name, coll_addr, coll_name, token_name, token_num,
                        seller_addr, seller_name, price_stars, price_usd, DATETIME(date_create) AS dt
                    FROM buys_monitoring bm
                        JOIN owners o ON bm.addr_monitor = o.owner_addr
                    WHERE monitor_id = ?
                        AND addr_monitor = ?
                    ORDER BY date_create"""
            async with conn.execute(sql, (monitor_id, addr_monitor,)) as cursor:
                result = await cursor.fetchone()
                return result if result else None

    async def select_last_monitor_id(self):
        """id последней записи в sales_monitoring"""
        async with self.connector as conn:
            sql = """SELECT monitor_id FROM sales_monitoring
                    ORDER BY date_add DESC LIMIT 1"""
            async with conn.execute(sql) as cursor:
                result = await cursor.fetchone()
                return result[0] if result else None

    async def select_no_send_sales_monitor_id(self):
        """id неотправленных активностей по ПРОДАЖАМ"""
        async with self.connector as conn:
            sql = """SELECT monitor_id FROM sales_monitoring
                    WHERE monitor_id NOT IN (
                        SELECT monitor_id
                        FROM send_monitor_info
                        WHERE action = 'sale') 
                    ORDER BY date_create DESC"""
            async with conn.execute(sql) as cursor:
                result = await cursor.fetchall()
                return [el[0] for el in result]

    async def select_no_send_buys_monitor_id(self):
        """id неотправленных активностей по ПОКУПКАМ"""
        async with self.connector as conn:
            sql = """SELECT monitor_id FROM sales_monitoring
                    WHERE monitor_id NOT IN (
                        SELECT monitor_id
                        FROM send_monitor_info
                        WHERE action = 'buy') 
                    ORDER BY date_create DESC"""
            async with conn.execute(sql) as cursor:
                result = await cursor.fetchall()
                return [el[0] for el in result]


if __name__ == "__main__":
    monitors_ids = 7
    addr_monitor = ''
    result = asyncio.run(UserDBSelect().select_user_addrs_monitoring())
    for r in result:
        print(r)
