import asyncio

from database.user_db_create import UserDB


class UserDBInsert(UserDB):
    async def insert_users(self, data_insert: list):
        """Вставляем данные нового пользователя
        data_insert = [user_id, username, first_name, last_name, language_code]"""
        async with self.connector as conn:
            sql = """INSERT INTO users(
                    user_id, username, first_name, last_name, language_code, date_add)
                    VALUES(?, ?, ?, ?, ?, DATETIME())"""
            await conn.execute(sql, data_insert)
            await conn.commit()

    async def insert_daily_reports(self, message: str):
        """Наполняем таблицу ежедневных отчетов"""
        async with self.connector as conn:
            sql = """INSERT INTO daily_reports(report_msg, date_add)
                    VALUES(?, DATETIME())"""
            await conn.execute(sql, (message, ))
            await conn.commit()

    async def insert_weekly_reports(self, message: str):
        """Наполняем таблицу еженедельных отчетов"""
        async with self.connector as conn:
            sql = """INSERT INTO weekly_reports(report_msg, date_add)
                    VALUES(?, DATETIME())"""
            await conn.execute(sql, (message, ))
            await conn.commit()

    async def insert_monthly_reports(self, message: str):
        """Наполняем таблицу еженедельных отчетов"""
        async with self.connector as conn:
            sql = """INSERT INTO monthly_reports(report_msg, date_add)
                    VALUES(?, DATETIME())"""
            await conn.execute(sql, (message, ))
            await conn.commit()

    async def insert_admins(self, user_id: int):
        """Добавляем админов боту"""
        async with self.connector as conn:
            sql = """INSERT INTO admins(user_id, date_add)
                    VALUES(?, DATETIME())"""
            await conn.execute(sql, (user_id,))
            await conn.commit()

    async def insert_inactive_users(self, user_id: int):
        """Отмечаем флагом тех кто удалил чат с ботом"""
        async with self.connector as conn:
            sql = """INSERT INTO inactive_users(user_id, inactive_flag, date_add)
                    VALUES(?, 1, DATETIME())"""
            await conn.execute(sql, (user_id,))
            await conn.commit()

    async def insert_users_subscribe(self, user_id: int):
        """Отмечаем флагом тех кто подписался на отчеты"""
        async with self.connector as conn:
            sql = """INSERT INTO users_subscribe(user_id, subscribe_flag, date_add)
                    VALUES(?, 1, DATETIME())"""
            await conn.execute(sql, (user_id,))
            await conn.commit()

    async def insert_send_daily_report(self, user_id: int, report_id: int):
        """Отмечаем флагом пользователя которому был отправлен
        ежеДНЕВНЫЙ отчет"""
        async with self.connector as conn:
            sql = """INSERT INTO send_daily_report(
                    user_id, report_id, send_flag, date_add)
                    VALUES(?, ?, 1, DATETIME())"""
            await conn.execute(sql, (user_id, report_id,))
            await conn.commit()

    async def insert_send_weekly_report(self, user_id: int, report_id: int):
        """Отмечаем флагом пользователя которому был отправлен
        ежеНЕДЕЛЬНЫЙ отчет"""
        async with self.connector as conn:
            sql = """INSERT INTO send_weekly_report(
                    user_id, report_id, send_flag, date_add)
                    VALUES(?, ?, 1, DATETIME())"""
            await conn.execute(sql, (user_id, report_id,))
            await conn.commit()

    async def insert_send_monthly_report(self, user_id: int, report_id: int):
        """Отмечаем флагом пользователя которому был отправлен
        ежеМЕСЯЧНЫЙ отчет"""
        async with self.connector as conn:
            sql = """INSERT INTO send_monthly_report(
                    user_id, report_id, send_flag, date_add)
                    VALUES(?, ?, 1, DATETIME())"""
            await conn.execute(sql, (user_id, report_id,))
            await conn.commit()

    async def insert_user_actions(self, user_id: int, action_name: str):
        """Сохраняем действия пользователей, такие как нажатие инлайн-кнопок"""
        async with self.connector as conn:
            sql = """INSERT INTO user_actions(user_id, action, date_add)
                    VALUES(?, ?, DATETIME())"""
            await conn.execute(sql, (user_id, action_name,))
            await conn.commit()

    #TODO: ############  MONITORING INSERT  ###############
    async def insert_sales_monitoring(self, data_insert: list):
        """Заполняем данными таблицу мониторинга продаж
        data_insert = [addr_monitor, coll_addr, coll_name, token_name, token_num,
        buyer_addr, buyer_name, price_stars, price_usd, date_create, date_add]"""
        async with self.connector as conn:
            sql = """INSERT INTO sales_monitoring(
                    addr_monitor, coll_addr, coll_name, token_name, token_num,
                    buyer_addr, buyer_name, price_stars, price_usd, date_create, date_add)
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, DATETIME())"""
            await conn.execute(sql, data_insert)
            await conn.commit()

    async def insert_send_monitor_info(self, user_id, monitor_id):
        """Вставляем данные об отправке сообщения о продаже.
        Т.е. бот отправляет сообщение о продаже и заполняется эта таблица"""
        async with self.connector as conn:
            sql = """INSERT INTO send_monitor_info(user_id, monitor_id, send_flag, date_add)
                    VALUES(?, ?, 1, DATETIME())"""
            await conn.execute(sql, (user_id, monitor_id,))
            await conn.commit()

    async def insert_addrs_monitor(self, user_id, addr_monitor):
        """Добавление пользователями адресов для отслеживания"""
        async with self.connector as conn:
            sql = """INSERT INTO addrs_monitor(user_id, addr_monitor, monitor_flag, date_add)
                    VALUES(?, ?, 1, DATETIME())"""
            await conn.execute(sql, (user_id, addr_monitor,))
            await conn.commit()


if __name__ == "__main__":
    data_insert = [1, 'username', 'first_name', 'last_name', 'language_code']
    asyncio.run(UserDBInsert().insert_admins(1916570670))
