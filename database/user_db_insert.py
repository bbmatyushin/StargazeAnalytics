import asyncio

from database.user_db_create import UserDB


class UserDBInsert(UserDB):
    async def insert_users(self, insert_data: list):
        """Вставляем данные нового пользователя
        insert_data = [user_id, username, first_name, last_name, language_code]"""
        async with self.connector as conn:
            sql = """INSERT INTO users(
                    user_id, username, first_name, last_name, language_code, date_add)
                    VALUES(?, ?, ?, ?, ?, DATETIME())"""
            await conn.execute(sql, insert_data)
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

    async def insert_users_block_bot(self, user_id: int):
        """Отмечаем флагом тех кто удалил чат с ботом"""
        async with self.connector as conn:
            sql = """INSERT INTO users_block_bot(user_id, block_flag, date_add)
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


if __name__ == "__main__":
    insert_data = [1, 'username', 'first_name', 'last_name', 'language_code']
    asyncio.run(UserDBInsert().insert_admins(1916570670))
