from database.user_db_create import UserDB


class UserDBDelete(UserDB):
    async def del_users_block_bot(self, user_id):
        """Удаляем пользователя из таблицы тех кто заблокировал бота,
        если пользователь передумал и решил опять пользоваться ботом"""
        async with self.connector as conn:
            sql = """DELETE FROM users_block_bot
                    WHERE user_id = ?"""
            await conn.execute(sql, (user_id, ))
            await conn.commit()
