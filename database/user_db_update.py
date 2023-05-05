from database.user_db_create import UserDB


class UserDBUpdate(UserDB):
    async def upd_users_block_bot_0(self, user_id: int, flag: int):
        """Обновляем состояние подписки + дату"""
        async with self.connector as conn:
            sql = """UPDATE users_subscribe
                    SET subscribe_flag = ?, date_add = DATETIME()
                    WHERE user_id = ?"""
            await conn.execute(sql, (flag, user_id,))
            await conn.commit()
