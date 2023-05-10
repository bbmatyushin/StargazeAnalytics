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

    async def upd_addrs_monitor(self, user_id: int, addr_monitor: str):
        """Снимаем флаг мониторинга за кошельком"""
        async with self.connector as conn:
            sql = """UPDATE addrs_monitor
                    SET monitor_flag = 0
                    WHERE user_id = ?
                        AND addr_monitor = ?"""
            await conn.execute(sql, (user_id, addr_monitor,))
            await conn.commit()
