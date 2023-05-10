"""Фильтры для хэндлеров"""

from aiogram.types import Message, CallbackQuery
from database.user_db_select import UserDBSelect


class ActiveUsersMsg:
    """Фильтрует по активным пользователям, для message_handlers"""
    def __init__(self):
        self.active_users_list = UserDBSelect().select_active_users()

    def __call__(self, msg: Message):
        return msg.from_user.id in self.active_users_list


class ActiveUsersCbq(ActiveUsersMsg):
    """Фильтрует по активным пользователям, для callback_query_handlers"""
    def __call__(self, callback: CallbackQuery):
        return callback.from_user.id in self.active_users_list


class IsAdminMsg:
    """Фильтрует по админам, для message_handlers"""
    def __init__(self):
        self.admins_list = UserDBSelect().select_admins()

    async def get_admins(self):
        return await UserDBSelect().select_admins()

    def __call__(self, msg: Message):
        admins_list = self.get_admins()
        return msg.from_user.id in admins_list


class IsAdminCbq(IsAdminMsg):
    """Фильтрует по админам, для callback_query_handlers"""
    def __call__(self, callbakc: CallbackQuery):
        return callbakc.from_user.id in self.admins_list


if __name__ == "__main__":
    a = IsAdminMsg().get_admins()
    print(a)
