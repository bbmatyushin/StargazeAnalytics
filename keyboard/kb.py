import asyncio

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton


"""Кнопка подписаться на рассылку отчетов"""
subscribe_btn = InlineKeyboardButton(text='📝 Subscribe', callback_data='subscribe')
"""Кнопка для получения отчёт прямо сейчас"""
get_report_bnt = InlineKeyboardButton(text='📊 Get report', callback_data='get_report')
"""Кнопка показать пример отчёта"""
example_report = InlineKeyboardButton(text='Example report 👀', callback_data='example_report')
"""Кнопка Нет спасибо"""
no_thanks = InlineKeyboardButton(text='✖️ No thanks', callback_data='no_thanks')
"""Кнопка НАЗАД"""
back_btn = InlineKeyboardButton(text='🔙 Back', callback_data='back_btn')
"""Кнопка НАЗАД для возврата к действиям с кошельком"""
back_wallet_btn = InlineKeyboardButton(text='🔙 Back', callback_data='back_wallet_btn')
"""Кнопка КОШЕЛЬКИ (для действий с отслеживанием)"""
wallets = InlineKeyboardButton(text='💼 Wallets', callback_data='wallets')
"""Кнопка добавить НОВЫЙ АДРЕС"""
add_new_addr = InlineKeyboardButton(text="➕ New addr", callback_data='add_new_addr')
"""Кнопка УДАЛИТЬ АДРЕС"""
delete_addr = InlineKeyboardButton(text='➖ Delete addr', callback_data='delete_addr')
"""Кнопка МОНИТОРИНГ (показать список отслеживаемых кошельков)"""
monitoring_addrs = InlineKeyboardButton(text='📔 Monitoring', callback_data='monitoring_wallet')


##############  НАБОРЫ КНОПОК  ######################
"""Кнопки при первом старте бота"""
ikb_start = InlineKeyboardMarkup(row_width=2).add(subscribe_btn, example_report)
"""Отдельная кнопка ПОДПИСАТЬСЯ"""
ikb_subscribe = InlineKeyboardMarkup(row_width=1).add(subscribe_btn)
"""Отдельная кнопка ПОКАЗАТЬ ОТЧЁТ"""
ikb_get_report = InlineKeyboardMarkup(row_width=1).add(get_report_bnt)
"""Подписаться или Отказаться"""
ikb_subscribe_no = InlineKeyboardMarkup(row_width=2).add(subscribe_btn, no_thanks)
"""ИнлайнКнопка основного МЕНЮ"""
ikb_bot_menu = InlineKeyboardMarkup(row_width=2).add(get_report_bnt, wallets)
"""Управление адресами кошельков"""
ikb_wallet_action = InlineKeyboardMarkup(row_width=2).add(delete_addr, add_new_addr,
                                                          monitoring_addrs, back_btn)


##############  ГЕНЕРАЦИЯ КНОПОК  ######################
def get_user_addrs_monitor(addrs_list: list):
    """Инлайн кнопки с адресами для мониторинга"""
    ikb_addrs_monitor = InlineKeyboardMarkup(row_width=1)
    for addr in addrs_list:
        addr_text = f"{addr[:12]}...{addr[-12:]}"
        addr_btn = InlineKeyboardButton(text=addr_text, callback_data=addr)
        ikb_addrs_monitor.add(addr_btn)
    return ikb_addrs_monitor.add(back_wallet_btn)
