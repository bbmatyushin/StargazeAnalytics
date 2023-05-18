import asyncio

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton


"""Кнопка подписаться на рассылку отчетов"""
subscribe_btn = InlineKeyboardButton(text='📝 Subscribe', callback_data='subscribe')
"""Кнопка для ВЫБОРА отчётов """
reports_bnt = InlineKeyboardButton(text='📊 Reports', callback_data='reports_menu')
"""Кнопка для получения отчёт за 24 ЧАСА прямо сейчас"""
get_24h_report = InlineKeyboardButton(text='1️⃣ 24H report', callback_data='get_report_24h')
"""Кнопка для получения отчёт за 7 ДНЕЙ прямо сейчас"""
get_7d_report = InlineKeyboardButton(text='2️⃣ 7D report', callback_data='get_report_7d')
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
"""Кнопка МЕНЮ ОТЧЁТОВ"""
ikb_reports_menu = InlineKeyboardMarkup(row_width=1).add(reports_bnt)
"""Кнопка для ВЫБОРА ОТЧЁТОВ"""
ikb_reports_choice = InlineKeyboardMarkup(row_width=2).add(get_24h_report, get_7d_report,
                                                           back_btn)
"""Отдельная кнопка ПОКАЗАТЬ ОТЧЁТ"""
ikb_get_report_24h = InlineKeyboardMarkup(row_width=1).add(get_24h_report)
"""Подписаться или Отказаться"""
ikb_subscribe_no = InlineKeyboardMarkup(row_width=2).add(subscribe_btn, no_thanks)
"""ИнлайнКнопка основного МЕНЮ"""
ikb_bot_menu = InlineKeyboardMarkup(row_width=2).add(reports_bnt, wallets)
"""Управление адресами кошельков"""
ikb_wallet_action = InlineKeyboardMarkup(row_width=2).add(delete_addr, add_new_addr,
                                                          monitoring_addrs, back_btn)


##############  ГЕНЕРАЦИЯ КНОПОК  ######################
def get_user_addrs_monitor(addrs_list: list):
    """Инлайн кнопки с адресами для мониторинга"""
    ikb_addrs_monitor = InlineKeyboardMarkup(row_width=1)
    for addr in addrs_list:
        # addr[1] это owner_name
        addr_text = addr[1] if addr[1] else f"{addr[0][:12]}...{addr[0][-12:]}"
        addr_btn = InlineKeyboardButton(text=addr_text, callback_data=addr[0])
        ikb_addrs_monitor.add(addr_btn)
    return ikb_addrs_monitor.add(back_wallet_btn)
