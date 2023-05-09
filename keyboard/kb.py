from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton


"""Кнопка подписаться на рассылку отчетов"""
subscribe_btn = InlineKeyboardButton(text='📝 Subscribe', callback_data='subscribe')
"""Кнопка для получения отчёт прямо сейчас"""
get_report_bnt = InlineKeyboardButton(text='📊 Get report', callback_data='get_report')
"""Кнопка показать пример отчёта"""
example_report = InlineKeyboardButton(text='Example report 👀', callback_data='example_report')
"""Кнопка Нет спасибо"""
no_thanks = InlineKeyboardButton(text='✖️ No thanks', callback_data='no_thanks')


##############  НАБОРЫ КНОПОК  ######################
"""Кнопки при первом старте бота"""
ikb_start = InlineKeyboardMarkup(row_width=2).add(subscribe_btn, example_report)
"""Отдельная кнопка ПОДПИСАТЬСЯ"""
ikb_subscribe = InlineKeyboardMarkup(row_width=1).add(subscribe_btn)
"""Отдельная кнопка ПОКАЗАТЬ ОТЧЁТ"""
ikb_get_report = InlineKeyboardMarkup(row_width=1).add(get_report_bnt)
"""Подписаться или Отказаться"""
ikb_subscribe_no = InlineKeyboardMarkup(row_width=2).add(subscribe_btn, no_thanks)
