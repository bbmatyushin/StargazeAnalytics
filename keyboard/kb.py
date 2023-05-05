from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton


"""Кнопка подписаться на рассылку отчетов"""
subscribe_btn = InlineKeyboardButton(text='📊 Subscribe report', callback_data='subscribe')

"""Кнопка показать пример отчёта"""
example_report = InlineKeyboardButton(text='Example report 👀', callback_data='example_report')

"""Кнопки при первом старте бота"""
ikb_start = InlineKeyboardMarkup(row_width=2).add(subscribe_btn, example_report)

"""Отдельная кнопка ПОДПИСАТЬСЯ"""
ikb_subscribe_report = InlineKeyboardMarkup(row_width=1).add(subscribe_btn)
