import asyncio
import aiosqlite
import random

from aiogram.types import Message, CallbackQuery
from aiogram.dispatcher import FSMContext

from keyboard import kb
from handlers.states import FSMReports
from lexicon.lexicon_ru import LEXICON_RU_HTML
from lexicon.lexicon_cmd_ru import LEXICON_CMD_RU
from database.user_db_select import UserDBSelect
from reports.report_24h import get_daily_report
from reports.report_7d import get_weekly_report
from usefultools.create_bot import dp, bot


sec = random.randrange(6, 12)/10


@dp.callback_query_handler(text='reports_menu', state="*")
async def get_report_menu(callback: CallbackQuery):
    """Показать варианты отчётов"""
    msg_id, user_id = callback.message.message_id, callback.from_user.id
    await asyncio.sleep(sec / 2)
    await bot.edit_message_text(message_id=msg_id, chat_id=user_id,
                                text=LEXICON_CMD_RU["report_menu"], parse_mode='HTML',
                                reply_markup=kb.ikb_reports_choice)
    await FSMReports.reports_menu.set()
    await callback.answer()


@dp.callback_query_handler(text='get_report_24h', state=FSMReports.reports_menu)
async def get_report_24h_callback(callback: CallbackQuery):
    """Получаем отчет за последние 24 часа"""
    if callback.from_user.id in await UserDBSelect().select_subscribe_users_report(report_name='24h_report'):
        text_ru = await get_daily_report()
        await callback.message.answer(text=text_ru, parse_mode="HTML")
        await asyncio.sleep(sec / 2)
        await callback.message.answer(text=LEXICON_CMD_RU["report_menu_single"],
                                      parse_mode='HTML', reply_markup=kb.ikb_reports_choice)
    else:
        await callback.message.answer(text=LEXICON_RU_HTML["no_subscribe"],
                                      parse_mode='HTML', reply_markup=kb.ikb_subscribe_no)
    await callback.answer()


@dp.callback_query_handler(text='get_report_7d', state=FSMReports.reports_menu)
async def get_report_7d_callback(callback: CallbackQuery):
    """Получаем отчет за последние 7 дней"""
    if callback.from_user.id in await UserDBSelect().select_subscribe_users_report(report_name='7d_report'):
        text_ru = await get_weekly_report()
        await callback.message.answer(text=text_ru, parse_mode="HTML")
        await asyncio.sleep(sec / 2)
        await callback.message.answer(text=LEXICON_CMD_RU["report_menu_single"],
                                      parse_mode='HTML', reply_markup=kb.ikb_reports_choice)
    else:
        await callback.message.answer(text=LEXICON_RU_HTML["no_subscribe"],
                                      parse_mode='HTML', reply_markup=kb.ikb_subscribe_no)
    await callback.answer()


@dp.callback_query_handler(text="back_btn", state=FSMReports.reports_menu)
async def push_back_btn(callback: CallbackQuery, state: FSMContext):
    """Нажатие кнопки Back для возврата обратно в МЕНЮ"""
    msg_id, user_id = callback.message.message_id, callback.from_user.id
    await asyncio.sleep(sec/2)
    await callback.answer()
    await bot.edit_message_text(message_id=msg_id, chat_id=user_id,
                                text=LEXICON_CMD_RU["bot_menu"], parse_mode='HTML',
                                reply_markup=kb.ikb_bot_menu)
    await state.finish()