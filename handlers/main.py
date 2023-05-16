import asyncio
import aiosqlite
import logging

from aiogram.types import Message, CallbackQuery
# from aiogram.dispatcher import FSMContext

# from handlers.states import FSMMain
from lexicon.lexicon_ru import LEXICON_RU_HTML
from usefultools.create_bot import dp, bot
from keyboard import kb
from database.user_db_insert import UserDBInsert
from database.user_db_select import UserDBSelect
from aiogram.utils.exceptions import ChatNotFound, BotBlocked

from reports.report_24h import get_daily_report

logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=logging.INFO)


@dp.message_handler(commands=['start'])
async def command_start(msg: Message):
    user_id, username, first_name, last_name, language_code = \
        msg.from_user.id, msg.from_user.username, msg.from_user.first_name, \
            msg.from_user.last_name, msg.from_user.language_code
    data_insert = [user_id, username, first_name, last_name, language_code]
    try:  # добавляем пользователя в БД
        await UserDBInsert().insert_users(data_insert=data_insert)
        admins = await UserDBSelect().select_admins()
        for admin in admins:
            try:  # оповещеть о новом пользователе
                await bot.send_message(chat_id=admin,
                                       text=f'{LEXICON_RU_HTML["new_user"]}{data_insert}',
                                       parse_mode='HTML')
            except ChatNotFound:
                continue
            except BotBlocked:
                continue
    except aiosqlite.IntegrityError as err:
        logging.warning(err)
    await bot.delete_message(chat_id=msg.from_user.id, message_id=msg.message_id)
    await msg.answer(text=LEXICON_RU_HTML['/start'], parse_mode='HTML')
    await asyncio.sleep(1)
    await msg.answer(text=LEXICON_RU_HTML['/start_2'], parse_mode='HTML',
                     reply_markup=kb.ikb_start)


@dp.callback_query_handler(text='example_report', state="*")
async def send_example_report(callback: CallbackQuery):
    text_ru = await get_daily_report()
    await callback.message.answer(text=text_ru, parse_mode="HTML")
    await callback.answer()
    await asyncio.sleep(1)
    await callback.message.answer(text=LEXICON_RU_HTML["example_report"], parse_mode='HTML',
                                  reply_markup=kb.ikb_subscribe_no)


@dp.callback_query_handler(text='subscribe', state="*")
async def subscribe_report(callback: CallbackQuery):
    user_id = callback.from_user.id
    try:  # ставим флаг, что пользователь подписан
        await UserDBInsert().insert_users_subscribe(user_id=user_id)
    except aiosqlite.IntegrityError as err:
        logging.warning(err)
    await callback.message.answer(text=LEXICON_RU_HTML["subscribe"], parse_mode='HTML',
                                  reply_markup=kb.ikb_get_report_24h)
    await callback.answer()


@dp.callback_query_handler(text='get_report_24h', state="*")
async def get_report_24h_callback(callback: CallbackQuery):
    """Получаем отчет за последние 24 часа"""
    if callback.from_user.id in await UserDBSelect().select_subscribe_users_24h_report():
        text_ru = await get_daily_report()
        await callback.message.answer(text=text_ru, parse_mode="HTML")
    else:
        await callback.message.answer(text=LEXICON_RU_HTML["no_subscribe"],
                                      parse_mode='HTML')
    await callback.answer()


@dp.callback_query_handler(text='no_thanks', state="*")
async def no_thanks(callback: CallbackQuery):
    user_id = callback.from_user.id
    msg_id = callback.message.message_id
    await bot.edit_message_text(chat_id=user_id, message_id=msg_id,
                                text=LEXICON_RU_HTML["no_thanks"], parse_mode='HTML',
                                reply_markup=kb.ikb_subscribe)
    await callback.answer()



