import asyncio
import aiosqlite
import logging

from aiogram.types import Message, CallbackQuery
from aiogram.dispatcher import FSMContext

from handlers.states import FSMMain
from lexicon.lexicon_ru import LEXICON_RU_HTML
from usefultools.create_bot import dp, bot
from keyboard import kb
from database.user_db_insert import UserDBInsert

from reports.report_24h import get_daily_report

logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=logging.INFO)


@dp.message_handler(commands=['start'])
async def command_start(msg: Message):
    await bot.delete_message(chat_id=msg.from_user.id, message_id=msg.message_id)
    await msg.answer(text=LEXICON_RU_HTML['/start'], parse_mode='HTML')
    await asyncio.sleep(1)
    await msg.answer(text=LEXICON_RU_HTML['/start_2'], parse_mode='HTML',
                     reply_markup=kb.ikb_start)
    await FSMMain.start_bot.set()


@dp.callback_query_handler(text='example_report', state=FSMMain.start_bot)
async def send_example_report(callback: CallbackQuery, state: FSMContext):
    user_id, username, first_name, last_name, language_code = \
        callback.from_user.id, callback.from_user.username, callback.from_user.first_name, \
        callback.from_user.last_name, callback.from_user.language_code
    insert_data = [user_id, username, first_name, last_name, language_code]
    try:
        await UserDBInsert().insert_users(insert_data=insert_data)
    except aiosqlite.IntegrityError as err:
        logging.warning(err)
    text_ru = await get_daily_report()
    await callback.message.answer(text=text_ru, parse_mode="HTML")
    await callback.answer()
    await asyncio.sleep(1)
    await callback.message.answer(text="ГОТОВЫ ПОДПИСАТЬСЯ", parse_mode='HTML',
                                  reply_markup=kb.ikb_subscribe_report)
    await state.finish()

