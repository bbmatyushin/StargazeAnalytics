from aiogram.types import Message
from database.user_db_select import UserDBSelect
from usefultools.create_bot import dp
from keyboard import kb
from lexicon.lexicon_ru import LEXICON_RU_HTML
from reports.report_24h import get_daily_report


@dp.message_handler(commands=['get_report'])
async def get_report_msg(msg: Message):
    if msg.from_user.id in await UserDBSelect().select_subscribe_users():
        text_ru = await get_daily_report()
        await msg.answer(text=text_ru, parse_mode='HTML',
                         reply_markup=kb.ikb_get_report)
    else:
        await msg.answer(text=LEXICON_RU_HTML["no_subscribe"], parse_mode='HTML',
                         reply_markup=kb.ikb_subscribe)
