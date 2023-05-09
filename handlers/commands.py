import aiosqlite

from aiogram.types import Message
from aiogram.dispatcher import FSMContext
from database.user_db_select import UserDBSelect
from usefultools.create_bot import dp
from keyboard import kb
from handlers.states import FSMCommands
from lexicon.lexicon_ru import LEXICON_RU_HTML
from lexicon.lexicon_cmd_ru import LEXICON_CMD_RU
from reports.report_24h import get_daily_report
from database.user_db_insert import UserDBInsert


@dp.message_handler(commands=['get_report'])
async def get_report_msg(msg: Message):
    if msg.from_user.id in await UserDBSelect().select_subscribe_users():
        text_ru = await get_daily_report()
        await msg.answer(text=text_ru, parse_mode='HTML',
                         reply_markup=kb.ikb_get_report)
    else:
        await msg.answer(text=LEXICON_RU_HTML["no_subscribe"], parse_mode='HTML',
                         reply_markup=kb.ikb_subscribe)


@dp.message_handler(commands=['wallet_monitor'])
async def query_wallet_monitor(msg: Message):
    text_ru = LEXICON_CMD_RU["wallet_monitor_2"]
    await msg.answer(text=text_ru, parse_mode='HTML')
    await FSMCommands.get_wallet_addr.set()


@dp.message_handler(content_types=['text'], state=FSMCommands.get_wallet_addr)
async def get_wallet_addr(msg: Message, state: FSMContext):
    if len(msg.text) == 44:
        try:
            await UserDBInsert().insert_addrs_monitor(user_id=msg.from_user.id, addr_monitor=msg.text)
            await msg.answer(text=LEXICON_CMD_RU["add_wallet_addr"], parse_mode='HTML')
            await state.finish()
        except aiosqlite.IntegrityError:
            await msg.answer(text=LEXICON_CMD_RU["dbl_wallet_addr"], parse_mode='HTML')
            await state.finish()
    else:
        await msg.answer(text=LEXICON_CMD_RU["err_wallet_addr"], parse_mode='HTML')
        await state.finish()


