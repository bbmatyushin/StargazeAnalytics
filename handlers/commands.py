import asyncio
import random
# import aiosqlite

from aiogram.types import Message
from aiogram.dispatcher import FSMContext
from database.user_db_select import UserDBSelect
from usefultools.create_bot import dp
from keyboard import kb
from handlers.states import FSMCommands
from lexicon.lexicon_ru import LEXICON_RU_HTML
from lexicon.lexicon_cmd_ru import LEXICON_CMD_RU
from reports.report_24h import get_daily_report
# from database.user_db_insert import UserDBInsert


@dp.message_handler(commands=['bot_menu', 'cancel'], state="*")
async def get_bot_menu(msg: Message, state: FSMContext):
    await msg.answer(text=LEXICON_CMD_RU["bot_menu"], parse_mode='HTML',
                     reply_markup=kb.ikb_bot_menu)
    await state.finish()


@dp.message_handler(commands=['get_report_24h'])
async def get_report_24h_msg(msg: Message):
    """Получаем отчет за последние 24 часа"""
    if msg.from_user.id in await UserDBSelect().select_subscribe_users_24h_report():
        text_ru = await get_daily_report()
        await msg.answer(text=text_ru, parse_mode='HTML',
                         reply_markup=kb.ikb_get_report_24h)
    else:
        await msg.answer(text=LEXICON_RU_HTML["no_subscribe"], parse_mode='HTML',
                         reply_markup=kb.ikb_subscribe)


@dp.message_handler(commands=['wallet_monitor'])
async def query_wallet_monitor(msg: Message):
    sec = random.randrange(6, 18)/10
    user_id = msg.from_user.id
    count_wallets = await UserDBSelect().select_count_addrs_monitor(user_id)
    admins = await UserDBSelect().select_admins()
    if user_id not in admins:  # для админов ограничение на отслеживание кошельков нет
        if count_wallets <= 5:
            await msg.answer(text=f'{LEXICON_CMD_RU["wallet_monitor_1"]}<b>{count_wallets} адрес(-а,-ов).</b>',
                             parse_mode='HTML')
            await asyncio.sleep(sec)
            await msg.answer(text=LEXICON_CMD_RU["wallet_monitor_2"], parse_mode='HTML')
            await FSMCommands.get_wallet_addr.set()
        else:
            await msg.answer(text=LEXICON_CMD_RU["addrs_limit_1"], parse_mode='HTML')
    else:
        await msg.answer(text=LEXICON_CMD_RU["wallet_monitor_2"], parse_mode='HTML')
        await FSMCommands.get_wallet_addr.set()
