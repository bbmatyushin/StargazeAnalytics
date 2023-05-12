import asyncio

from aiogram.types import Message
from aiogram.dispatcher import FSMContext
from aiogram.utils.exceptions import ChatNotFound, BotBlocked

from usefultools.create_bot import dp, bot
from database.user_db_select import UserDBSelect
from lexicon.lexicon_upd import LEXICON_UPD_RU
from lexicon.lexicon_ru import LEXICON_RU_HTML
from handlers.states import FSMAdmin

from reports.report_blitz import get_blitz_report


@dp.message_handler(commands=['send_all'], state="*")
async def send_msg_all_users_1(msg: Message, state: FSMContext):
    if msg.from_user.id in await UserDBSelect().select_admins():
        text_head = "Вы собираетесь отправить всем пользователям сообщение:\n\n"
        text_tail = "\n\n<b>ДА</b> /send_upd\t\t\t\t\t<b>ОТМЕНА</b> /cancel"
        text = text_head + LEXICON_UPD_RU["update_1"] + text_tail
        await msg.answer(text=text, parse_mode='HTML')
        async with state.proxy() as data:
            data["msg_text"] = LEXICON_UPD_RU["update_1"]
        await FSMAdmin.send_msg_all.set()


@dp.message_handler(commands=['send_upd'], state=FSMAdmin.send_msg_all)
async def send_msg_all_users_2(msg: Message, state: FSMContext):
    user_ids = await UserDBSelect().select_active_users()
    users_list: list = []
    async with state.proxy() as data:
        text = data["msg_text"]
    for user_id in user_ids:
        try:
            await bot.send_message(chat_id=user_id, text=text, parse_mode='HTML')
            users_list.append(f"\n{user_id}")
        except ChatNotFound:
            continue
        except BotBlocked:
            continue
    await asyncio.sleep(1.5)
    text_for_admin = f"Было отправлено сообщение следующим пользователям:\n" \
                     f"{''.join(users_list)}"
    await msg.answer(text=text_for_admin)
    await state.finish()


@dp.message_handler(commands='floor_dif', state="*")
async def admin_send_blitz_report(msg: Message):
    """Ограничение - только для админов"""
    report = await get_blitz_report()
    report_text_ru = "".join(report[:10])
    admins = await UserDBSelect().select_admins_subscribe()
    if msg.from_user.id in admins:
        try:
            await msg.answer(text=report_text_ru, parse_mode='HTML')
        except ChatNotFound:
            pass
        except BotBlocked:
            pass


@dp.message_handler(commands=['admin'], state="*")
async def admin_cmds(msg: Message):
    # проверяем пользователя на права админа
    if msg.from_user.id in await UserDBSelect().select_admins():
        await msg.answer(text=LEXICON_RU_HTML["admin_cmds"], parse_mode='HTML')
