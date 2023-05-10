import asyncio
import random
import aiosqlite

from aiogram.types import Message, CallbackQuery
from aiogram.dispatcher import FSMContext

from usefultools.create_bot import dp, bot
from keyboard import kb
from handlers.states import FSMMonitoring, FSMCommands
from lexicon.lexicon_cmd_ru import LEXICON_CMD_RU
from database.user_db_select import UserDBSelect
from database.user_db_update import UserDBUpdate
from database.user_db_insert import UserDBInsert


sec = random.randrange(6, 12)/10


@dp.callback_query_handler(text='wallets', state="*")
async def get_wallets_menu(callback: CallbackQuery):
    msg_id, user_id = callback.message.message_id, callback.from_user.id
    await asyncio.sleep(sec/2)
    await bot.edit_message_text(message_id=msg_id, chat_id=user_id,
                                text=LEXICON_CMD_RU["wallet_menu"], parse_mode='HTML',
                                reply_markup=kb.ikb_wallet_action)
    await FSMMonitoring.wallet_menu.set()
    await callback.answer()


@dp.callback_query_handler(text="back_btn", state=FSMMonitoring.wallet_menu)
async def push_back_btn(callback: CallbackQuery, state: FSMContext):
    """Нажатие кнопки Back для возврата обратно в МЕНЮ"""
    msg_id, user_id = callback.message.message_id, callback.from_user.id
    await asyncio.sleep(sec/2)
    await callback.answer()
    await bot.edit_message_text(message_id=msg_id, chat_id=user_id,
                                text=LEXICON_CMD_RU["bot_menu"], parse_mode='HTML',
                                reply_markup=kb.ikb_bot_menu)
    await state.finish()


@dp.callback_query_handler(text="back_wallet_btn", state=FSMMonitoring.wallet_menu)
async def push_back_wallet_btn(callback: CallbackQuery, state: FSMContext):
    """Нажатие кнопки Back для возврата к действиям с КОШЕЛЬКАМИ"""
    msg_id, user_id = callback.message.message_id, callback.from_user.id
    await asyncio.sleep(sec/2)
    await callback.answer()
    await bot.edit_message_text(message_id=msg_id, chat_id=user_id,
                                text=LEXICON_CMD_RU["wallet_menu"], parse_mode='HTML',
                                reply_markup=kb.ikb_wallet_action)


@dp.callback_query_handler(text='add_new_addr', state=FSMMonitoring.wallet_menu)
async def query_wallet_monitor(callback: CallbackQuery):
    msg_id, user_id = callback.message.message_id, callback.from_user.id
    count_wallets = await UserDBSelect().select_count_addrs_monitor(user_id)
    admins = await UserDBSelect().select_admins()
    if user_id not in admins:  # для админов ограничение на отслеживание кошельков нет
        if count_wallets <= 5:
            await asyncio.sleep(sec/2)
            await bot.edit_message_text(message_id=msg_id, chat_id=user_id,
                                        text=f'{LEXICON_CMD_RU["wallet_monitor_1"]}<b>{count_wallets} адрес(-а,-ов).</b>',
                                        parse_mode='HTML', reply_markup='')
            await callback.answer()
            await asyncio.sleep(sec/2)
            await callback.message.answer(text=LEXICON_CMD_RU["wallet_monitor_2"], parse_mode='HTML')
            await FSMCommands.get_wallet_addr.set()
        else:  # если у пользователя добавлено больше 5 кошельков
            await asyncio.sleep(sec / 2)
            await bot.edit_message_text(message_id=msg_id, chat_id=user_id,
                                        text=LEXICON_CMD_RU["addrs_limit_1"],
                                        parse_mode='HTML', reply_markup='')
            await asyncio.sleep(sec)
            await callback.message.answer(text=LEXICON_CMD_RU["wallet_menu"], parse_mode='HTML',
                                          reply_markup=kb.ikb_wallet_action)
    else:
        await asyncio.sleep(sec)
        await bot.edit_message_text(message_id=msg_id, chat_id=user_id,
                                    text=LEXICON_CMD_RU["wallet_monitor_2"], parse_mode='HTML')
        await FSMCommands.get_wallet_addr.set()


@dp.message_handler(content_types=['text'], state=FSMCommands.get_wallet_addr)
async def get_wallet_addr(msg: Message, state: FSMContext):
    """Добавляем кошелёк для отслеживания в БД"""
    if len(msg.text) == 44:
        try:
            await UserDBInsert().insert_addrs_monitor(user_id=msg.from_user.id, addr_monitor=msg.text)
            await msg.answer(text=LEXICON_CMD_RU["add_wallet_addr"], parse_mode='HTML')
            await asyncio.sleep(sec)
            await msg.answer(text=LEXICON_CMD_RU["wallet_menu"], parse_mode='HTML',
                             reply_markup=kb.ikb_wallet_action)
            await FSMMonitoring.wallet_menu.set()
        except aiosqlite.IntegrityError:
            await msg.answer(text=LEXICON_CMD_RU["dbl_wallet_addr"], parse_mode='HTML')
    else:
        await msg.answer(text=LEXICON_CMD_RU["err_wallet_addr"], parse_mode='HTML')
        await asyncio.sleep(sec)
        await msg.answer(text=LEXICON_CMD_RU["wallet_menu"], parse_mode='HTML',
                         reply_markup=kb.ikb_wallet_action)
        await FSMMonitoring.wallet_menu.set()


@dp.callback_query_handler(text="delete_addr", state=FSMMonitoring.wallet_menu)
async def choice_delete_addr(callback: CallbackQuery, state: FSMContext):
    msg_id, user_id = callback.message.message_id, callback.from_user.id
    addrs_list = await UserDBSelect().select_user_addrs_monitoring(user_id)
    await asyncio.sleep(sec)
    await bot.edit_message_text(message_id=msg_id, chat_id=user_id,
                                text=LEXICON_CMD_RU["choice_delete_addr"], parse_mode='HTML',
                                reply_markup=kb.get_user_addrs_monitor(addrs_list))
    await callback.answer()


@dp.callback_query_handler(lambda callback: callback.data.startswith('stars'),
                           state=FSMMonitoring.wallet_menu)
async def delete_addr(callback: CallbackQuery, state: FSMContext):
    """Снимаем флаг с отслеживаемого адреса"""
    msg_id, user_id = callback.message.message_id, callback.from_user.id
    del_addr = callback.data
    del_addr_text = f"{callback.data[:9]}...{callback.data[-4]}"
    await asyncio.sleep(sec)
    await bot.edit_message_text(message_id=msg_id, chat_id=user_id,
                                text=f"Происходит удаление <b>{del_addr_text}</b>...",
                                parse_mode='HTML', reply_markup='')
    try:
        await UserDBUpdate().upd_addrs_monitor(user_id=user_id, addr_monitor=del_addr)
        await asyncio.sleep(sec*2)
        await bot.edit_message_text(message_id=msg_id, chat_id=user_id,
                                    text=LEXICON_CMD_RU["delete_addr"])
    except:
        pass
    await asyncio.sleep(sec)
    await callback.answer()
    await state.finish()
    await callback.message.answer(text=LEXICON_CMD_RU["bot_menu"], parse_mode='HTML',
                                  reply_markup=kb.ikb_bot_menu)


@dp.callback_query_handler(text='monitoring_wallet', state=FSMMonitoring.wallet_menu)
async def monitoring_list(callback: CallbackQuery):
    """Выводит список отслеживаемых адресов"""
    msg_id, user_id = callback.message.message_id, callback.from_user.id
    full_addrs_list = await UserDBSelect().select_user_addrs_monitoring(user_id)
    if full_addrs_list:
        count = await UserDBSelect().select_count_addrs_monitor(user_id)
        addrs_list: list = []
        for addr in full_addrs_list:
            addrs_list.append(f"{addr[:12]}...{addr[-8:]}\n")
        text_ru = f"<b>Сейчас отслеживается {count} адрес(-а,-ов)</b>:\n\n" \
                  f"◗ {'◗ '.join(addrs_list)}"
        await asyncio.sleep(sec/2)
        await callback.answer()
        await bot.edit_message_text(message_id=msg_id, chat_id=user_id,
                                    text=text_ru, parse_mode='HTML',
                                    reply_markup='')
        await asyncio.sleep(sec)
        await callback.message.answer(text=LEXICON_CMD_RU["wallet_menu"], parse_mode='HTML',
                                      reply_markup=kb.ikb_wallet_action)
    else:
        await callback.answer()
        await bot.edit_message_text(message_id=msg_id, chat_id=user_id,
                                    text=LEXICON_CMD_RU["no_addrs_monitoring"],
                                    parse_mode='HTML', reply_markup='')
        await asyncio.sleep(sec)
        await callback.message.answer(text=LEXICON_CMD_RU["wallet_menu"], parse_mode='HTML',
                                      reply_markup=kb.ikb_wallet_action)