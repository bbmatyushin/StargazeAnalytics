import logging

from aiogram.types import BotCommand
from aiogram.utils import executor
from aiogram.utils.exceptions import ChatNotFound, BotBlocked

from usefultools.create_bot import bot, dp
from database.user_db_select import UserDBSelect, UserDB
from crontab import crontab_users, crontab_db, crontab_reports  # Запуск задач по расписанию

from handlers import admins, main, commands, reports, addrs_monitoring


async def on_startup(_):
    crontab_users.send_actions_monitor_info.start()
    crontab_db.addrs_monitoring.start()
    crontab_db.transfer_owners.start()
    crontab_reports.send_daily_report.start()
    crontab_reports.send_weekly_report.start()
    crontab_reports.send_blitz_report.start()
    # asyncio.get_event_loop().run_forever()
    try:
        await UserDB().create_tables()  # создаем таблицы для пользоватлей
    except:
        logging.error("Create users tables ERROR!")
    text_ru = f"#restart_bot\n\n" \
              f"🤖 Бот успешно перезагрузился."
    admins_list = await UserDBSelect().select_admins()
    for admin in admins_list:
        try:
            await bot.send_message(chat_id=admin, text=text_ru)
        except ChatNotFound as err:
            logging.warning(err)
        except BotBlocked as err:
            logging.warning(err)
    menu_commands = [
        BotCommand(command='/bot_menu',
                   description='меню Бота'),
        BotCommand(command='/cancel',
                   description='отменить действия')
    ]
    await bot.set_my_commands(menu_commands)


async def on_shutdown(_):
    text_ru = f"#shutdown_bot\n\n" \
           f"🤖 Бот упал..."
    admins_list = await UserDBSelect().select_admins()
    for admin in admins_list:
        try:
            await bot.send_message(chat_id=admin, text=text_ru)
        except ChatNotFound as err:
            logging.warning(err)
        except BotBlocked as err:
            logging.warning(err)


if __name__ == "__main__":
    executor.start_polling(dp, timeout=120, skip_updates=True,
                           on_startup=on_startup, on_shutdown=on_shutdown)
