from aiogram.dispatcher.filters.state import State, StatesGroup


class FSMAdmin(StatesGroup):
    msg_text = State()  # сюда записываем текст который будет отрпаляться всем
    send_msg_all = State()


class FSMMain(StatesGroup):
    start_bot = State()  # нажали кнопку - посмотреть пример отчета


class FSMCommands(StatesGroup):
    get_wallet_addr = State()


class FSMMonitoring(StatesGroup):
    wallet_menu = State()
    receive_wallet_addr = State()


class FSMReports(StatesGroup):
    reports_menu = State()
