from aiogram.dispatcher.filters.state import State, StatesGroup


class FSMMain(StatesGroup):
    start_bot = State()  # нажали кнопку - посмотреть пример отчета


class FSMCommands(StatesGroup):
    get_wallet_addr = State()