import os

from pathlib import Path
from dotenv import load_dotenv

from aiogram import Bot
from aiogram.dispatcher import Dispatcher
from aiogram.contrib.fsm_storage.memory import MemoryStorage


this_dir_path = os.path.dirname(os.path.realpath(__file__))
load_dotenv(Path(this_dir_path, '.env'))

BOT_TOKEN = os.getenv('BOT_TOKEN')

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot=bot, storage=MemoryStorage())
