import asyncio
import logging
from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage
from handlers import router

from config import API_KEY

logging.basicConfig(level=logging.INFO)


async def main():
    bot = Bot(token=API_KEY)
    dp = Dispatcher(storage=MemoryStorage())

    dp.include_router(router)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
