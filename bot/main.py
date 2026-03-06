"""Telegram bot with Mini App integration."""
import os
import asyncio
import logging
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, types
from aiogram.filters import CommandStart
from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    WebAppInfo,
)

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
WEBAPP_URL = os.getenv("WEBAPP_URL", "https://your-domain.com")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()


@dp.message(CommandStart())
async def cmd_start(message: types.Message):
    """Send welcome message with Mini App button."""
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="📋 Katalogni ochish",
                    web_app=WebAppInfo(url=WEBAPP_URL),
                )
            ]
        ]
    )

    await message.answer(
        "Assalomu alaykum! 👋\n\n"
        "Qurilish materiallari katalogiga xush kelibsiz.\n\n"
        "Quyidagi tugmani bosib, mahsulotlar ro'yxatini ko'ring, "
        "savatga qo'shing va buyurtma yarating.",
        reply_markup=keyboard,
    )


@dp.message()
async def fallback(message: types.Message):
    """Handle any other messages."""
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="📋 Katalogni ochish",
                    web_app=WebAppInfo(url=WEBAPP_URL),
                )
            ]
        ]
    )
    await message.answer(
        "Katalogni ochish uchun quyidagi tugmani bosing:",
        reply_markup=keyboard,
    )


async def main():
    logger.info("Bot started in polling mode...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
