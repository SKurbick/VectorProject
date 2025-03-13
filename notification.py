import asyncio
import time
from settings import settings
import requests
import aiohttp


async def telegram(message):
    chat_id = settings.TELEGRAM_CHAT_ID
    token = settings.TELEGRAM_TOKEN
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=120)) as session:
        if len(message) < 4095:
            params = {"chat_id": f"{chat_id}", "text": message, "parse_mode": 'HTML'}
            async with session.get(url, params=params) as response:
                await response.read()
        else:
            for n, x in enumerate(range(0, len(message), 4095), 1):
                m = message[x:x + 4095]
                params = {"chat_id": f"{chat_id}", "text": m, "parse_mode": 'HTML'}
                async with session.get(url, params=params) as response:
                    await response.read()
                time.sleep(2)
