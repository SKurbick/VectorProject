from settings import settings
import aiohttp


async def telegram(message):
    chat_id = settings.TELEGRAM_CHAT_ID
    token = settings.TELEGRAM_TOKEN
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    params = {"chat_id": f"{chat_id}", "text": message, "parse_mode": 'HTML'}
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=120)) as session:
        async with session.get(url, params=params, ssl=False) as response:
            await response.text()
            print("telegram", response)
