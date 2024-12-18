import asyncio
import aiohttp


class Statistic:
    def __init__(self, token):
        self.token = token
        self.url = "https://statistics-api.wildberries.ru/api/v1/supplier/incomes"
        self.headers = {
            "Authorization": self.token,
            'Content-Type': 'application/json'
        }

    async def get_supplies_data(self, date):
        params = {
            "dateFrom": date
        }
        print("get_supplies_data")
        for _ in range(10):
            async with aiohttp.ClientSession() as session:
                async with session.get(url=self.url, params=params, headers=self.headers) as response:
                    response_json = await response.json()
                    if response.status == 200:
                        print("get_supplies_data, 200")
                        return response_json
                    print("[ERROR] status code",response.status, response_json, "sleep 36 sec")
                    await asyncio.sleep(36)