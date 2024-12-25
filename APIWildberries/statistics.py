import asyncio

import aiohttp


class Statistic:
    def __init__(self, token):
        self.token = token
        self.url = "https://statistics-api.wildberries.ru/api/v1/supplier/"
        self.headers = {
            "Authorization": self.token,
            'Content-Type': 'application/json'
        }

    async def get_supplies_data(self, date):
        url = self.url + "incomes"
        params = {
            "dateFrom": date
        }
        print("get_supplies_data")
        for _ in range(10):
            async with aiohttp.ClientSession() as session:
                async with session.get(url=url, params=params, headers=self.headers) as response:
                    response_json = await response.json()
                    if response.status == 200:
                        print("get_supplies_data, 200")
                        return response_json
                    print("[ERROR] status code", response.status, response_json, "sleep 36 sec")
                    await asyncio.sleep(36)

    async def get_orders_data(self, date):
        url = self.url + "orders"
        params = {
            "dateFrom": date,
            "flag": 1
        }
        print("get_orders_data")
        for _ in range(10):
            async with aiohttp.ClientSession() as session:
                async with session.get(url=url, params=params, headers=self.headers) as response:
                    response_json = await response.json()
                    if response.status == 200:
                        print("get_supplies_data, 200")
                        return response_json
                    print("[ERROR] status code", response.status, response_json, "sleep 36 sec")
                    await asyncio.sleep(36)
