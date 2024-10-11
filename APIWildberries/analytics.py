import asyncio
import datetime
import time
from pprint import pprint

import aiohttp
import requests

from utils import get_last_weeks_dates, add_orders_data_in_database


class Analytics:
    pass


class AnalyticsNMReport:
    def __init__(self, token):
        self.token = token
        self.url = "https://seller-analytics-api.wildberries.ru/api/v2/nm-report/{}"
        self.headers = {
            "Authorization": self.token,
            'Content-Type': 'application/json'
        }

    async def get_last_days_revenue(self, nm_ids: list, begin_date: datetime, end_date: datetime, step: int = 20,
                                    account=None, orders_db_ad=False):
        """По методу есть ограничения на 3 запроса в минуту и в 20 nmID за запрос.
            По умолчанию передаются даты последнего (вчерашнего) дня
        """
        url = self.url.format("detail/history")
        result_data = {}
        orders_data_for_database = {}
        for_db_all_data = {}
        for start in range(0, len(nm_ids), step):
            nm_ids_part = nm_ids[start: start + step]

            json_data = {
                "nmIDs": nm_ids_part,
                "period": {
                    "begin": str(begin_date),
                    "end": str(end_date)
                },
                "timezone": "Europe/Moscow",
                "aggregationLevel": "day"
            }

            for i in range(10):
                try:
                    async with aiohttp.ClientSession() as session:
                        async with session.post(url=url, headers=self.headers, json=json_data) as response:
                            if response.status == 200:
                                response_result = await response.json()
                                for data in response_result["data"]:
                                    nm_id_from_data = data["nmID"]

                                    for history_data in data["history"]:
                                        history_date = history_data['dt']
                                        if history_data['dt'] not in for_db_all_data.keys():
                                            for_db_all_data.update({history_date: {}})
                                        for_db_all_data[history_date].update({nm_id_from_data: history_data})

                                    revenue_by_dates = {}
                                    orders_by_dates = {}
                                    for nm_id_history in data["history"]:
                                        date_object = datetime.datetime.strptime(nm_id_history["dt"], "%Y-%m-%d")
                                        output_date = date_object.strftime("%d-%m-%Y")
                                        output_date_by_orders_db = date_object.strftime("%d.%m")
                                        revenue_by_dates[output_date] = nm_id_history["ordersSumRub"]
                                        orders_by_dates[output_date_by_orders_db] = nm_id_history["ordersCount"]
                                    result_data[nm_id_from_data] = revenue_by_dates
                                    orders_data_for_database[nm_id_from_data] = orders_by_dates
                                break
                            else:
                                print(f"account:{account}|Превышен лимит запросов: {response.status}")
                                await asyncio.sleep(63)
                except aiohttp.ClientError as e:
                    print(f"Превышен лимит запросов: {e}")
                    await asyncio.sleep(63)

        if orders_db_ad:
            # добавляем в БД данные по количеству заказов за определенный день
            add_orders_data_in_database(orders_data_for_database)

        # pprint(for_db_all_data)
        return {"result_data": result_data, "all_data": for_db_all_data}

    async def get_last_week_revenue(self, nm_ids, week_count):
        weeks = get_last_weeks_dates(last_week_count=week_count)
        url = self.url.format("detail")
        result_data = {}
        page = 1
        for key_dates, dates in weeks.items():
            while True:
                json_data = {
                    "nmIDs": nm_ids,
                    "timezone": "Europe/Moscow",
                    "period": {
                        "begin": dates["Start"],
                        "end": dates["End"]
                    },
                    "orderBy": {
                        "field": "ordersSumRub",
                        "mode": "asc"
                    },
                    "page": page
                }
                for _ in range(10):
                    try:
                        async with aiohttp.ClientSession() as session:
                            async with session.post(url=url, headers=self.headers, json=json_data) as response:
                                response_data = await response.json()
                                for card in response_data["data"]["cards"]:
                                    if card["nmID"] not in result_data.keys():
                                        result_data[card["nmID"]] = {}
                                    result_data[card["nmID"]].update({
                                        key_dates: card["statistics"]["selectedPeriod"]["ordersSumRub"]
                                    })
                                break
                    except (aiohttp.ClientError, Exception) as e:
                        print(e)

                if response_data["data"]["isNextPage"] is False:
                    # если нет следующей страницы, цикл должен остановиться
                    page = 1
                    break

                page += 1

        return result_data


class AnalyticsWarehouseLimits:
    def __init__(self, token):
        self.url = "https://seller-analytics-api.wildberries.ru/api/v1/warehouse_remains"
        self.headers = {
            "Authorization": token,
            'Content-Type': 'application/json'
        }

    def create_report(self):
        """Создает и возвращает taskId для остатков по баркодам"""
        result = None
        url = self.url
        params = {
            "groupByBarcode": True
        }
        for _ in range(10):
            try:
                response = requests.get(url=url, headers=self.headers, params=params)
                if response.status_code == 200:
                    result = response.json()["data"]["taskId"]
                    break

                print("create_report", response.status_code, "sleep 63 sec")
                time.sleep(63)

            except (requests.exceptions.ConnectionError, requests.exceptions.HTTPError) as e:
                print(e)
                time.sleep(63)

        return result

    def check_data_by_task_id(self, task_id):
        time.sleep(10)

        url = f"https://seller-analytics-api.wildberries.ru/api/v1/warehouse_remains/tasks/{task_id}/download"
        result = {}
        for _ in range(10):
            try:
                response = requests.get(url=url, headers=self.headers, params=task_id)
                print(response.status_code)

                if response.status_code == 200:
                    result = response.json()
                    break
                print(response.status_code)
                print(response.json())
                time.sleep(63)
            except (requests.exceptions.ConnectionError, requests.exceptions.HTTPError) as e:
                print(e)
                time.sleep(63)

        return result
