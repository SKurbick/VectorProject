import asyncio
import datetime
import time
from logger import app_logger as logger

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
                        async with session.post(url=url, headers=self.headers, json=json_data, timeout=250) as response:
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
                                logger.info(
                                    f"account:{account}|Превышен лимит запросов:error {response.status} повторение {i}")
                                await asyncio.sleep(63)

                except aiohttp.ClientError as e:
                    logger.exception(f"aiohttp.ClientError: {e} повторение{i}")
                    await asyncio.sleep(63)

                except asyncio.TimeoutError:
                    logger.info(f"account:{account}|Превышен лимит запросов:error asyncio.TimeoutError повторение {i}")
                    await asyncio.sleep(63)

        if orders_db_ad:
            # добавляем в БД данные по количеству заказов за определенный день
            add_orders_data_in_database(orders_data_for_database)

        # pprint(for_db_all_data)
        return {"result_data": result_data, "all_data": for_db_all_data}

    async def get_last_week_revenue(self, nm_ids: list[int], week_count: int) -> dict:
        "V2"
        weeks = get_last_weeks_dates(last_week_count=week_count)
        url = "https://seller-analytics-api.wildberries.ru/api/analytics/v3/sales-funnel/products"
        result_data = {}

        async with aiohttp.ClientSession() as session:
            for key_dates, dates in weeks.items():
                offset = 0
                limit = 1000

                while True:
                    json_data = {
                        "selectedPeriod": {
                            "start": dates["Start"][:10],
                            "end": dates["End"][:10]
                        },
                        "nmIds": nm_ids,
                        "timezone": "Europe/Moscow",
                        "orderBy": {
                            "field": "orderSum",
                            "mode": "asc"
                        },
                        "limit": limit,
                        "offset": offset
                    }

                    response_data = None
                    for attempt in range(9):
                        try:
                            async with session.post(url, headers=self.headers, json=json_data) as response:

                                # Обработка 429 — слишком много запросов
                                if response.status == 429:
                                    logger.warning(f"Rate limit (429). Attempt {attempt + 1}/9. Waiting 60 seconds...")
                                    await asyncio.sleep(60)
                                    continue

                                response_data = await response.json()

                                for product in response_data.get("data", {}).get("products", []):
                                    nm_id = product["product"]["nmId"]
                                    orders_sum = product["statistic"]["selected"]["orderSum"]

                                    if nm_id not in result_data:
                                        result_data[nm_id] = {}
                                    result_data[nm_id][key_dates] = orders_sum
                                break

                        except (aiohttp.ClientError, Exception) as e:
                            logger.exception(f"Attempt {attempt + 1}/9 failed: {e}")
                            await asyncio.sleep(5)

                    if response_data is None:
                        logger.error(f"All 9 attempts failed for period {key_dates}")
                        break

                    products_count = len(response_data.get("data", {}).get("products", []))
                    if products_count < limit:
                        break

                    offset += limit

        return result_data


class AnalyticsWarehouseLimits:
    def __init__(self, token):
        self.url = "https://seller-analytics-api.wildberries.ru/api/v1/warehouse_remains"
        self.headers = {
            "Authorization": token,
            'Content-Type': 'application/json'
        }

    async def create_report(self):
        logger.info("create_report")
        """Создает и возвращает taskId для остатков по баркодам"""
        result = None
        url = self.url
        params = {
            "groupByBarcode": True,
            "groupByNm": True
        }
        for _ in range(10):
            try:
                response = requests.get(url=url, headers=self.headers, params=params)
                if response.status_code == 200:
                    result = response.json()["data"]["taskId"]
                    logger.info(f"taskId: {result}")
                    break

                logger.info(f"create_report {response.status_code} sleep 63 sec")
                await asyncio.sleep(63)

            except (requests.exceptions.ConnectionError, requests.exceptions.HTTPError) as e:
                logger.exception(f"create_report [ERROR] {e} sleep 63 sec")
                await asyncio.sleep(63)

        return result

    async def check_data_by_task_id(self, task_id):
        await asyncio.sleep(10)

        url = f"https://seller-analytics-api.wildberries.ru/api/v1/warehouse_remains/tasks/{task_id}/download"
        result = {}
        for _ in range(10):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url=url, headers=self.headers, params=task_id) as response:
                        # response = requests.get(url=url, headers=self.headers, params=task_id)
                        # print(response.status_code)
                        response_json = await response.json()

                        logger.info(response.status)
                        if response.status == 200:
                            result = response_json
                            break
                        logger.info(f"{response.status} time sleep 36")
                        logger.info(response_json)
                        await asyncio.sleep(36)
            # except (requests.exceptions.ConnectionError, requests.exceptions.HTTPError) as e:
            except (aiohttp.ClientError, aiohttp.ClientConnectionError, aiohttp.ClientResponseError) as e:
                logger.exception(e)
                await asyncio.sleep(63)
            # except requests.exceptions.JSONDecodeError as e:
            except Exception as e:
                logger.exception(f"[ERROR] {e}")
                break
        return result
