import asyncio
import json
import time
from logger import app_logger as logger

import aiohttp
import requests


class PricesAndDiscounts:
    """API Цены и товары"""
    pass


class ListOfGoodsPricesAndDiscounts:
    """API Список товаров """

    def __init__(self, token, limit: int = 1, offset: int = 0):
        self.limit = limit
        self.offset = offset
        self.token = token
        self.url = "https://discounts-prices-api.wildberries.ru/api/v2/list/goods/{}"
        self.post_url = "https://discounts-prices-api.wildberries.ru/api/v2/upload/task"

        self.headers = {
            "Authorization": self.token,
            'Content-Type': 'application/json'
        }

    def get_log_for_nm_ids(self, filter_nm_ids, eng_json_data: bool = False) -> json:
        """Получение цен и скидок по совпадению с nmID"""
        url = self.url.format("filter")
        nm_ids = [*filter_nm_ids]
        nm_ids_list = {}
        logger.info("попали в функцию get_log_for_nm_ids")
        logger.info(f"filter_nm_ids len: {len(filter_nm_ids)}")
        offset = 0
        limit = 1000
        while True:
            params = {
                "limit": limit,
                "offset": offset,
            }
            response = requests.get(url, headers=self.headers, params=params)
            if "data" not in response.json() or response.status_code > 400:
                for i in range(1, 10):
                    try:
                        response = requests.get(url, headers=self.headers, params=params)
                        if "data" in response.json():
                            break
                    except Exception as e:
                        time.sleep(30)
                        logger.exception(e)
                        logger.error(f"Ошибка на просмотре цены и скидки по артикулам. Попытка {i}")

            try:
                for card in response.json()["data"]["listGoods"]:
                    if card["nmID"] in nm_ids:
                        if eng_json_data is False:
                            nm_ids_list[card["nmID"]] = {
                                "Цена на WB без скидки": card["sizes"][0]["price"],
                                "Скидка %": card["discount"]
                            }
                        nm_ids.remove(card["nmID"])
            except Exception as e:
                logger.exception(e)
                break

            if len(nm_ids) == 0:
                break
            else:
                offset += limit
        logger.info("НЕВАЛИДНЫЕ АРТИКУЛЫ get_log_for_nm_ids")
        logger.info(nm_ids)
        return nm_ids_list

    async def get_log_for_nm_ids_async(self, filter_nm_ids, account=None) -> dict:
        """Получение цен и скидок по совпадению с nmID"""
        url = self.url.format("filter")
        nm_ids = [*filter_nm_ids]
        nm_ids_list = {}
        logger.info("В функции get_log_for_nm_ids")
        offset = 0
        limit = 1000
        while True:
            params = {
                "limit": limit,
                "offset": offset,
            }

            for i in range(1, 10):
                try:
                    async with aiohttp.ClientSession() as session:
                        async with session.get(url, headers=self.headers, params=params, timeout=60) as response:
                            response_result = await response.json()
                            if "data" in response_result:
                                if response_result['data'] is not None:
                                    for card in response_result["data"]["listGoods"]:
                                        if card["nmID"] in nm_ids:
                                            nm_ids_list[card["nmID"]] = {
                                                "Цена на WB без скидки": card["sizes"][0]["price"],
                                                "Скидка %": card["discount"]
                                            }
                                            nm_ids.remove(card["nmID"])
                                    break
                                else:
                                    break
                            elif len(response_result) == 0:
                                break
                            elif response.status == 429:
                                logger.info(nm_ids)
                                logger.info(f"попытка: {i} sleep 10 sec")
                                await asyncio.sleep(10)
                                continue
                            else:
                                break
                except (aiohttp.ClientError, aiohttp.ClientResponseError, aiohttp.ConnectionTimeoutError,
                        asyncio.TimeoutError) as e:
                    logger.error(f"[ERROR] func -get_log_for_nm_ids_async {e} sleep 36 sec")
                    await asyncio.sleep(36)

            logger.info("Дошел до условия прерывания бесконечного цикла")
            logger.info(f"offset {offset}")
            if len(nm_ids) == 0 or i == 9 or "data" not in response_result or response_result['data'] is None or \
                    response_result["data"]["listGoods"] is None or len(response_result["data"]["listGoods"]) == 0:
                logger.info("прерывание бесконечного цикла")
                # для того что бы прервать бесконечный цикл
                break
            else:  # пагинация
                offset += limit
        if len(nm_ids) != 0:
            logger.info(f"в запросе просмотра цен есть невалидные артикулы -> {account}: {nm_ids}")
        return nm_ids_list

    def add_new_price_and_discount(self, data: list, step=1000):
        url = self.post_url
        for start in range(0, len(data), step):
            butch_data = data[start: start + step]
            for _ in range(10):
                try:
                    response = requests.post(url=url, headers=self.headers, json={"data": butch_data})
                    logger.info(f"Артикулы на изменение цены: {butch_data}")
                    logger.info(f"price and discount edit result: {response.json()}")
                    time.sleep(2)
                    if (response.status_code in (200, 208) or response.json()['errorText'] in
                            ("Task already exists", "No goods for process", "The specified prices and discounts are already set")):
                        break

                except (Exception, requests.exceptions.ConnectionError, requests.exceptions.HTTPError) as e:
                    logger.exception(e)
                    time.sleep(63)

    # def add_new_price_and_discount_async(self, data: list, step=1000):
    #     url = self.post_url
    #     for start in range(0, len(data), step):
    #         butch_data = data[start: start + step]
    #         for _ in range(10):
    #             try:
    #                 async with aiohttp.ClientSession() as session:
    #                     async with session.post(url=url, headers=self.headers, json={"data": butch_data}) as response:
    #                     if (response.status in (200, 208) or response.json()['errorText'] in
    #                             ("Task already exists", "No goods for process")):
    #                         break
    #
    #             except:
    # response = requests.post(url=url, headers=self.headers, json={"data": butch_data})
    #     print("Артикулы на изменение цены:", butch_data)
    #     print("price and discount edit result:", response.json())
    #     time.sleep(2)
    #     if (response.status_code in (200, 208) or response.json()['errorText'] in
    #             ("Task already exists", "No goods for process")):
    #         break
    #
    # except (Exception, requests.exceptions.ConnectionError, requests.exceptions.HTTPError) as e:
    #     print(e)
    #     time.sleep(63)
