import json
import time
from pprint import pprint

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

    def get_log_for_nm_ids(self, filter_nm_ids, eng_json_data: bool = False, step=1000) -> json:
        """Получение цен и скидок по совпадению с nmID"""
        url = self.url.format("filter")
        nm_ids = [*filter_nm_ids]
        nm_ids_list = {}
        # for start in range(0, len(filter_nm_ids), step):
        # nm_ids_part = filter_nm_ids[start: start + step]
        offset = 0
        limit = 1000
        while True:
            params = {
                "limit": limit,
                "offset": offset,
                # "filterNmID": nm_ids_part
            }
            response = requests.get(url, headers=self.headers, params=params)
            if "data" not in response.json() or response.status_code > 400:
                try:
                    for i in range(1,10):
                        response = requests.get(url, headers=self.headers, params=params)
                        if "data" in response.json():
                            break
                except Exception as e:
                    time.sleep(30)
                    print(e)
                    print(f"Ошибка на просмотре цены и скидки по артикулам. Попытка {i}")
            # if "data" not in response.json():
            #     continue
            # Если артикул не будет найден, то он его пропустит
            for card in response.json()["data"]["listGoods"]:
                if card["nmID"] in nm_ids:
                    if eng_json_data is False:
                        nm_ids_list[card["nmID"]] = {
                            "Цена на WB без скидки": card["sizes"][0]["price"],
                            "Скидка %": card["discount"]
                        }
                    nm_ids.remove(card["nmID"])

            if len(nm_ids) == 0:
                break
            else:
                offset += limit
        print("get_log_for_nm_ids")
        # pprint(nm_ids_list)
        return nm_ids_list

    def add_new_price_and_discount(self, data: list):
        url = self.post_url

        response = requests.post(url=url, headers=self.headers, json={"data": data})

        print("price and discount edit result:", response.json())
        time.sleep(10)
        if False is response.json()["error"]:
            return True
        else:
            return False

        # {'data': {'id': 38529453, 'alreadyExists': True}, 'error': False, 'errorText': 'Task already exists'}
        # {'data': {}, 'error': False, 'errorText': '', 'additionalErrors': {}}
