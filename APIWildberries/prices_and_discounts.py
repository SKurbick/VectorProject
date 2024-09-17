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
        nm_ids_list = {}
        for start in range(0, len(filter_nm_ids), step):
            nm_ids_part = filter_nm_ids[start: start + step]
            params = {
                "limit": 1000,
                "offset": self.offset,
                "filterNmID": nm_ids_part
            }
            response = requests.get(url, headers=self.headers, params=params)

            if "data" not in response.json() or response.json()["data"]["listGoods"] is None:
                # Если артикул не будет найден, то он его пропустит
                continue
            for card in response.json()["data"]["listGoods"]:
                # response_result = response.json()["data"]["listGoods"][0]

                if eng_json_data is False:

                    nm_ids_list[card["nmID"]] = {
                        "Цена на WB без скидки": card["sizes"][0]["price"],
                        "Скидка %": card["discount"]
                    }
                elif eng_json_data is True:
                    nm_ids_list[card["nmID"]] = {
                        "price": card["sizes"][0]["price"],
                        "discount": card["discount"]
                    }

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
