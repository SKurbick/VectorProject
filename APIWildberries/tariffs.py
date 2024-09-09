import datetime
import time
from pprint import pprint

import requests


class Tariffs:
    pass


class CommissionTariffs:
    def __init__(self, token):
        self.url = "https://common-api.wildberries.ru/api/v1/tariffs/{}"
        self.headers = {
            "Authorization": token,
            'Content-Type': 'application/json'
        }

    def get_commission_on_subject(self, subject_names) -> dict:
        url = self.url.format("commission")

        result_commission_data = {}
        response = requests.get(url, headers=self.headers)
        if response.status_code != 200:
            print(response.json())
        if response.status_code == 429:
            print("превысил лимит запросов, ограничение запроса в 1 минуту. Сервис упал в сон на 1 минуту")
            time.sleep(60)
            response = requests.get(url, headers=self.headers)

        for subject_name in subject_names:
            for i in response.json()["report"]:
                if i["subjectName"] == subject_name:
                    result_commission_data[subject_name] = i['kgvpMarketplace']
                    break
        return result_commission_data

    def get_tariffs_box_from_marketplace(self, date: datetime.date.today() = datetime.date.today()) -> dict or None:
        url = self.url.format("box")

        params = {
            "date": date
        }

        response = requests.get(url=url, headers=self.headers, params=params)
        # pprint(response.json())
        for warehouse_data in response.json()["response"]["data"]["warehouseList"]:
            if warehouse_data["warehouseName"] == "Маркетплейс":
                current_tariffs_data = {'boxDeliveryBase': warehouse_data['boxDeliveryBase'],
                                        'boxDeliveryLiter': warehouse_data['boxDeliveryLiter']}

                return current_tariffs_data  # 'boxDeliveryBase': '...', 'boxDeliveryLiter': '...'

        else:
            return None
