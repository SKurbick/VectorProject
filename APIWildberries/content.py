import json
import time
from pprint import pprint
from utils import add_data_for_nm_ids, add_data_from_warehouse

import requests


class Content:
    """API Цены и товары"""
    pass


class ListOfCardsContent:
    """API Список товаров """

    def __init__(self, token):
        self.url = "https://content-api.wildberries.ru/content/v2/get/cards/{}"
        self.update_url = "https://content-api.wildberries.ru/content/v2/cards/{}"
        self.token = token
        self.headers = {
            "Authorization": self.token,
            'Content-Type': 'application/json'

        }

    def get_list_of_cards(self, nm_ids_list: list, limit: int = 1, eng_json_data: bool = False,
                          only_edits_data=False, add_data_in_db=True, account = None) -> json:
        """Получение всех карточек  по совпадению с nm_ids_list"""
        nm_ids_list_for_edit = [*nm_ids_list]
        url = self.url.format("list")
        card_result_for_match = {}
        nm_ids_data_for_database = {}
        data_for_warehouse = {}
        json_obj = {
            "settings": {
                "cursor": {
                    "limit": limit
                },
                "filter": {
                    "withPhoto": -1
                }
            }
        }
        count = 0
        while True:
            count += 1
            try:
                for i in range(10):
                    response = requests.post(url, headers=self.headers, json=json_obj)
                    if response.status_code >= 400:
                        print("[ERROR]", response.status_code, f"попытка {i}")
                        print("ожидание 1 минута")
                        time.sleep(60)
                    else:
                        break
            except Exception as e:
                print(e)

            request_wb = response.json()
            for card in request_wb["cards"]:
                if eng_json_data is False:
                    if card["nmID"] in nm_ids_list_for_edit:
                        # добавляем в словарь данные по карточке по ключу артикула на русском
                        card_result_for_match[card["nmID"]] = {
                            "Артикул": card["nmID"],
                            "Текущая\nДлина (см)": card["dimensions"]["length"],
                            "Текущая\nШирина (см)": card["dimensions"]["width"],
                            "Текущая\nВысота (см)": card["dimensions"]["height"],
                            "Предмет": card["subjectName"],
                            "Баркод": card["sizes"][0]["skus"][-1]
                        }
                        if only_edits_data is False:
                            photo = "НЕТ"
                            if "photos" in card:
                                photo = card["photos"][0]["tm"]

                            card_result_for_match[card["nmID"]].update({
                                "Артикул продавца": card["vendorCode"],
                                "Фото": photo,
                                # для таблицы будет использоваться последний баркод из списка
                                })
                        # добавляем данные по размерам в БД
                        nm_ids_data_for_database[str(card["nmID"])] = {
                            "sizes": card["sizes"]
                        }

                        if self.token not in data_for_warehouse.keys():
                            data_for_warehouse[account] = {}
                        # добавляем данные по skus с ключем кабинета и артикла
                        data_for_warehouse[account].update({card["nmID"]: {"skus": card["sizes"][0]["skus"]}})

                        nm_ids_list_for_edit.remove(card["nmID"])

                elif eng_json_data is True:
                    # добавляем в словарь данные по карточке по ключу артикула на английском
                    if card["nmID"] in nm_ids_list_for_edit:
                        card_result_for_match[card["nmID"]] = {
                            "nmID": card["nmID"],
                            "subjectName": card["subjectName"],
                            "vendorCode": card["vendorCode"],
                            "photo": card["photos"][0]["tm"],
                            "length": card["dimensions"]["length"],
                            "width": card["dimensions"]["width"],
                            "height": card["dimensions"]["height"]
                        }

                        # собираем данные по размерам в БД
                        nm_ids_data_for_database[str(card["nmID"])] = {
                            "sizes": card["sizes"]
                        }
                        if self.token not in data_for_warehouse.keys():
                            data_for_warehouse[self.token] = {}
                        # добавляем в БД данные по skus с ключем кабинета и артикла
                        data_for_warehouse[self.token].update({card["nmID"]: {"skus": card["sizes"][0]["skus"]}})

                        nm_ids_list_for_edit.remove(card["nmID"])

                    if len(nm_ids_list_for_edit) == 0:
                        break

            if request_wb["cursor"]["total"] < limit or len(nm_ids_list_for_edit) == 0:
                break

            else:
                update_data = {
                    "updatedAt": request_wb["cursor"]["updatedAt"],
                    "nmID": request_wb["cursor"]["nmID"]
                }

                json_obj["settings"]["cursor"].update(update_data)

        # добавляем данные по размерам в БД
        if add_data_in_db is True:
            add_data_for_nm_ids(nm_ids_data_for_database)
            add_data_from_warehouse(data_for_warehouse)
        return card_result_for_match

    def size_edit(self, data: list):

        url = self.update_url.format("update")

        response = requests.post(url=url, headers=self.headers, json=data)
        print("size edit result:", response.json())
        time.sleep(10)
        if False is response.json()["error"]:
            return True

        # {'data': {'id': 38529453, 'alreadyExists': True}, 'error': False, 'errorText': 'Task already exists'}
        # {'data': {}, 'error': False, 'errorText': '', 'additionalErrors': {}}
