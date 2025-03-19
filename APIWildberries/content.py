import asyncio
import json
import time

import aiohttp
from aiohttp import ClientSession

from utils import add_data_for_nm_ids, add_data_from_warehouse, process_string, process_local_vendor_code

import requests

from logger import app_logger as logger


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
                          only_edits_data=False, add_data_in_db=True, account=None) -> json:
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
                        logger.info(f"[ERROR]  {response.status_code} попытка {i}")
                        logger.info("ожидание 1 минута")
                        time.sleep(60)
                    else:
                        break
            except Exception as e:
                time.sleep(60)
                logger.info(e)
                continue
            request_wb = response.json()
            for card in request_wb["cards"]:
                if eng_json_data is False and card["nmID"] in nm_ids_list_for_edit:
                    card_result_for_match[card["nmID"]] = {
                        "Артикул": card["nmID"],
                        "Текущая\nДлина (см)": card["dimensions"]["length"],
                        "Текущая\nШирина (см)": card["dimensions"]["width"],
                        "Текущая\nВысота (см)": card["dimensions"]["height"],
                        "Предмет": card["subjectName"],
                        "Баркод": card["sizes"][0]["skus"][-1],
                        "wild": process_string(card["vendorCode"]),
                        "vendor_code": card["vendorCode"],
                        "account": account
                    }
                    # if only_edits_data is False:
                    photo = "НЕТ"
                    if "photos" in card:
                        photo = card["photos"][0]["tm"]

                    card_result_for_match[card["nmID"]].update({
                        "Фото": photo,
                        # для таблицы будет использоваться последний баркод из списка
                    })
                    # добавляем данные по размерам в БД
                    nm_ids_data_for_database[str(card["nmID"])] = {
                        "sizes": card["sizes"],
                        "vendorCode": card["vendorCode"],
                    }

                    # добавляем данные по skus с ключом кабинета и артикула
                    if account not in data_for_warehouse.keys():
                        data_for_warehouse[account] = {}
                    data_for_warehouse[account].update({str(card["nmID"]): {"skus": card["sizes"][0]["skus"]}})

                    nm_ids_list_for_edit.remove(card["nmID"])

                if not nm_ids_list_for_edit:
                    break

            if request_wb["cursor"]["total"] < limit or not nm_ids_list_for_edit:
                logger.info(f"total: {request_wb['cursor']['total']}")
                break

            else:
                update_data = {
                    "updatedAt": request_wb["cursor"]["updatedAt"],
                    "nmID": request_wb["cursor"]["nmID"]
                }

                json_obj["settings"]["cursor"].update(update_data)

        # добавляем данные по размерам и баркодам в БД
        if add_data_in_db is True:
            add_data_for_nm_ids(nm_ids_data_for_database)
            add_data_from_warehouse(data_for_warehouse)
        logger.info("get_list_of_cards")
        # pprint(card_result_for_match)
        if nm_ids_list_for_edit:
            logger.info("if len(nm_ids_list_for_edit) > 0:")
            logger.info(f"нет карточек по этим артикулам в кабинете {account}: {nm_ids_list_for_edit}")
            if only_edits_data is False:
                logger.info("if only_edits_data is False:")
                for nm_id in nm_ids_list_for_edit:
                    card_result_for_match[nm_id] = {
                        "Артикул": nm_id,
                        "Текущая\nДлина (см)": "не найдено",
                        "Текущая\nШирина (см)": "не найдено",
                        "Текущая\nВысота (см)": "не найдено",
                        "Предмет": "не найдено",
                        "Баркод": "не найдено",
                        "wild": "не найдено",
                        "vendor_code": "не найдено",
                        "account": account,
                        "Фото": "НЕТ",
                    }
        # print(card_result_for_match)
        return card_result_for_match

    def size_edit(self, data: list):

        url = self.update_url.format("update")

        response = requests.post(url=url, headers=self.headers, json=data)
        logger.info(f"size edit result: {response.json()}")
        time.sleep(2)
        if False is response.json()["error"]:
            return True

        # {'data': {'id': 38529453, 'alreadyExists': True}, 'error': False, 'errorText': 'Task already exists'}
        # {'data': {}, 'error': False, 'errorText': '', 'additionalErrors': {}}

    async def get_list_of_cards_async(self, nm_ids_list: list, limit: int = 100,
                                      account=None) -> json:
        """Получение всех карточек  по совпадению с nm_ids_list"""
        nm_ids_list_for_edit = [*nm_ids_list]
        url = self.url.format("list")
        card_result_for_match = {}
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
        # данные для пагинации списков с карточками
        total = 1
        updated_at = None
        cursor_nm_id = None
        while True:
            for i in range(10):
                try:
                    async with ClientSession() as session:
                        async with session.post(url, headers=self.headers, json=json_obj) as response:
                            response_result = await response.json()
                            # переопределяем количество предоставленное в запросе
                            total = response_result["cursor"]["total"]
                            if total == 0:
                                break
                            updated_at = response_result["cursor"]["updatedAt"]
                            cursor_nm_id = response_result["cursor"]["nmID"]
                            for card in response_result["cards"]:
                                if card["nmID"] in nm_ids_list_for_edit:
                                    # добавляем в словарь данные по карточке по ключу артикула на русском
                                    card_result_for_match[card["nmID"]] = {
                                        "article_id": card["nmID"],
                                        "length": card["dimensions"]["length"],
                                        "width": card["dimensions"]["width"],
                                        "height": card["dimensions"]["height"],
                                        "subject_name": card["subjectName"],
                                        "barcode": card["sizes"][0]["skus"][-1],
                                        "local_vendor_code": process_local_vendor_code(card["vendorCode"]),
                                        "vendor_code": card["vendorCode"],
                                        "account": account,
                                        "skus": card["sizes"][0]["skus"]
                                    }
                                    photo = "НЕТ"
                                    if "photos" in card:
                                        photo = card["photos"][0]["tm"]
                                    card_result_for_match[card["nmID"]].update({
                                        "photo_link": photo,
                                    })
                                    # удаление артикула найденного из полученного списка
                                    nm_ids_list_for_edit.remove(card["nmID"])
                                # остановить поиск если все артикулы в списке для поиска закончены
                                if len(nm_ids_list_for_edit) == 0:
                                    break
                    # что бы прервать range
                    break
                except aiohttp.ClientError as e:
                    logger.warning(f"Exception {e} sleep 63 sec")
                    await asyncio.sleep(63)
                    continue
                except Exception as e:
                    logger.warning(f"Exception {e} {account} sleep 36 sec")
                    logger.warning(f"response {response_result}")
                    await asyncio.sleep(36)

            if total < limit or len(nm_ids_list_for_edit) == 0:
                logger.info(f"account: {account} total: {total}")
                break
            else:
                update_data = {
                    "updatedAt": updated_at,
                    "nmID": cursor_nm_id
                }
                json_obj["settings"]["cursor"].update(update_data)
        logger.info(f"account: {account} len(card_result_for_match): {len(card_result_for_match)}")
        logger.info("Невалидные артикулы в запросе:")
        logger.info(f"nm_ids_list_for_edit : {nm_ids_list_for_edit}")
        return card_result_for_match
