import datetime
import json
import time
from collections import ChainMap

from database.postgresql.repositories.article import ArticleTable
from logger import app_logger as logger
import re
import math


async def create_valid_data_from_db(data):
    result_data = {}
    for record in data:
        article = record['article_id']
        quantity = record['quantity']
        supply_qty = record['supply_qty']
        federal_district = record['federal_district']

        if article in result_data:
            result_data[article].update({federal_district: {"quantity": quantity, "supply_qty": supply_qty}})
        else:
            result_data[article] = {federal_district: {"quantity": quantity, "supply_qty": supply_qty}}

    return result_data


def merge_dicts(d1, d2):
    result = {}
    for key in d1.keys() | d2.keys():
        result[key] = dict(ChainMap({}, d1.get(key, {}), d2.get(key, {})))
    return result


def new_merge_dicts(d1, d2):
    result = {}

    for nm_id, data in d1.items():
        result[nm_id] = {
            **data, **d2[nm_id]
        }

    return result


def calculate_sum_for_logistic(for_one_liter: float,
                               next_liters: float,
                               length,
                               width: int,
                               height: int):
    volume_good = (length * width * height) / 1000
    if volume_good > 1.0:
        only_next_liters = volume_good - 1
        result_sum = only_next_liters * next_liters + for_one_liter
        return result_sum
    else:
        return for_one_liter


def add_orders_data(revenue_data: dict):  # добавление или обновление выручки по артикулам и дням в бд
    with open('database.json', 'r+') as file:
        # Загрузите данные из файла
        database = json.load(file)
        for nm_id in revenue_data:
            str_nm_id = str(nm_id)
            if str_nm_id in database["revenue_result"]:  # если артикул есть, то данные будут обновленны или дополненны
                database["revenue_result"][str_nm_id].update(revenue_data[nm_id])
            else:  # если артикула нет, то будет добавлен с актуальными данными
                database["revenue_result"].update({str_nm_id: revenue_data[nm_id]})
        file.seek(0)
        json.dump(database, file, indent=4, ensure_ascii=False)
        file.truncate()


def get_data_for_nm_ids():
    with open("database.json", "r", encoding='utf-8') as file:
        nm_ids = json.load(file)
    return nm_ids["nm_ids_data"]


def add_nm_ids_in_db(account, new_nm_ids):
    """Добавление артикулов в БД"""
    with open('database.json', 'r+') as file:
        # Загрузите данные из файла
        data = json.load(file)
        if account not in data['account_nm_ids']:
            data['account_nm_ids'].update({account: []})
        data['account_nm_ids'][account].extend(new_nm_ids)
        file.seek(0)
        json.dump(data, file, indent=4, ensure_ascii=False)
        file.truncate()


def add_data_for_nm_ids(nm_ids_data: dict):
    with open('database.json', 'r+') as file:
        # Загрузите данные из файла
        database = json.load(file)
        # добавляет данные по артикулу в БД
        for nm_id in nm_ids_data:
            if nm_id in database["nm_ids_data"].keys():
                database["nm_ids_data"][nm_id].update(nm_ids_data[nm_id])
            else:
                # print("решил что nmid нет в бд")
                database["nm_ids_data"].update({nm_id: nm_ids_data[nm_id]})
        file.seek(0)
        json.dump(database, file, indent=4, ensure_ascii=False)
        file.truncate()


def total_revenue_for_week(date_dict: dict, revenue_result):
    monday_key = datetime.datetime.strptime(date_dict["second_last_monday"], "%d-%m-%Y").strftime("%d.%m")
    sunday_key = datetime.datetime.strptime(date_dict["last_sunday"], "%d-%m-%Y").strftime("%d.%m")
    revenue_date_key = f"{monday_key}-{sunday_key}"
    logger.info(revenue_date_key)
    logger.info(monday_key)
    # dates_key =
    nm_ids_total = {}
    for nm_id, revenue_data in revenue_result.items():
        nm_ids_total[nm_id] = 0
        monday = date_dict["second_last_monday"]
        sunday = date_dict["last_sunday"]
        search_date = monday
        while True:
            date_object = datetime.datetime.strptime(search_date, '%d-%m-%Y')

            if search_date not in revenue_data:
                if search_date == sunday:
                    break
                date_object += datetime.timedelta(days=1)
                search_date = date_object.strftime('%d-%m-%Y')
                continue

            nm_ids_total[nm_id] += revenue_data[search_date]  # прибавляем к последнему значению

            date_object += datetime.timedelta(days=1)
            search_date = date_object.strftime('%d-%m-%Y')

            if search_date == sunday:
                nm_ids_total[nm_id] += revenue_data[search_date]

                break
    return {revenue_date_key: nm_ids_total}


async def validate_data(nm_ids_db_data, data: dict):
    """Редактирует и возвращает валидные данные для API WB"""
    result_valid_data = {}
    for nm_id, edit_data in data.items():
        if nm_id.isdigit():
            nm_ids_data = {}
            if "price_discount" in edit_data:
                if edit_data["price_discount"]["Установить новую скидку %"].isdigit():
                    nm_ids_data.setdefault("price_discount", {})["discount"] = int(edit_data["price_discount"][
                                                                                       "Установить новую скидку %"])

                if edit_data["price_discount"]["Установить новую цену"].isdigit():
                    nm_ids_data.setdefault("price_discount", {})["price"] = int(edit_data["price_discount"][
                                                                                    "Установить новую цену"])
            if "dimensions" in edit_data and (
                    edit_data["dimensions"]['Новая\nВысота (см)'].isdigit() and edit_data["dimensions"][
                'Новая\nДлина (см)'].isdigit() and edit_data["dimensions"]['Новая\nШирина (см)'].isdigit()):
                nm_ids_data.setdefault("dimensions", {})["height"] = int(edit_data["dimensions"][
                                                                             'Новая\nВысота (см)'])
                nm_ids_data.setdefault("dimensions", {})["length"] = int(edit_data["dimensions"][
                                                                             'Новая\nДлина (см)'])
                nm_ids_data.setdefault("dimensions", {})["width"] = int(edit_data["dimensions"][
                                                                            'Новая\nШирина (см)'])

            if nm_ids_data:
                nm_ids_data["vendorCode"] = edit_data['wild']
                if "price_discount" in nm_ids_data:
                    nm_ids_data['net_profit'] = int(edit_data['Чистая прибыль 1ед.'].replace(" ", "").replace("₽", ""))
                if "dimensions" in nm_ids_data:
                    nm_ids_data["sizes"] = nm_ids_db_data[nm_id]["sizes"]

                result_valid_data[int(nm_id)] = nm_ids_data

    return result_valid_data


def get_nm_ids_in_db(account):
    with open("database.json", "r", encoding='utf-8') as file:
        nm_ids = json.load(file)
        if account not in nm_ids["account_nm_ids"]:
            return []
    return nm_ids["account_nm_ids"][account]


def get_warehouse_data():
    with open("database.json", "r", encoding='utf-8') as file:
        warehouse_data = json.load(file)

    return warehouse_data["warehouse_data"]


def add_data_from_warehouse(warehouse_data):
    with open('database.json', 'r+') as file:
        # Загрузите данные из файла
        database = json.load(file)
        for account, wh_data in warehouse_data.items():
            if account not in database["warehouse_data"].keys():
                database["warehouse_data"].update({account: {}})
            database["warehouse_data"][account].update(wh_data)
        file.seek(0)
        json.dump(database, file, indent=4, ensure_ascii=False)
        file.truncate()


def column_index_to_letter(index):
    letter = ''
    while index > 0:
        index -= 1
        letter = chr((index % 26) + 65) + letter
        index //= 26
    return letter


def get_last_weeks_dates(last_week_count=1):
    """Функция для получения срезов дат начало и конца недели не включая текущую.
     last_week_count: количество недель (указывая 1 - получишь последнюю)"""
    now = datetime.datetime.now()
    current_week_start = now - datetime.timedelta(days=now.weekday())

    result_dates = {}
    for i in range(1, last_week_count + 1):
        week_start = current_week_start - datetime.timedelta(weeks=i)
        week_end = week_start + datetime.timedelta(days=6)

        week_start = week_start.replace(hour=0, minute=0, second=0, microsecond=0)
        week_end = week_end.replace(hour=23, minute=59, second=59, microsecond=999999)

        date_key = f"{week_start.strftime('%m.%d')}-{week_end.strftime('%m.%d')}"
        result_dates[date_key] = {"Start": week_start.strftime('%Y-%m-%d %H:%M:%S'),
                                  "End": week_end.strftime('%Y-%m-%d %H:%M:%S')}

    return result_dates


def process_string(s):
    # Шаблон для извлечения "wild" и цифр
    wild_pattern = r'^wild(\d+).*$'
    word_pattern = r'^[a-zA-Z\s]+$'
    wild_match = re.match(wild_pattern, s)
    if wild_match:
        return f"wild{wild_match.group(1)}"
    word_match = re.match(word_pattern, s)
    if word_match:
        return s
    return s


def json_int_key_hook(json_dict):
    def convert_keys(d):
        new_dict = {}
        for k, v in d.items():
            if isinstance(v, dict):
                v = convert_keys(v)
            if isinstance(k, str) and k.isdigit():
                k = int(k)
            new_dict[k] = v
        return new_dict

    return convert_keys(json_dict)


def get_order_data_from_database() -> dict:
    with open('orders_data.json', 'r+') as file:
        # Загрузите данные из файла
        database = json.load(file, object_hook=json_int_key_hook)
    return database["nm_ids_orders_data"]


def add_orders_data_in_database(orders_data):
    with open('orders_data.json', 'r+') as file:
        # Загрузите данные из файла
        database = json.load(file)
        for nm_id, od in orders_data.items():
            str_nm_id = str(nm_id)
            if str_nm_id not in database["nm_ids_orders_data"].keys():
                database["nm_ids_orders_data"].update({str_nm_id: {}})
            database["nm_ids_orders_data"][str_nm_id].update(od)
        file.seek(0)
        json.dump(database, file, indent=4, ensure_ascii=False)
        file.truncate()


def subtract_percentage(number, percentage):
    return math.ceil(number * percentage / 100)


def can_be_int(value):
    try:
        int(value)  # Попытка преобразования
        return True
    except (ValueError, TypeError):
        return False


def process_local_vendor_code(s):
    # Шаблон для извлечения "wild" и цифр
    wild_pattern = r'^wild(\d+).*$'
    word_pattern = r'^[a-zA-Z\s]+$'
    wild_match = re.match(wild_pattern, s)
    if wild_match:
        return f"wild{wild_match.group(1)}"
    word_match = re.match(word_pattern, s)
    if word_match:
        return s
    return s
