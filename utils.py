import datetime
import json
import time
from collections import ChainMap
from pprint import pprint


def merge_dicts(d1, d2):
    result = {}
    for key in d1.keys() | d2.keys():
        result[key] = dict(ChainMap({}, d1.get(key, {}), d2.get(key, {})))
    return result


def calculate_sum_for_logistic(for_one_liter: int,
                               next_liters: int,
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
            if nm_id in database["revenue_result"]:  # если артикул есть, то данные будут обновленны или дополненны
                database["revenue_result"][nm_id].update(revenue_data[nm_id])
            else:  # если артикула нет, то будет добавлен с актуальными данными
                database["revenue_result"].update({nm_id: revenue_data[nm_id]})
        file.seek(0)
        json.dump(database, file, indent=4)
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
        json.dump(data, file, indent=4,ensure_ascii=False )
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
                print("решил что nmid нет в бд")

                database["nm_ids_data"].update({nm_id: nm_ids_data[nm_id]})
        file.seek(0)
        json.dump(database, file, indent=4, ensure_ascii=False)
        file.truncate()


def total_revenue_for_week(date_dict: dict, revenue_result):
    monday_key = datetime.datetime.strptime(date_dict["second_last_monday"], "%d-%m-%Y").strftime("%d.%m")
    sunday_key = datetime.datetime.strptime(date_dict["last_sunday"], "%d-%m-%Y").strftime("%d.%m")
    revenue_date_key = f"{monday_key}-{sunday_key}"
    print(revenue_date_key)
    print(monday_key)
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


def validate_data(data: dict):
    nm_ids_db_data = get_data_for_nm_ids()
    """Редактирует и возвращает валидные данные для API WB"""
    result_valid_data = {}
    for nm_id, edit_data in data.items():
        if nm_id.isdigit():
            nm_ids_data = {}
            if edit_data["price_discount"]["Установить новую скидку %"].isdigit():
                nm_ids_data.setdefault("price_discount", {})["discount"] = int(edit_data["price_discount"][
                                                                                   "Установить новую скидку %"])

            if edit_data["price_discount"]["Установить новую цену"].isdigit():
                nm_ids_data.setdefault("price_discount", {})["price"] = int(edit_data["price_discount"][
                                                                                "Установить новую цену"])

            if edit_data["dimensions"]['Новая\nВысота (см)'].isdigit() and edit_data["dimensions"][
                'Новая\nДлина (см)'].isdigit() and edit_data["dimensions"]['Новая\nШирина (см)'].isdigit():
                nm_ids_data.setdefault("dimensions", {})["height"] = int(edit_data["dimensions"][
                                                                             'Новая\nВысота (см)'])
                nm_ids_data.setdefault("dimensions", {})["length"] = int(edit_data["dimensions"][
                                                                             'Новая\nДлина (см)'])
                nm_ids_data.setdefault("dimensions", {})["width"] = int(edit_data["dimensions"][
                                                                            'Новая\nШирина (см)'])

            if len(nm_ids_data) > 0:
                nm_ids_data["vendorCode"] = edit_data['Артикул продавца']
                if "dimensions" in nm_ids_data:
                    nm_ids_data["sizes"] = nm_ids_db_data[nm_id]["sizes"]

                result_valid_data[int(nm_id)] = nm_ids_data
    pprint(result_valid_data)
    return result_valid_data


def get_nm_ids_in_db(account):
    with open("database.json", "r", encoding='utf-8') as file:
        nm_ids = json.load(file)
        if account not in nm_ids["account_nm_ids"]:
            return []
    return nm_ids["account_nm_ids"][account]




fake_data = {'': {'price_discount': {'Установить новую скидку %': '',
                                     'Установить новую цену': ''},
                  'dimdimensions': {'Новая\nВысота (см)': '',
                                    'Новая\nДлина (см)': '',
                                    'Новая\nШирина (см)': ''},
                  'Артикул продавца': '',
                  'Новый остаток': ''},
             '190912901': {'price_discount': {'Установить новую скидку %': '10',
                                              'Установить новую цену': '500'},
                           'dimensions': {'Новая\nВысота (см)': '21',
                                          'Новая\nДлина (см)': '30',
                                          'Новая\nШирина (см)': '40'},
                           'Артикул продавца': 'wild608',
                           'Новый остаток': ''},
             '3123131': {'price_discount': {'Установить новую скидку %': '2',
                                            'Установить новую цену': ''},
                         'dimensions': {'Новая\nВысота (см)': '',
                                        'Новая\nДлина (см)': '',
                                        'Новая\nШирина (см)': ''},
                         'Артикул продавца': 'wild1301',
                         'Новый остаток': ''},
             'вручную': {'price_discount': {'Установить новую скидку %': 'обмен данными с '
                                                                         'вб',
                                            'Установить новую цену': 'обмен данными с вб'},
                         'dimensions': {'Новая\nВысота (см)': '',
                                        'Новая\nДлина (см)': '',
                                        'Новая\nШирина (см)': ''},
                         'Артикул продавца': 'тянет с апи вб',
                         'Новый остаток': 'остаток с вб по фбс парсится с апи вб'}}

# pprint(validate_data(fake_data))


# for i,w in test_result.items():
#     if "price_discount" in w:
#
#         p_d_data.append(
#             {
#                 "nmID": i,
#                 **w["price_discount"]
#             }
#         )
#     if "sizes" in w:
#
# print(p_d_data)
#
# fake_data = {"178600541": {'02-09-2024': 24964},
#              "248268608": {'02-09-2024': 123, '01-09-2024': 123123},
#              "248825415": {'02-09-2024': 0},
#              "248952002": {'02-09-2024': 0},
#              "249111077": {'02-09-2024': 0, '01-09-2024': 0},
#              "249111078": {'02-09-2024': 0},
#              "249111079": {'02-09-2024': 0},
#              "249245604": {'02-09-2024': 0},
#              "249751299": {'02-09-2024': 0},
#              "250111738": {'02-09-2024': 0},
#              "250511046": {'02-09-2024': 0}}
