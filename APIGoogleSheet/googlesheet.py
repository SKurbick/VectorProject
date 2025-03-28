import asyncio
import json
import time
from datetime import datetime

import pandas
from gspread.utils import rowcol_to_a1

import gspread
import requests
from gspread import Client, service_account
from utils import get_nm_ids_in_db, column_index_to_letter, can_be_int
import pandas as pd

from logger import app_logger as logger


def retry_on_quota_exceeded_async(max_retries=10, delay=60):
    def decorator(func):
        async def async_wrapper(*args, **kwargs):
            retries = 0
            while retries < max_retries:
                try:
                    return await func(*args, **kwargs)
                except gspread.exceptions.APIError as e:
                    logger.error(e)
                    logger.error(f"Async sleep {delay} sec [сработал декоратор]")
                    await asyncio.sleep(delay)
                    retries += 1
            raise Exception("Не удалось выполнить операцию после нескольких попыток.")

        return async_wrapper

    return decorator


class GoogleSheet:
    def __init__(self, spreadsheet: str, sheet: str, creds_json='creds.json'):
        self.creds_json = creds_json
        self.spreadsheet = spreadsheet
        client = self.client_init_json()
        for _ in range(10):
            try:
                spreadsheet = client.open(self.spreadsheet)
                self.sheet = spreadsheet.worksheet(sheet)
                break
            except (gspread.exceptions.APIError, requests.exceptions.ConnectionError) as e:
                logger.error(e)
                logger.info("time sleep 60 sec")
                time.sleep(60)

    def client_init_json(self) -> Client:
        """Создание клиента для работы с Google Sheets."""
        return service_account(filename=self.creds_json)

    @staticmethod
    def get_table_by_id(client: Client, table_key):
        """Получение таблицы из Google Sheets по ID таблицы."""
        return client.open_by_key(table_key)

    def get_nm_ids(self):

        column_index = None
        headers = self.sheet.row_values(1)
        if "Артикул" in headers:
            column_index = headers.index("Артикул") + 1

        column_data = self.sheet.col_values(column_index)
        column_data.pop(0)

        column_data = [int(item) for item in column_data if item.isdigit()]

        return column_data

    @staticmethod
    def check_new_nm_ids(account, nm_ids: list):
        nm_ids_in_db = get_nm_ids_in_db(account=account)
        result = []
        if len(nm_ids):
            result = (set(nm_ids) - set(nm_ids_in_db))
            return [*result]

        logger.info(f"итог: {result}")
        logger.info(f"инфа в бд: {nm_ids_in_db}")
        return result

    @retry_on_quota_exceeded_async()
    async def add_data_to_count_list(self, data_json):
        # сначала мы добавляем новые nmId которых нет в листе "Количество заказов"
        nm_ids_list = list(data_json.keys())

        existing_data = self.sheet.get_all_records()
        existing_articles = {row['Артикул'] for row in existing_data}

        # Собираем все отсутствующие артикулы
        missing_articles = [article for article in nm_ids_list if article not in existing_articles]

        # Добавляем все отсутствующие артикулы одним запросом
        if missing_articles:
            self.sheet.append_rows([[article] for article in missing_articles])

        # на всякий пожарный, что бы гугл не ныл на спам запросов
        await asyncio.sleep(10)

        data = self.sheet.get_all_records(expected_headers=[])
        df = pd.DataFrame(data)

        # Преобразуем данные из словаря в DataFrame
        json_df = pd.DataFrame.from_dict(data_json, orient='index')

        # Преобразуем все значения в json_df в типы данных, которые могут быть сериализованы в JSON
        json_df = json_df.astype(object).where(pd.notnull(json_df), None)

        # Обновите данные в основном DataFrame на основе "Артикул"
        for index, row in json_df.iterrows():
            matching_rows = df[df["Артикул"] == index].index
            for idx in matching_rows:
                for column in row.index:
                    if column in df.columns and (pd.isna(df.at[idx, column]) or df.at[idx, column] == ""):
                        df.at[idx, column] = row[column]

        # Обновите Google Таблицу только для измененных строк
        updates = []
        headers = df.columns.tolist()
        for index, row in json_df.iterrows():
            matching_rows = df[df["Артикул"] == index].index
            for idx in matching_rows:
                row_number = idx + 2  # +2 потому что индексация в Google Таблицах начинается с 1, а первая строка - заголовки
                for column in row.index:
                    if column in headers:
                        # +1 потому что индексация в Google Таблицах начинается с 1
                        column_index = headers.index(column) + 1
                        column_letter = column_index_to_letter(column_index)
                        updates.append({'range': f'{column_letter}{row_number}', 'values': [[row[column]]]})

        self.sheet.batch_update(updates)

        logger.info("Проверка и добавление завершены")

    def update_rows(self, data_json, edit_column_clean: dict = None):
        logger.info("Попал в функцию обновления таблицы")
        data = self.sheet.get_all_records(expected_headers=[])
        df = pd.DataFrame(data)
        json_df = pd.DataFrame(list(data_json.values()))
        try:
            json_df = json_df.drop(["vendor_code", "account"], axis=1)
        except KeyError as e:
            logger.error(f"[func:update_rows] {e} 'vendor_code', 'account'")
        # Преобразуем все значения в json_df в типы данных, которые могут быть сериализованы в JSON
        json_df = json_df.astype(object).where(pd.notnull(json_df), None)
        # Обновите данные в основном DataFrame на основе "Артикул"
        for index, row in json_df.iterrows():
            matching_rows = df[df["Артикул"] == row["Артикул"]].index
            for idx in matching_rows:
                for column in row.index:
                    if pd.isna(df.at[idx, column]) or df.at[idx, column] == "":
                        df.at[idx, column] = row[column]

                if edit_column_clean is not None:
                    if edit_column_clean["price_discount"]:
                        df.at[idx, 'Установить новую скидку %'] = ""
                        df.at[idx, 'Установить новую цену'] = ""

                    if edit_column_clean["dimensions"]:
                        df.at[idx, 'Новая\nДлина (см)'] = ""
                        df.at[idx, 'Новая\nШирина (см)'] = ""
                        df.at[idx, 'Новая\nВысота (см)'] = ""

                    if edit_column_clean["qty"]:
                        df.at[idx, 'Новый остаток'] = ""

        # Обновите Google Таблицу только для измененных строк
        updates = []
        headers = df.columns.tolist()
        for index, row in json_df.iterrows():
            matching_rows = df[df["Артикул"] == row["Артикул"]].index
            for idx in matching_rows:
                # +2 потому что индексация в Google Таблицах начинается с 1, а первая строка - заголовки
                row_number = idx + 2
                for column in row.index:
                    if column in headers:
                        # +1 потому что индексация в Google Таблицах начинается с 1
                        column_index = headers.index(column) + 1
                        column_letter = column_index_to_letter(column_index)
                        updates.append({'range': f'{column_letter}{row_number}', 'values': [[row[column]]]})
                if edit_column_clean is not None:
                    if edit_column_clean["price_discount"]:
                        updates.append({'range': f'L{row_number}',
                                        'values': [['']]})  # Очистка столбца 'Установить новую скидку %'
                        updates.append(
                            {'range': f'J{row_number}', 'values': [['']]})  # Очистка столбца 'Установить новую цену'
                    if edit_column_clean["dimensions"]:
                        updates.append({'range': f'T{row_number}', 'values': [['']]})
                        updates.append({'range': f'U{row_number}', 'values': [['']]})
                        updates.append({'range': f'V{row_number}', 'values': [['']]})

                    if edit_column_clean["qty"]:
                        updates.append({'range': f'AF{row_number}', 'values': [['']]})

        # pprint(updates)
        self.sheet.batch_update(updates)
        logger.info("Данные успешно обновлены.")
        return True

    @staticmethod
    def get_article_dict(service_google_sheet, row, row_article):
        article_dict = {'wild': row_article["vendor_code"],
                        'Чистая прибыль 1ед.': row['Чистая прибыль 1ед.'].replace('\xa0', '')}
        if service_google_sheet["Цены/Скидки"] and str(row['Чистая прибыль 1ед.'].replace('\xa0', '')).lstrip(
                '-').isdigit():
            article_dict["price_discount"] = \
                {'Установить новую цену': row['Установить новую цену'].replace('\xa0', ''),
                 'Установить новую скидку %': row['Установить новую скидку %'].replace('\xa0', '')}
        if service_google_sheet["Габариты"]:
            article_dict["dimensions"] = {'Новая\nДлина (см)': row['Новая\nДлина (см)'].replace('\xa0', ''),
                                          'Новая\nШирина (см)': row['Новая\nШирина (см)'].replace('\xa0', ''),
                                          'Новая\nВысота (см)': row['Новая\nВысота (см)'].replace('\xa0', '')}
        return article_dict

    @staticmethod
    def update_result_qty_edit_data(service_google_sheet, result_qty_edit_data, account, row):
        if service_google_sheet["Остаток"]:
            if account not in result_qty_edit_data:
                result_qty_edit_data[account] = {"stocks": [], "nm_ids": []}
            if str(row["Новый остаток"]).isdigit():
                result_qty_edit_data[account]["stocks"].append(
                    {"sku": row["Баркод"], "amount": int(row["Новый остаток"].replace('\xa0', ''))}, )
                # nm_id нам будет нужен для функции обновления данных почему в список?
                result_qty_edit_data[account]["nm_ids"].append(int(row["Артикул"]))

    async def get_edit_data(self, db_nm_ids_data, service_google_sheet):
        """
        Получает данные с запросом на изменение с таблицы
        """
        data = self.sheet.get_all_values()

        # Преобразуйте данные в DataFrame
        df = pd.DataFrame(data[1:], columns=data[0])
        result_nm_ids_data = {}
        result_qty_edit_data = {}
        for index, row in df.iterrows():
            article = row['Артикул']
            account = str(row['ЛК']).capitalize()
            # if any([not article.isdigit(), not account.strip(), article not in db_nm_ids_data.keys(),
            #         "vendor_code" not in db_nm_ids_data[article]]):
            #     continue
            if not article.isdigit() or not account.strip() or article not in db_nm_ids_data or "vendor_code" not in db_nm_ids_data[article]:
                continue
            article_dict = self.get_article_dict(service_google_sheet, row, db_nm_ids_data[article])
            self.update_result_qty_edit_data(service_google_sheet, result_qty_edit_data, account, row)
            if account not in result_nm_ids_data:
                result_nm_ids_data[account] = {}
            result_nm_ids_data[account][article] = article_dict

        return {"nm_ids_edit_data": result_nm_ids_data, "qty_edit_data": result_qty_edit_data}

    def create_lk_articles_list(self):
        """Создает словарь из ключей кабинета и его Артикулов"""
        data = self.sheet.get_all_records()
        df = pd.DataFrame(data)
        lk_articles_dict = {}
        for index, row in df.iterrows():

            article = row['Артикул']
            lk = row['ЛК'].upper()
            # Пропускаем строки с пустыми значениями в столбце "ЛК" "Артикул"
            if pd.isna(lk) or lk == "":
                continue
            if pd.isna(article) or article == "":
                continue

            # если ячейки, выделенные для изменения, будут иметь число, то они не будут отобраны для обновления данных
            if True in (str(row['Новая\nДлина (см)']).replace('\xa0', '').isdigit(),
                        str(row['Новая\nШирина (см)']).replace('\xa0', '').isdigit(),
                        str(row['Новая\nВысота (см)']).replace('\xa0', '').isdigit(),
                        str(row['Установить новую цену']).replace('\xa0', '').isdigit(),
                        str(row['Установить новую скидку %']).replace('\xa0', '').isdigit(),
                        str(row["Новый остаток"]).replace('\xa0', '').isdigit()):
                continue
            if lk.upper() not in lk_articles_dict:
                lk_articles_dict[lk.upper()] = []
            lk_articles_dict[lk.upper()].append(article)
        return lk_articles_dict

    @retry_on_quota_exceeded_async()
    async def create_lk_barcodes_articles(self):
        """Создание словарь из таблицы в формате Кабинет:{Артикул:Баркод}"""
        data = self.sheet.get_all_records()
        df = pd.DataFrame(data)
        result_dict = {}

        for index, row in df.iterrows():

            article = row['Артикул']
            lk = row['ЛК'].upper()
            barcode = row['Баркод']

            # пропуск невалидных значений
            if pd.isna(lk) or lk == "" or False in (can_be_int(barcode), can_be_int(article)):
                continue

            if lk not in result_dict:
                result_dict[lk] = {}
            result_dict[lk][str(barcode)] = int(article)

        return result_dict

    def create_lk_articles_dict(self):
        """Создает словарь из ключей кабинета и его Артикулов"""
        data = self.sheet.get_all_records()
        df = pd.DataFrame(data)
        lk_articles_dict = {}
        for index, row in df.iterrows():

            article = row['Артикул']
            lk = row['ЛК'].upper()
            profit = str(row['Чистая прибыль 1ед.']).replace("\xa0", "")
            # Пропускаем строки с пустыми значениями в столбце "ЛК" "Артикул"
            if pd.isna(lk) or lk == "":
                continue
            if pd.isna(article) or article == "":
                continue
            if profit.lstrip('-').isdigit() is False:
                continue
            if lk.upper() not in lk_articles_dict:
                lk_articles_dict[lk.upper()] = {}
            lk_articles_dict[lk.upper()].update({article: profit})
        return lk_articles_dict

    def check_status_service_sheet(self):
        all_data = self.sheet.get_all_values()

        # Функция для преобразования строк в числа, если это возможно
        def try_parse_int(value):
            try:
                return int(value)
            except ValueError:
                return value

        # Получение первых заголовков и их значений
        first_header_row = 0  # Индекс строки с первыми заголовками (нумерация с 0)
        first_header_values = all_data[first_header_row]
        first_data_values = all_data[first_header_row + 1]  # Первая строка после первой строки с заголовками

        # Преобразование значений первой строки в числа, если это возможно
        first_data_values = [try_parse_int(value) for value in first_data_values]

        # Получение вторых заголовков и их значений
        second_header_row = 3  # Индекс строки с вторыми заголовками (нумерация с 0)
        second_header_values = all_data[second_header_row]
        second_data_values = all_data[second_header_row + 1]  # Первая строка после четвертой строки с заголовками

        # Преобразование значений второй строки в числа, если это возможно
        second_data_values = [try_parse_int(value) for value in second_data_values]

        # Создание словаря с результатами
        result = dict(zip(first_header_values, first_data_values))
        result.update(dict(zip(second_header_values, second_data_values)))
        return result

    def get_data_quantity_limit(self):
        """Проверяем остатки и лимит по остаткам"""
        data = self.sheet.get_all_records()
        df = pd.DataFrame(data)
        result_data = {}
        for index, row in df.iterrows():
            article = row["Артикул"]
            account = str(row["ЛК"].capitalize())
            min_qty = row["Минимальный остаток"]
            current_qty = row["ФБС"]
            barcode = row["Баркод"]
            wild = row["wild"]

            if str(article).isdigit():
                if str(min_qty).isdigit() and int(min_qty) != 0:
                    if int(min_qty) >= int(current_qty):
                        if account not in result_data:
                            result_data[account] = {"qty": [], "nm_ids": []}
                        result_data[account]["qty"].append(
                            {"wild": wild,
                             "sku": str(barcode)}
                        )
                        result_data[account]["nm_ids"].append(int(article))

        return result_data

    def add_photo(self, data_dict):
        for _ in range(10):
            try:
                client = self.client_init_json()
                spreadsheet = client.open("UNIT 2.0 (tested)")
                sheet = spreadsheet.worksheet("ФОТО")
                existing_articles = sheet.col_values(1)  # Столбец "A" имеет индекс 1
                # Преобразуем ключи в словаре в строки
                data_dict_str = {article: photo for article, photo in data_dict.items()}

                # Создаем список для обновлений
                updates = []

                # Добавляем или обновляем данные
                for article, photo in data_dict_str.items():
                    if article not in existing_articles:
                        # Если артикул не существует, добавляем новую строку
                        updates.append([article, photo])

                sheet.append_rows(updates)
                break
            except (requests.exceptions.ConnectionError, requests.exceptions.HTTPError) as e:
                logger.error(f"{e}")
                time.sleep(63)

    @retry_on_quota_exceeded_async()
    async def add_data_to_count_list(self, data_json):
        # сначала мы добавляем новые nmId которых нет в листе "Количество заказов"
        nm_ids_list = list(data_json.keys())

        existing_data = self.sheet.get_all_records()
        existing_articles = {row['Артикул'] for row in existing_data}

        # Собираем все отсутствующие артикулы
        missing_articles = [article for article in nm_ids_list if article not in existing_articles]

        # Добавляем все отсутствующие артикулы одним запросом
        if missing_articles:
            self.sheet.append_rows([[article] for article in missing_articles])

        # на всякий пожарный, что бы гугл не ныл на спам запросов
        await asyncio.sleep(10)

        data = self.sheet.get_all_records(expected_headers=[])
        df = pd.DataFrame(data)

        # Преобразуем данные из словаря в DataFrame
        json_df = pd.DataFrame.from_dict(data_json, orient='index')

        # Преобразуем все значения в json_df в типы данных, которые могут быть сериализованы в JSON
        json_df = json_df.astype(object).where(pd.notnull(json_df), None)

        # Обновите данные в основном DataFrame на основе "Артикул"
        for index, row in json_df.iterrows():
            matching_rows = df[df["Артикул"] == index].index
            for idx in matching_rows:
                for column in row.index:
                    if column in df.columns and (pd.isna(df.at[idx, column]) or df.at[idx, column] == ""):
                        df.at[idx, column] = row[column]

        # Обновите Google Таблицу только для измененных строк
        updates = []
        headers = df.columns.tolist()
        for index, row in json_df.iterrows():
            matching_rows = df[df["Артикул"] == index].index
            for idx in matching_rows:
                row_number = idx + 2  # +2 потому что индексация в Google Таблицах начинается с 1, а первая строка - заголовки
                for column in row.index:
                    if column in headers:
                        # +1 потому что индексация в Google Таблицах начинается с 1
                        column_index = headers.index(column) + 1
                        column_letter = column_index_to_letter(column_index)
                        updates.append({'range': f'{column_letter}{row_number}', 'values': [[row[column]]]})

        self.sheet.batch_update(updates)

        logger.info("Проверка и добавление завершены")

    def shift_headers_count_list(self, today):
        all_values = self.sheet.get_all_values()
        all_formulas = self.sheet.get_all_values(value_render_option='FORMULA')
        logger.info("Смещаем столбцы листа - Количество заказов")
        # Преобразование в DataFrame
        df_values = pd.DataFrame(all_values[1:], columns=all_values[0])
        df_formulas = pd.DataFrame(all_formulas[1:], columns=all_values[0])

        # Сохраняем формулы из столбцов, которые не попадают в диапазон смещения
        formulas_to_preserve = df_formulas.iloc[:, 33:].values

        # Смещение заголовков и содержимого столбцов
        header_values = df_values.columns[3:33].tolist()  # Индексы столбцов
        shifted_header_values = header_values[:29]
        shifted_header_values.insert(0, today)

        # Обновление заголовков
        df_values.columns = df_values.columns[:3].tolist() + shifted_header_values + df_values.columns[33:].tolist()
        df_formulas.columns = df_values.columns  # Обновляем заголовки в формулах
        # Смещение содержимого столбцов
        df_values.iloc[:, 4:33] = df_values.iloc[:, 3:32].values
        df_values.iloc[:, 3] = ""  # Очистка первого столбца

        # Восстанавливаем формулы в столбцах, которые не попадают в диапазон смещения
        df_formulas.iloc[:, 4:33] = df_formulas.iloc[:, 3:32].values
        df_formulas.iloc[:, 3] = ""  # Очистка первого столбца
        df_formulas.iloc[:, 33:] = formulas_to_preserve

        # Преобразование обратно в список списков
        updated_values = [df_values.columns.tolist()] + df_values.values.tolist()
        updated_formulas = [df_formulas.columns.tolist()] + df_formulas.values.tolist()

        # Обновление таблицы одним запросом
        self.sheet.update('A1', updated_values, value_input_option='USER_ENTERED')
        self.sheet.update('A1', updated_formulas, value_input_option='USER_ENTERED')
        """Значения заголовков и содержимого смещены влево в рамках индексов от 'AG' до 'AM'."""

    def shift_orders_header(self, today):
        all_values = self.sheet.get_all_values()
        all_formulas = self.sheet.get_all_values(value_render_option='FORMULA')
        logger.info("смещает столбцы листа MAIN (столбцы = ЧП по дням)")
        # Преобразование в DataFrame
        df_values = pd.DataFrame(all_values[1:], columns=all_values[0])
        df_formulas = pd.DataFrame(all_formulas[1:], columns=all_values[0])

        # Сохраняем формулы из столбцов, которые не попадают в диапазон смещения
        formulas_to_preserve = df_formulas.iloc[:, 115:].values

        # Смещение заголовков и содержимого столбцов
        header_values = df_values.columns[85:115].tolist()  # Индексы столбцов
        shifted_header_values = header_values[:29]
        shifted_header_values.insert(0, today)
        # Обновление заголовков
        df_values.columns = df_values.columns[:85].tolist() + shifted_header_values + df_values.columns[115:].tolist()
        df_formulas.columns = df_values.columns  # Обновляем заголовки в формулах
        # Смещение содержимого столбцов
        df_values.iloc[:, 86:115] = df_values.iloc[:, 85:114].values
        df_values.iloc[:, 77] = ""  # Очистка первого столбца

        # Восстанавливаем формулы в столбцах, которые не попадают в диапазон смещения
        df_formulas.iloc[:, 86:115] = df_formulas.iloc[:, 85:114].values
        df_formulas.iloc[:, 85] = ""  # Очистка первого столбца
        df_formulas.iloc[:, 115:] = formulas_to_preserve

        # Преобразование обратно в список списков
        updated_values = [df_values.columns.tolist()] + df_values.values.tolist()
        updated_formulas = [df_formulas.columns.tolist()] + df_formulas.values.tolist()

        # Обновление таблицы одним запросом
        self.sheet.update('A1', updated_values, value_input_option='USER_ENTERED')
        self.sheet.update('A1', updated_formulas, value_input_option='USER_ENTERED')
        """Значения заголовков и содержимого смещены влево в рамках индексов"""

    def check_header(self, header):
        # Если заголовка нет в листе, то выдаст True, для функции которая будет добавлять новый header
        headers = self.sheet.row_values(1)
        if header not in headers:
            logger.info(f"заголовка {header} нет в таблице")
            return True
        else:
            logger.info(f"Заголовок {header} уже есть в таблице")
            return False

    @retry_on_quota_exceeded_async()
    async def get_warehouses_info(self) -> dict:
        """ Получить данные по региональным разделениям складов"""

        region_headers = ["Центральный", "Приволжский", "Южный", "Северо-Кавказский"]
        warehouses_by_region = self.sheet.get_all_records(
            expected_headers=region_headers)
        result_dict_data = {}
        df_war_by_reg = pandas.DataFrame(warehouses_by_region)

        for reg_name in region_headers:
            warehouse_names = df_war_by_reg[reg_name].tolist()
            for wh_name in warehouse_names:
                result_dict_data[wh_name] = reg_name

        return result_dict_data

    @retry_on_quota_exceeded_async()
    async def update_untracked_warehouses_quantity(self, update_data):
        """Актуализация остатков по неотслеживаемым складам"""
        # Retrieve all values from the sheet
        values = self.sheet.get_all_values()

        # Assign headers and data
        if values:
            headers = values[0]
            data = values[1:]
        else:
            headers = ["НЕОТСЛЕЖИВАЕМЫЕ СКЛАДЫ", "ОСТАТКИ"]
            data = []

        # Create the DataFrame
        df = pd.DataFrame(data, columns=headers)

        # Ensure required columns exist
        required_headers = ["НЕОТСЛЕЖИВАЕМЫЕ СКЛАДЫ", "ОСТАТКИ"]
        for header in required_headers:
            if header not in df.columns:
                df[header] = ""

        # Identify existing warehouses
        existing_warehouses = df["НЕОТСЛЕЖИВАЕМЫЕ СКЛАДЫ"].tolist()

        # Determine new warehouses to add
        new_warehouses = [key for key in update_data.keys() if key not in existing_warehouses]

        # Prepare new rows for the DataFrame
        if new_warehouses:
            new_rows = []
            for warehouse in new_warehouses:
                row = {header: "" for header in headers}
                row["НЕОТСЛЕЖИВАЕМЫЕ СКЛАДЫ"] = warehouse
                row.update(update_data[warehouse])
                new_rows.append(row)
            new_df = pd.DataFrame(new_rows)
            df = pd.concat([df, new_df], ignore_index=True)

        # Create DataFrame from update data
        json_df = pd.DataFrame.from_dict(update_data, orient='index')
        json_df = json_df.astype(object).where(pd.notnull(json_df), None)

        # Update existing rows in the DataFrame
        for index, row in json_df.iterrows():
            matching_rows = df[df["НЕОТСЛЕЖИВАЕМЫЕ СКЛАДЫ"] == index].index
            for idx in matching_rows:
                for column in row.index:
                    if column in df.columns:
                        df.at[idx, column] = row[column]

        # Prepare updates for the Google Sheet
        updates = []
        headers_list = df.columns.tolist()
        for index, row in df.iterrows():
            row_number = index + 2  # +2 because indexing starts at 1, with the first row being headers
            for column in headers_list:
                column_index = headers_list.index(column) + 1
                column_letter = column_index_to_letter(column_index)
                value = row[column]
                if value is not None:
                    updates.append({'range': f'{column_letter}{row_number}', 'values': [[value]]})

        # Batch update the Google Sheet with error handling
        try:
            self.sheet.batch_update(updates)
        except Exception as e:
            logger.error(f'Error during batch update: {e}')

    @retry_on_quota_exceeded_async()
    async def update_qty_by_reg(self, update_data):
        data = self.sheet.get_all_records(expected_headers=[])
        df = pd.DataFrame(data)

        # Преобразуем данные из словаря в DataFrame
        json_df = pd.DataFrame.from_dict(update_data, orient='index')

        # Преобразуем все значения в json_df в типы данных, которые могут быть сериализованы в JSON
        json_df = json_df.astype(object).where(pd.notnull(json_df), None)

        # Обновите данные в основном DataFrame на основе "Артикул"
        for index, row in json_df.iterrows():
            matching_rows = df[df["Артикул"] == index].index
            for idx in matching_rows:
                for column in row.index:
                    if column in df.columns and (pd.isna(df.at[idx, column]) or df.at[idx, column] == ""):
                        df.at[idx, column] = row[column]

        # Обновите Google Таблицу только для измененных строк
        updates = []
        headers = df.columns.tolist()
        for index, row in json_df.iterrows():
            matching_rows = df[df["Артикул"] == index].index
            for idx in matching_rows:
                row_number = idx + 2  # +2 потому что индексация в Google Таблицах начинается с 1, а первая строка - заголовки
                for column in row.index:
                    if column in headers:
                        # +1 потому что индексация в Google Таблицах начинается с 1
                        column_index = headers.index(column) + 1
                        column_letter = column_index_to_letter(column_index)
                        updates.append({'range': f'{column_letter}{row_number}', 'values': [[row[column]]]})

        self.sheet.batch_update(updates)


class GoogleSheetServiceRevenue:
    """Выручка: AD-AN"""

    def __init__(self, spreadsheet: str, sheet: str, creds_json='creds.json'):
        self.creds_json = creds_json
        self.spreadsheet = spreadsheet
        client = self.client_init_json()
        for _ in range(10):
            try:
                spreadsheet = client.open(self.spreadsheet)
                self.sheet = spreadsheet.worksheet(sheet)
                break
            except (gspread.exceptions.APIError, requests.exceptions.ConnectionError) as e:
                logger.info(datetime.now())
                logger.error(e)
                logger.info("time sleep 60 sec")
                time.sleep(60)

    def client_init_json(self) -> Client:
        """Создание клиента для работы с Google Sheets."""
        return service_account(filename=self.creds_json)

    def add_for_all_new_nm_id_revenue(self, nm_ids_revenue_data: dict):
        """
        Добавляет выручку в таблицу за все 7 дней по совпадениям столбцов артикула и дней из nm_ids_revenue_data
        Задумана отрабатывать каждые 3 минуты, для всех новых артикулов
        """
        #
        # client = self.client_init_json()
        # spreadsheet = client.open(self.spreadsheet)
        # sheet = spreadsheet.worksheet(self.sheet)
        all_values = self.sheet.get_all_values()

        # Находим индекс столбца "Артикул"
        header_row = all_values[0]
        article_col_index = header_row.index('Артикул')

        # Создаем словарь для хранения обновлений
        updates = []

        # Проходим по всем строкам, начиная со второй (индекс 1)
        for row_index, row in enumerate(all_values[1:], start=2):
            article = row[article_col_index]
            if article in nm_ids_revenue_data:
                for date_col_name, value in nm_ids_revenue_data[article].items():
                    if date_col_name in header_row:
                        date_col_index = header_row.index(date_col_name)
                        cell_address = gspread.utils.rowcol_to_a1(row_index, date_col_index + 1)
                        updates.append({
                            'range': cell_address,
                            'values': [[value]]
                        })

        # Отправка обновлений одним запросом
        self.sheet.batch_update(updates)

        logger.info("Значения обновлены в таблице.")

    def add_last_day_revenue(self, last_day, nm_ids_revenue_data: dict):
        """
        Добавляет выручку за новый день.
        Задумана отрабатывать строго после shift_revenue_columns_to_the_left (добавление нового дня)
        """

        all_values = self.sheet.get_all_values()

        # Находим индекс столбца "Артикул" и колонку формата="03-09-2024"
        header_row = all_values[0]
        article_col_index = header_row.index('Артикул')
        date_col_index = header_row.index(last_day)

        # Обновление значений в таблице
        updates = []
        for row_index, row in enumerate(all_values[1:], start=2):  # Начинаем с 2, так как первая строка - заголовки
            article = row[article_col_index]
            if article in nm_ids_revenue_data:
                value = nm_ids_revenue_data[article].get(last_day, '')
                cell = self.sheet.cell(row_index, date_col_index + 1)
                updates.append({
                    'range': cell.address,
                    'values': [[value]]
                })

        # Отправка обновлений одним запросом
        logger.info(updates)
        self.sheet.batch_update(updates, value_input_option="USER_ENTERED")

        logger.info("Значения обновлены в таблице.")

    def shift_revenue_columns_to_the_left(self, last_day):
        """
        Сдвигает содержимое столбцов с выручкой влево и добавляет новый день.
        Функция задумана отрабатывать раз в день.
        Должна отрабатывать по условию если заголовок это вчерашний день
        """
        all_values = self.sheet.get_all_values()
        all_formulas = self.sheet.get_all_values(value_render_option='FORMULA')

        # Преобразование в DataFrame
        df_values = pd.DataFrame(all_values[1:], columns=all_values[0])
        df_formulas = pd.DataFrame(all_formulas[1:], columns=all_values[0])

        # Сохраняем формулы из столбцов, которые не попадают в диапазон смещения
        formulas_to_preserve = df_formulas.iloc[:, 61:].values
        # Смещение заголовков и содержимого столбцов
        header_values = df_values.columns[53:61].tolist()  # Индексы столбцов

        shifted_header_values = header_values[1:]
        shifted_header_values.append(last_day)
        # Обновление заголовков
        df_values.columns = df_values.columns[:53].tolist() + shifted_header_values + df_values.columns[61:].tolist()

        df_formulas.columns = df_values.columns  # Обновляем заголовки в формулах

        # Смещение содержимого столбцов
        df_values.iloc[:, 53:60] = df_values.iloc[:, 54:61].values

        # Восстанавливаем формулы в столбцах, которые не попадают в диапазон смещения
        df_formulas.iloc[:, 53:60] = df_formulas.iloc[:, 54:61].values
        df_formulas.iloc[:, 60] = ""  # Очистка последнего столбца
        df_formulas.iloc[:, 61:] = formulas_to_preserve

        # Преобразование обратно в список списков
        updated_values = [df_values.columns.tolist()] + df_values.values.tolist()
        updated_formulas = [df_formulas.columns.tolist()] + df_formulas.values.tolist()

        # Обновление таблицы одним запросом
        self.sheet.update('A1', updated_values, value_input_option='USER_ENTERED')
        self.sheet.update('A1', updated_formulas, value_input_option='USER_ENTERED')

    def shift_week_revenue_columns_to_the_left(self, last_week):
        """
        Сдвигает содержимое столбцов с выручкой влево и добавляет новый день.
        Функция задумана отрабатывать раз в день.
        """

        all_values = self.sheet.get_all_values()
        all_formulas = self.sheet.get_all_values(value_render_option='FORMULA')

        # Преобразование в DataFrame
        df_values = pd.DataFrame(all_values[1:], columns=all_values[0])
        df_formulas = pd.DataFrame(all_formulas[1:], columns=all_values[0])

        # Сохраняем формулы из столбцов, которые не попадают в диапазон смещения
        formulas_to_preserve = df_formulas.iloc[:, 65:].values

        # Смещение заголовков и содержимого столбцов

        header_values = df_values.columns[61:65].tolist()  # Индексы столбцов
        shifted_header_values = header_values[1:]
        shifted_header_values.append(last_week)

        # Обновление заголовков
        df_values.columns = df_values.columns[:61].tolist() + shifted_header_values + df_values.columns[65:].tolist()

        df_formulas.columns = df_values.columns  # Обновляем заголовки в формулах

        # Смещение содержимого столбцов
        df_values.iloc[:, 61:64] = df_values.iloc[:, 62:65].values

        # Восстанавливаем формулы в столбцах, которые не попадают в диапазон смещения
        df_formulas.iloc[:, 61:64] = df_formulas.iloc[:, 62:65].values
        df_formulas.iloc[:, 64] = ""  # Очистка последнего столбца
        df_formulas.iloc[:, 65:] = formulas_to_preserve

        # Преобразование обратно в список списков
        updated_values = [df_values.columns.tolist()] + df_values.values.tolist()
        updated_formulas = [df_formulas.columns.tolist()] + df_formulas.values.tolist()

        # # Обновление таблицы одним запросом
        self.sheet.update('A1', updated_values, value_input_option='USER_ENTERED')
        self.sheet.update('A1', updated_formulas, value_input_option='USER_ENTERED')

        """Значения заголовков и содержимого смещены влево в рамках индексов от 'AP' до 'AR'."""

    def add_week_revenue_by_article(self, week_revenue_data):

        all_values = self.sheet.get_all_values()

        headers = all_values[0]
        df = pd.DataFrame(all_values[1:], columns=headers)

        # Преобразуем столбец "Артикул" в числовой тип
        df['Артикул'] = pd.to_numeric(df['Артикул'], errors='coerce')

        # Проходим по всем строкам и обновляем значения
        for date_range, articles in week_revenue_data.items():
            if date_range in df.columns:
                for article, value in articles.items():
                    df.loc[df['Артикул'] == int(article), date_range] = value

        # Заменяем NaN на пустые строки
        df = df.fillna('')

        # Преобразуем DataFrame обратно в список списков
        updated_values = [df.columns.tolist()] + df.values.tolist()

        # Отправляем обновленные данные обратно в таблицу
        self.sheet.update('A1', updated_values)
        logger.info("week data added")

    def check_last_day_header_from_table(self, header):
        headers = self.sheet.row_values(1)
        if header not in headers:
            logger.info(f"заголовка {header} нет в таблице")
            return True
        else:
            logger.info(f"Заголовок {header} уже есть в таблице")

            return False

    @retry_on_quota_exceeded_async()
    async def add_data_to_count_list(self, data_json):
        # сначала мы добавляем новые nmId которых нет в листе "Количество заказов"
        nm_ids_list = list(data_json.keys())

        existing_data = self.sheet.get_all_records()
        existing_articles = {row['Артикул'] for row in existing_data}

        # Собираем все отсутствующие артикулы
        missing_articles = [article for article in nm_ids_list if article not in existing_articles]

        # Добавляем все отсутствующие артикулы одним запросом
        if missing_articles:
            self.sheet.append_rows([[article] for article in missing_articles])

        # на всякий пожарный, что бы гугл не ныл на спам запросов
        await asyncio.sleep(10)

        data = self.sheet.get_all_records(expected_headers=[])
        df = pd.DataFrame(data)

        # Преобразуем данные из словаря в DataFrame
        json_df = pd.DataFrame.from_dict(data_json, orient='index')

        # Преобразуем все значения в json_df в типы данных, которые могут быть сериализованы в JSON
        json_df = json_df.astype(object).where(pd.notnull(json_df), None)

        # Обновите данные в основном DataFrame на основе "Артикул"
        for index, row in json_df.iterrows():
            matching_rows = df[df["Артикул"] == index].index
            for idx in matching_rows:
                for column in row.index:
                    if column in df.columns and (pd.isna(df.at[idx, column]) or df.at[idx, column] == ""):
                        df.at[idx, column] = row[column]

        # Обновите Google Таблицу только для измененных строк
        updates = []
        headers = df.columns.tolist()
        for index, row in json_df.iterrows():
            matching_rows = df[df["Артикул"] == index].index
            for idx in matching_rows:
                row_number = idx + 2  # +2 потому что индексация в Google Таблицах начинается с 1, а первая строка - заголовки
                for column in row.index:
                    if column in headers:
                        # +1 потому что индексация в Google Таблицах начинается с 1
                        column_index = headers.index(column) + 1
                        column_letter = column_index_to_letter(column_index)
                        updates.append({'range': f'{column_letter}{row_number}', 'values': [[row[column]]]})

        self.sheet.batch_update(updates)

        logger.info("Проверка и добавление завершены")

    def update_revenue_rows(self, data_json):
        data = self.sheet.get_all_records(expected_headers=[])
        df = pd.DataFrame(data)

        # Преобразуем данные из словаря в DataFrame
        json_df = pd.DataFrame.from_dict(data_json, orient='index')

        # Преобразуем все значения в json_df в типы данных, которые могут быть сериализованы в JSON
        json_df = json_df.astype(object).where(pd.notnull(json_df), None)

        # Обновите данные в основном DataFrame на основе "Артикул"
        for index, row in json_df.iterrows():
            matching_rows = df[df["Артикул"] == index].index
            for idx in matching_rows:
                for column in row.index:
                    if column in df.columns and (pd.isna(df.at[idx, column]) or df.at[idx, column] == ""):
                        df.at[idx, column] = row[column]
        # Обновите Google Таблицу только для измененных строк
        updates = []
        headers = df.columns.tolist()
        for index, row in json_df.iterrows():
            matching_rows = df[df["Артикул"] == index].index
            for idx in matching_rows:
                row_number = idx + 2  # +2 потому что индексация в Google Таблицах начинается с 1, а первая строка - заголовки
                for column in row.index:
                    if column in headers:
                        # +1 потому что индексация в Google Таблицах начинается с 1
                        column_index = headers.index(column) + 1
                        column_letter = column_index_to_letter(column_index)
                        updates.append({'range': f'{column_letter}{row_number}', 'values': [[row[column]]]})

        self.sheet.batch_update(updates)


class GoogleSheetSopostTable:
    from settings import settings
    def __init__(self, sheet="Сопост", spreadsheet=settings.SPREADSHEET, creds_json=settings.CREEDS_FILE_NAME):
        # self.sheet = sheet
        # self.spreadsheet = spreadsheet
        self.creds_json = creds_json
        client = self.client_init_json()
        for _ in range(10):
            try:
                spreadsheet = client.open(spreadsheet)
                self.sheet = spreadsheet.worksheet(sheet)
                break
            except (gspread.exceptions.APIError, requests.exceptions.JSONDecodeError) as e:
                logger.info(datetime.now())
                logger.info(e)
                time.sleep(60)
                spreadsheet = client.open(spreadsheet)
                self.sheet = spreadsheet.worksheet(sheet)

    def client_init_json(self) -> Client:
        """Создание клиента для работы с Google Sheets."""
        return service_account(filename=self.creds_json)

    def wild_quantity(self):
        data = self.sheet.get_all_records(expected_headers=["wild", "Добавляем"])
        df = pd.DataFrame(data)

        return dict(zip(df["wild"], df["Добавляем"]))


class PCGoogleSheet:
    def __init__(self, spreadsheet: str, sheet: str, creds_json='creds.json'):
        self.creds_json = creds_json
        self.spreadsheet = spreadsheet
        client = self.client_init_json()
        for _ in range(10):
            try:
                spreadsheet = client.open(self.spreadsheet)
                self.sheet = spreadsheet.worksheet(sheet)
                break
            except (gspread.exceptions.APIError, requests.exceptions.ConnectionError) as e:
                logger.info(datetime.now())
                logger.info(e)
                logger.info("time sleep 60 sec")
                time.sleep(60)

    def client_init_json(self) -> Client:
        """Создание клиента для работы с Google Sheets."""
        return service_account(filename=self.creds_json)

    def check_last_day_header_from_table(self, header):
        headers = self.sheet.row_values(1)
        if header not in headers:
            logger.info(f"заголовка {header} нет в таблице")
            return True
        else:
            logger.info(f"Заголовок {header} уже есть в таблице")

            return False

    def create_lk_articles_dict(self):
        """Создает словарь из ключей кабинета и его Артикулов"""
        data = self.sheet.get_all_records()
        df = pd.DataFrame(data)
        lk_articles_dict = {}
        for index, row in df.iterrows():

            article = row['Артикул']
            lk = row['ЛК'].upper()
            # profit = str(row['ЧП']).replace("\xa0", "")
            profit = row['ЧП']
            # Пропускаем строки с пустыми значениями в столбце "ЛК" "Артикул"
            if pd.isna(lk) or lk == "":
                continue
            if pd.isna(article) or article == "":
                continue
            if str(profit).lstrip('-').isdigit() is False:
                continue
            if lk.upper() not in lk_articles_dict:
                lk_articles_dict[lk.upper()] = {}
            lk_articles_dict[lk.upper()].update({article: profit})
        return lk_articles_dict

    def create_lk_articles_list(self):
        """Создает словарь из ключей кабинета и его Артикулов"""
        data = self.sheet.get_all_records()
        df = pd.DataFrame(data)
        lk_articles_dict = {}
        for index, row in df.iterrows():

            article = row['Артикул']
            lk = row['ЛК'].upper()
            # Пропускаем строки с пустыми значениями в столбце "ЛК" "Артикул"
            if pd.isna(lk) or lk == "":
                continue
            if pd.isna(article) or article == "":
                continue

            if lk.upper() not in lk_articles_dict:
                lk_articles_dict[lk.upper()] = []
            lk_articles_dict[lk.upper()].append(article)
        return lk_articles_dict

    def shift_orders_header(self, day):
        all_values = self.sheet.get_all_values()
        all_formulas = self.sheet.get_all_values(value_render_option='FORMULA')
        logger.info("смещает столбцы листа ПРОДАЖИ в таблице 'Условный расчет' (столбцы = ЧП по дням)")
        # Преобразование в DataFrame
        df_values = pd.DataFrame(all_values[1:], columns=all_values[0])
        df_formulas = pd.DataFrame(all_formulas[1:], columns=all_values[0])

        # Сохраняем формулы из столбцов, которые не попадают в диапазон смещения
        formulas_to_preserve = df_formulas.iloc[:, 48:].values

        # Смещение заголовков и содержимого столбцов
        header_values = df_values.columns[18:48].tolist()  # Индексы столбцов
        logger.info(header_values)
        shifted_header_values = header_values[:29]
        shifted_header_values.insert(0, day)
        # Обновление заголовков
        df_values.columns = df_values.columns[:18].tolist() + shifted_header_values + df_values.columns[48:].tolist()

        df_formulas.columns = df_values.columns  # Обновляем заголовки в формулах
        # Смещение содержимого столбцов
        df_values.iloc[:, 19:48] = df_values.iloc[:, 18:47].values
        df_values.iloc[:, 18] = ""  # Очистка первого столбца

        # Восстанавливаем формулы в столбцах, которые не попадают в диапазон смещения
        df_formulas.iloc[:, 19:48] = df_formulas.iloc[:, 18:47].values
        df_formulas.iloc[:, 18] = ""  # Очистка первого столбца
        df_formulas.iloc[:, 48:] = formulas_to_preserve

        # Преобразование обратно в список списков
        updated_values = [df_values.columns.tolist()] + df_values.values.tolist()
        updated_formulas = [df_formulas.columns.tolist()] + df_formulas.values.tolist()

        # Обновление таблицы одним запросом
        self.sheet.update('A1', updated_values, value_input_option='USER_ENTERED')
        self.sheet.update('A1', updated_formulas, value_input_option='USER_ENTERED')

    def update_revenue_rows(self, data_json):
        data = self.sheet.get_all_records(expected_headers=[])
        df = pd.DataFrame(data)

        # Преобразуем данные из словаря в DataFrame
        json_df = pd.DataFrame.from_dict(data_json, orient='index')

        # Преобразуем все значения в json_df в типы данных, которые могут быть сериализованы в JSON
        json_df = json_df.astype(object).where(pd.notnull(json_df), None)

        # Обновите данные в основном DataFrame на основе "Артикул"
        for index, row in json_df.iterrows():
            matching_rows = df[df["Артикул"] == index].index
            for idx in matching_rows:
                for column in row.index:
                    if column in df.columns and (pd.isna(df.at[idx, column]) or df.at[idx, column] == ""):
                        df.at[idx, column] = row[column]

        # Обновите Google Таблицу только для измененных строк
        updates = []
        headers = df.columns.tolist()
        for index, row in json_df.iterrows():
            matching_rows = df[df["Артикул"] == index].index
            for idx in matching_rows:
                row_number = idx + 2  # +2 потому что индексация в Google Таблицах начинается с 1, а первая строка - заголовки
                for column in row.index:
                    if column in headers:
                        # +2 потому что индексация в Google Таблицах начинается с 1 (я хз почему именно в этой таблице нужно делать +2)
                        column_index = headers.index(column) + 1
                        column_letter = column_index_to_letter(column_index)
                        updates.append({'range': f'{column_letter}{row_number}', 'values': [[row[column]]]})
        self.sheet.batch_update(updates)
        logger.info("Актуализированы данные по дням в листе ПРОДАЖИ")


def update_columns_in_purchase_calculation():
    # Дает права на взаимодействие с гугл-таблицами
    gc = gspread.service_account(filename='creds.json')

    # Таблица из которой берем данные
    table_from = gc.open("UNIT 2.0 (tested)")

    #  Таблица в которую закидываем данные
    table_to = gc.open("Условный расчет")

    # Лист откуда берем данные
    sheet_from = table_from.worksheet('MAIN (tested)').get_all_values()
    # Лист куда вставляем данные
    sheet_to = table_to.worksheet('Продажи')
    sheet_from_df = pd.DataFrame(sheet_from[1:], columns=sheet_from[0])
    sheet_from_before = sheet_from_df.iloc[:, :3]
    # Данные для вставки в гугл таблицу
    sheet_from_before_data = [sheet_from_before.columns.values.tolist()] + sheet_from_before.values.tolist()
    sheet_to.update(sheet_from_before_data, 'A:C', value_input_option='USER_ENTERED')

    # Данные для вставки в гугл таблицу
    sheet_from_after = sheet_from_df.iloc[:, 4:10]
    sheet_from_after_data = [sheet_from_after.columns.values.tolist()] + sheet_from_after.values.tolist()
    sheet_to.update(sheet_from_after_data, 'E:J', value_input_option='USER_ENTERED')
