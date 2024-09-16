import json
import time
from datetime import timedelta, datetime
from pprint import pprint

import gspread
import requests
from gspread import Client, service_account
from utils import get_nm_ids_in_db, column_index_to_letter
import pandas as pd


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
                print(datetime.now())
                print(e)
                print("time sleep 60 sec")
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

        print("итог:", result)
        print("инфа в бд:", nm_ids_in_db)
        return result

    def update_rows(self, data_json, edit_column_clean: dict = None):

        data = self.sheet.get_all_records(expected_headers=[])
        df = pd.DataFrame(data)
        json_df = pd.DataFrame(list(data_json.values()))

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

                row_number = idx + 2  # +2 потому что индексация в Google Таблицах начинается с 1, а первая строка - заголовки
                for column in row.index:
                    if column in headers:
                        # +1 потому что индексация в Google Таблицах начинается с 1
                        column_index = headers.index(column) + 1
                        column_letter = column_index_to_letter(column_index)
                        updates.append({'range': f'{column_letter}{row_number}', 'values': [[row[column]]]})
                if edit_column_clean is not None:
                    if edit_column_clean["price_discount"]:
                        updates.append({'range': f'K{row_number}',
                                        'values': [['']]})  # Очистка столбца 'Установить новую скидку %'
                        updates.append(
                            {'range': f'I{row_number}', 'values': [['']]})  # Очистка столбца 'Установить новую цену'
                    if edit_column_clean["dimensions"]:
                        updates.append({'range': f'S{row_number}', 'values': [['']]})
                        updates.append({'range': f'T{row_number}', 'values': [['']]})
                        updates.append({'range': f'U{row_number}', 'values': [['']]})

                    if edit_column_clean["qty"]:
                        updates.append({'range': f'AE{row_number}', 'values': [['']]})

        self.sheet.batch_update(updates)
        print("Данные успешно обновлены.")
        return True

    def get_edit_data(self, dimension_status, price_and_discount_status, qty_status):
        """
        Получает данные с запросом на изменение с таблицы
        """
        data = self.sheet.get_all_values()

        # Преобразуйте данные в DataFrame
        df = pd.DataFrame(data[1:], columns=data[0])

        # Определите индексы столбцов по их названиям
        header_indices = {header: df.columns.get_loc(header) for header in df.columns}

        # Инициализация пустого словаря для результата
        result_nm_ids_data = {}
        result_qty_edit_data = {}
        # Перебор строк DataFrame
        for index, row in df.iterrows():
            article = row['Артикул']
            account = row['ЛК']
            # Пропуск строки, если "ЛК" или "Артикул" пустые
            if pd.isna(article) or pd.isna(article) or article.strip() == '' or article.strip() == '':
                continue
            # Создание словаря для текущего артикула
            article_dict = {
                # 'Новый остаток': row['Новый остаток'],
                'Артикул продавца': row['Артикул продавца'],
                'Чистая прибыль 1ед.': row['Чистая прибыль 1ед.']
            }
            if price_and_discount_status:
                article_dict.update({"price_discount": {'Установить новую цену': row['Установить новую цену'],
                                                        'Установить новую скидку %': row['Установить новую скидку %']}})
            if dimension_status:
                article_dict.update({"dimensions": {
                    'Новая\nДлина (см)': row['Новая\nДлина (см)'],
                    'Новая\nШирина (см)': row['Новая\nШирина (см)'],
                    'Новая\nВысота (см)': row['Новая\nВысота (см)']}})

            if qty_status:
                if account not in result_qty_edit_data:
                    result_qty_edit_data[account] = {"stocks": [], "nm_ids": []}
                if str(row["Новый остаток"]).isdigit():
                    result_qty_edit_data[account]["stocks"].append(
                        {
                            "sku": row["Баркод"],
                            "amount": int(row["Новый остаток"])
                        },
                    )
                    # nm_id нам будет нужен для функции обновления данных
                    result_qty_edit_data[account]["nm_ids"].append(int(row["Артикул"]))

            if account not in result_nm_ids_data:
                result_nm_ids_data[account] = {}
            # Добавление словаря в результирующий словарь
            result_nm_ids_data[account][article] = article_dict

        # возвращаем словарь
        return {"nm_ids_edit_data": result_nm_ids_data, "qty_edit_data": result_qty_edit_data}

    def create_lk_articles_list(self):
        """Создает словарь из ключей кабинета и его Артикулов"""
        # client = self.client_init_json()
        # spreadsheet = client.open(self.spreadsheet)
        # sheet = spreadsheet.worksheet(self.sheet)
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

    def check_status_service_sheet(self):
        data = self.sheet.get_all_records()
        sheet_status = data[0]
        return sheet_status

    def get_data_quantity_limit(self):
        """Проверяем остатки и лимит по остаткам"""
        data = self.sheet.get_all_records()
        df = pd.DataFrame(data)

        result_data = {}
        for index, row in df.iterrows():
            account = str(row["ЛК"])
            if str(row['Минимальный остаток']).isdigit():
                if int(row["Минимальный остаток"]) > int(row["Текущий остаток"]):
                    if account not in result_data:
                        result_data[account] = {"qty": [], "nm_ids": []}
                    result_data[account]["qty"].append(
                        {"wild": row["Артикул продавца"],
                         "sku": str(row["Баркод"])}
                    )
                    result_data[account]["nm_ids"].append(int(row["Артикул"]))

        return result_data


class GoogleSheetServiceRevenue:
    """Выручка: AD-AN"""

    def __init__(self, spreadsheet: str, sheet: str, creds_json='creds.json'):
        self.creds_json = creds_json
        self.spreadsheet = spreadsheet
        self.sheet = sheet

    def client_init_json(self) -> Client:
        """Создание клиента для работы с Google Sheets."""
        return service_account(filename=self.creds_json)

    def add_for_all_new_nm_id_revenue(self, nm_ids_revenue_data: dict):

        """
        Добавляет выручку в таблицу за все 7 дней по совпадениям столбцов артикула и дней из nm_ids_revenue_data
        Задумана отрабатывать каждые 3 минуты, для всех новых артикулов
        """

        client = self.client_init_json()
        spreadsheet = client.open(self.spreadsheet)
        sheet = spreadsheet.worksheet(self.sheet)
        all_values = sheet.get_all_values()

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
        sheet.batch_update(updates)

        print("Значения обновлены в таблице.")

    def add_last_day_revenue(self, last_day, nm_ids_revenue_data: dict):
        """
        Добавляет выручку за новый день.
        Задумана отрабатывать строго после shift_revenue_columns_to_the_left (добавление нового дня)
        """
        client = self.client_init_json()
        spreadsheet = client.open(self.spreadsheet)
        sheet = spreadsheet.worksheet(self.sheet)

        all_values = sheet.get_all_values()

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
                cell = sheet.cell(row_index, date_col_index + 1)
                updates.append({
                    'range': cell.address,
                    'values': [[value]]
                })

        # Отправка обновлений одним запросом
        sheet.batch_update(updates)

        print("Значения обновлены в таблице.")

    def shift_revenue_columns_to_the_left(self):
        """
        Сдвигает содержимое столбцов (AD-AZ) с выручкой влево и добавляет новый день в AZ.
        Функция задумана отрабатывать раз в день.
        Должна отрабатывать по условию если заголовок AZ это позавчерашний день
        """

        client = self.client_init_json()
        spreadsheet = client.open(self.spreadsheet)
        sheet = spreadsheet.worksheet(self.sheet)

        all_values = sheet.get_all_values()

        # Преобразование в DataFrame
        df = pd.DataFrame(all_values[1:], columns=all_values[0])

        # Смещение заголовков и содержимого столбцов от "AD" до "AJ"
        header_values = df.columns[29:36].tolist()  # Индексы столбцов "AD" до "AJ"
        shifted_header_values = header_values[1:]
        last_date = datetime.strptime(header_values[-1], '%d-%m-%Y') + timedelta(days=1)
        shifted_header_values.append(last_date.strftime('%d-%m-%Y'))

        # Обновление заголовков
        df.columns = df.columns[:29].tolist() + shifted_header_values + df.columns[36:].tolist()

        # Смещение содержимого столбцов от "AD" до "AJ"
        df.iloc[:, 29:35] = df.iloc[:, 30:36].values
        df.iloc[:, 35] = ""  # Очистка последнего столбца "AJ"

        # Преобразование обратно в список списков
        updated_values = [df.columns.tolist()] + df.values.tolist()

        # Обновление таблицы одним запросом
        sheet.update('A1', updated_values)
        """Значения заголовков и содержимого смещены влево в рамках индексов от 'AD' до 'AJ'."""

    def add_week_revenue_by_article(self, week_revenue_data):
        client = self.client_init_json()
        spreadsheet = client.open(self.spreadsheet)
        sheet = spreadsheet.worksheet(self.sheet)

        all_values = sheet.get_all_values()

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
        sheet.update('A1', updated_values)
        print("week data added")

    def check_last_day_header_from_table(self, last_day):
        client = self.client_init_json()
        spreadsheet = client.open(self.spreadsheet)
        sheet = spreadsheet.worksheet(self.sheet)
        headers = sheet.row_values(1)
        if last_day not in headers:
            print("Вчерашнего дня не найдено в заголовках")
            return True
        else:
            print("Заголовок с выручкой вчерашнего дня уже есть в таблице")

            return False

    def update_revenue_rows(self, data_json):
        client = self.client_init_json()
        spreadsheet = client.open(self.spreadsheet)
        sheet = spreadsheet.worksheet(self.sheet)
        # Получаем все записи из таблицы
        data = sheet.get_all_records(expected_headers=[])
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
                    # if pd.isna(df.at[idx, column]) or df.at[idx, column] == "":
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

        sheet.batch_update(updates)


class GoogleSheetSopostTable:
    def __init__(self, sheet="Сопост", spreadsheet="Новая таблица UNIT", creds_json="creds_sopost.json"):
        self.sheet = sheet
        self.spreadsheet = spreadsheet
        self.creds_json = creds_json
        client = self.client_init_json()
        try:
            spreadsheet = client.open(self.spreadsheet)
            self.sheet = spreadsheet.worksheet(self.sheet)

        except gspread.exceptions.APIError as e:
            print(datetime.now())
            print(e)
            time.sleep(60)
            spreadsheet = client.open(self.spreadsheet)
            self.sheet = spreadsheet.worksheet(self.sheet)

    def client_init_json(self) -> Client:
        """Создание клиента для работы с Google Sheets."""
        return service_account(filename=self.creds_json)

    def wild_quantity(self):
        data = self.sheet.get_all_records(expected_headers=["wild", "Добавляем"])
        df = pd.DataFrame(data)

        return dict(zip(df["wild"], df["Добавляем"]))
