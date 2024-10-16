import json
import time
from datetime import timedelta, datetime
from pprint import pprint
from gspread.utils import rowcol_to_a1

import gspread
import requests
from gspread import Client, service_account
from utils import get_nm_ids_in_db, column_index_to_letter, get_data_for_nm_ids
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
        print("Попал в функцию обновления таблицы")
        data = self.sheet.get_all_records(expected_headers=[])
        df = pd.DataFrame(data)
        json_df = pd.DataFrame(list(data_json.values()))
        json_df = json_df.drop(["vendor_code", "account"], axis=1)
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

        # pprint(updates)
        self.sheet.batch_update(updates)
        print("Данные успешно обновлены.")
        return True

    def get_edit_data(self, dimension_status, price_and_discount_status, qty_status):
        db_nm_ids_data = get_data_for_nm_ids()
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
            # Пропуск если данных по артикулу нет в бд (нужен для подтягивания валидно вилда)
            if str(article) not in db_nm_ids_data.keys() or "vendorCode" not in db_nm_ids_data[str(article)]:
                continue
            # Создание словаря для текущего артикула
            article_dict = {
                # подтягиваем wild с БД
                'wild': db_nm_ids_data[str(article)]["vendorCode"],
                'Чистая прибыль 1ед.': row['Чистая прибыль 1ед.'].replace('\xa0', '')
            }
            if price_and_discount_status:
                article_dict.update(
                    {"price_discount": {'Установить новую цену': row['Установить новую цену'].replace('\xa0', ''),
                                        'Установить новую скидку %': row['Установить новую скидку %'].replace('\xa0',
                                                                                                              '')}})
            if dimension_status:
                article_dict.update({"dimensions": {
                    'Новая\nДлина (см)': row['Новая\nДлина (см)'].replace('\xa0', ''),
                    'Новая\nШирина (см)': row['Новая\nШирина (см)'].replace('\xa0', ''),
                    'Новая\nВысота (см)': row['Новая\nВысота (см)'].replace('\xa0', '')}})

            if qty_status:
                if account not in result_qty_edit_data:
                    result_qty_edit_data[account] = {"stocks": [], "nm_ids": []}
                if str(row["Новый остаток"]).isdigit():
                    result_qty_edit_data[account]["stocks"].append(
                        {
                            "sku": row["Баркод"],
                            "amount": int(row["Новый остаток"].replace('\xa0', ''))
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

            # todo готов к использованию, раскомментировать после подготовки бд
            # если ячейки, выделенные для изменения, будут иметь число, то они не будут отобраны для обновления данных
            # if True in (str(row['Новая\nДлина (см)']).replace('\xa0', '').isdigit(),
            #             str(row['Новая\nШирина (см)']).replace('\xa0', '').isdigit(),
            #             str(row['Новая\nВысота (см)']).replace('\xa0', '').isdigit(),
            #             str(row['Установить новую цену']).replace('\xa0', '').isdigit(),
            #             str(row['Установить новую скидку %']).replace('\xa0', '').isdigit(),
            #             str(row["Новый остаток"]).replace('\xa0', '').isdigit()):
            #     continue
            if lk.upper() not in lk_articles_dict:
                lk_articles_dict[lk.upper()] = []
            lk_articles_dict[lk.upper()].append(article)
        return lk_articles_dict

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
            if profit.isdigit() is False:
                continue
            if lk.upper() not in lk_articles_dict:
                lk_articles_dict[lk.upper()] = {}
            lk_articles_dict[lk.upper()].update({article: profit})
        return lk_articles_dict

    def check_status_service_sheet(self):
        data = self.sheet.get_all_records()
        sheet_status = data[0]
        return sheet_status

    def get_data_quantity_limit(self):
        import math
        """Проверяем остатки и лимит по остаткам"""
        data = self.sheet.get_all_records()
        df = pd.DataFrame(data)

        result_data = {}
        check_fbs_fbo_data = {}
        for index, row in df.iterrows():
            article = row["Артикул"]
            account = str(row["ЛК"])
            status_fbo = str(row["Признак ФБО"])
            min_qty = row["Минимальный остаток"]
            current_qty = row["Текущий остаток"]
            barcode = row["Баркод"]
            current_qty_wb = row["Текущий остаток\nСклады WB"]
            average_day_orders = row["Среднее в день"]
            if str(article).isdigit():
                if str(min_qty).isdigit() and int(min_qty) != 0:
                    if int(min_qty) >= int(current_qty):
                        if account not in result_data:
                            result_data[account] = {"qty": [], "nm_ids": []}
                        result_data[account]["qty"].append(
                            {"wild": row["wild"],
                             "sku": str(barcode)}
                        )
                        result_data[account]["nm_ids"].append(int(article))

                "собираем артикулы\баркоды формируем запрос на изменение остатков и коррекции ячейки 'Минимальный остаток'"
                # if str(current_qty_wb).isdigit():  # проверяет что ячейка является числом
                #
                #     if (status_fbo == 'да' and str(min_qty).isdigit() is True) and str(average_day_orders).isdigit():
                #
                #
                #     # собираем данные для закрытия ФБС
                #     # собираем данные для открытия ФБС
                #     if



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
                print("[ERROR]", e)
                time.sleep(63)

    def add_data_to_count_list(self, data_json):
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
        time.sleep(10)

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

        print("Проверка и добавление завершены")

    def shift_headers_count_list(self, today):
        all_values = self.sheet.get_all_values()
        all_formulas = self.sheet.get_all_values(value_render_option='FORMULA')
        print("Смещаем столбцы листа - Количество заказов")
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
        print("смещает столбцы листа MAIN (столбцы = ЧП по дням)")
        # Преобразование в DataFrame
        df_values = pd.DataFrame(all_values[1:], columns=all_values[0])
        df_formulas = pd.DataFrame(all_formulas[1:], columns=all_values[0])

        # Сохраняем формулы из столбцов, которые не попадают в диапазон смещения
        formulas_to_preserve = df_formulas.iloc[:, 97:].values

        # Смещение заголовков и содержимого столбцов
        header_values = df_values.columns[67:97].tolist()  # Индексы столбцов
        shifted_header_values = header_values[:29]
        shifted_header_values.insert(0, today)
        # # # Обновление заголовков
        df_values.columns = df_values.columns[:67].tolist() + shifted_header_values + df_values.columns[97:].tolist()
        df_formulas.columns = df_values.columns  # Обновляем заголовки в формулах
        # # Смещение содержимого столбцов от "AG" до "AM"
        df_values.iloc[:, 68:97] = df_values.iloc[:, 67:96].values
        df_values.iloc[:, 67] = ""  # Очистка первого столбца

        # Восстанавливаем формулы в столбцах, которые не попадают в диапазон смещения
        df_formulas.iloc[:, 68:97] = df_formulas.iloc[:, 67:96].values
        df_formulas.iloc[:, 67] = ""  # Очистка первого столбца
        df_formulas.iloc[:, 97:] = formulas_to_preserve

        # Преобразование обратно в список списков
        updated_values = [df_values.columns.tolist()] + df_values.values.tolist()
        updated_formulas = [df_formulas.columns.tolist()] + df_formulas.values.tolist()

        # Обновление таблицы одним запросом
        self.sheet.update('A1', updated_values, value_input_option='USER_ENTERED')
        self.sheet.update('A1', updated_formulas, value_input_option='USER_ENTERED')
        """Значения заголовков и содержимого смещены влево в рамках индексов от 'AG' до 'AM'."""

    def check_header(self, header):
        # Если заголовка нет в листе, то выдаст True, для функции которая будет добавлять новый header
        headers = self.sheet.row_values(1)
        if header not in headers:
            print(f"заголовка {header} нет в таблице")
            return True
        else:
            print(f"Заголовок {header} уже есть в таблице")
            return False


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
                print(datetime.now())
                print(e)
                print("time sleep 60 sec")
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

        print("Значения обновлены в таблице.")

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
        print(updates)
        self.sheet.batch_update(updates, value_input_option="USER_ENTERED")

        print("Значения обновлены в таблице.")

    def shift_revenue_columns_to_the_left(self, last_day):
        """
        Сдвигает содержимое столбцов (AJ-AQ) с выручкой влево и добавляет новый день в AQ.
        Функция задумана отрабатывать раз в день.
        Должна отрабатывать по условию если заголовок AQ это вчерашний день
        """
        all_values = self.sheet.get_all_values()
        all_formulas = self.sheet.get_all_values(value_render_option='FORMULA')

        # Преобразование в DataFrame
        df_values = pd.DataFrame(all_values[1:], columns=all_values[0])
        df_formulas = pd.DataFrame(all_formulas[1:], columns=all_values[0])

        # Сохраняем формулы из столбцов, которые не попадают в диапазон смещения
        formulas_to_preserve = df_formulas.iloc[:, 43:].values

        # Смещение заголовков и содержимого столбцов
        header_values = df_values.columns[35:43].tolist()  # Индексы столбцов
        shifted_header_values = header_values[1:]
        shifted_header_values.append(last_day)

        # Обновление заголовков
        df_values.columns = df_values.columns[:35].tolist() + shifted_header_values + df_values.columns[43:].tolist()
        df_formulas.columns = df_values.columns  # Обновляем заголовки в формулах

        # Смещение содержимого столбцов
        df_values.iloc[:, 35:42] = df_values.iloc[:, 36:43].values
        df_values.iloc[:, 40] = ""  # Очистка последнего столбца "AM"

        # Восстанавливаем формулы в столбцах, которые не попадают в диапазон смещения
        df_formulas.iloc[:, 35:42] = df_formulas.iloc[:, 36:43].values
        df_formulas.iloc[:, 42] = ""  # Очистка последнего столбца
        df_formulas.iloc[:, 43:] = formulas_to_preserve

        # Преобразование обратно в список списков
        updated_values = [df_values.columns.tolist()] + df_values.values.tolist()
        updated_formulas = [df_formulas.columns.tolist()] + df_formulas.values.tolist()

        # Обновление таблицы одним запросом
        self.sheet.update('A1', updated_values, value_input_option='USER_ENTERED')
        self.sheet.update('A1', updated_formulas, value_input_option='USER_ENTERED')

    def shift_week_revenue_columns_to_the_left(self, last_week):
        """
        Сдвигает содержимое столбцов (AO-AR) с выручкой влево и добавляет новый день в AR.
        Функция задумана отрабатывать раз в день.
        Должна отрабатывать по условию если заголовок AR это позавчерашний день
        """

        all_values = self.sheet.get_all_values()
        all_formulas = self.sheet.get_all_values(value_render_option='FORMULA')

        # Преобразование в DataFrame
        df_values = pd.DataFrame(all_values[1:], columns=all_values[0])
        df_formulas = pd.DataFrame(all_formulas[1:], columns=all_values[0])

        # Сохраняем формулы из столбцов, которые не попадают в диапазон смещения
        formulas_to_preserve = df_formulas.iloc[:, 47:].values
        # Смещение заголовков и содержимого столбцов от "AO" до "AR"
        header_values = df_values.columns[43:47].tolist()  # Индексы столбцов "AO" до "AR"
        shifted_header_values = header_values[1:]
        shifted_header_values.append(last_week)

        # Обновление заголовков
        df_values.columns = df_values.columns[:43].tolist() + shifted_header_values + df_values.columns[47:].tolist()
        df_formulas.columns = df_values.columns  # Обновляем заголовки в формулах

        # Смещение содержимого столбцов от "AO" до "AR"
        df_values.iloc[:, 43:46] = df_values.iloc[:, 44:47].values
        df_values.iloc[:, 44] = ""  # Очистка последнего столбца "AR"

        # Восстанавливаем формулы в столбцах, которые не попадают в диапазон смещения
        df_formulas.iloc[:, 43:46] = df_formulas.iloc[:, 44:47].values
        df_formulas.iloc[:, 46] = ""  # Очистка последнего столбца "AM"
        df_formulas.iloc[:, 47:] = formulas_to_preserve

        # # Преобразование обратно в список списков
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
        print("week data added")

    def check_last_day_header_from_table(self, header):
        headers = self.sheet.row_values(1)
        if header not in headers:
            print(f"заголовка {header} нет в таблице")
            return True
        else:
            print(f"Заголовок {header} уже есть в таблице")

            return False

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
                print(datetime.now())
                print(e)
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
