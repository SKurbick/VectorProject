import json
from datetime import timedelta, datetime
from pprint import pprint

import gspread
from gspread import Client, service_account
from utils import get_nm_ids_in_db
import pandas as pd


class GoogleSheet:
    def __init__(self, spreadsheet: str, sheet: str, creds_json='creds.json'):
        self.creds_json = creds_json
        self.spreadsheet = spreadsheet
        self.sheet = sheet

    def client_init_json(self) -> Client:
        """Создание клиента для работы с Google Sheets."""
        return service_account(filename=self.creds_json)

    @staticmethod
    def get_table_by_id(client: Client, table_key):
        """Получение таблицы из Google Sheets по ID таблицы."""
        return client.open_by_key(table_key)

    def get_nm_ids(self):
        print(self.spreadsheet)
        client = self.client_init_json()
        print(client.http_client)
        spreadsheet = client.open(self.spreadsheet)
        sheet = spreadsheet.worksheet(self.sheet)

        column_index = None
        headers = sheet.row_values(1)
        if "Артикул" in headers:
            column_index = headers.index("Артикул") + 1

        column_data = sheet.col_values(column_index)
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

    def update_rows(self, data_json, edit_column_clean=None):

        client = self.client_init_json()
        spreadsheet = client.open(self.spreadsheet)
        sheet = spreadsheet.worksheet(self.sheet)
        data = sheet.get_all_records(expected_headers=[])
        df = pd.DataFrame(data)
        json_df = pd.DataFrame(list(data_json.values()))

        # # Обновите данные в основном DataFrame на основе "Артикул"
        # for index, row in json_df.iterrows():
        #     matching_rows = df[df["Артикул"] == row["Артикул"]].index
        #     for idx in matching_rows:
        #         if pd.isna(df.at[idx, "Фото"]) or df.at[idx, "Фото"] == "":
        #             df.at[idx, "Фото"] = row["Фото"]
        #         if pd.isna(df.at[idx, "Предмет"]) or df.at[idx, "Предмет"] == "":
        #             df.at[idx, "Предмет"] = row["Предмет"]
        #         if pd.isna(df.at[idx, "Артикул продавца"]) or df.at[idx, "Артикул продавца"] == "":
        #             df.at[idx, "Артикул продавца"] = row["Артикул продавца"]
        #         if pd.isna(df.at[idx, "Текущая\nДлина (см)"]) or df.at[idx, "Текущая\nДлина (см)"] == "":
        #             df.at[idx, "Текущая\nДлина (см)"] = row["Текущая\nДлина (см)"]
        #         if pd.isna(df.at[idx, "Текущая\nШирина (см)"]) or df.at[idx, "Текущая\nШирина (см)"] == "":
        #             df.at[idx, "Текущая\nШирина (см)"] = row["Текущая\nШирина (см)"]
        #         if pd.isna(df.at[idx, "Текущая\nВысота (см)"]) or df.at[idx, "Текущая\nВысота (см)"] == "":
        #             df.at[idx, "Текущая\nВысота (см)"] = row["Текущая\nВысота (см)"]
        #         if pd.isna(df.at[idx, "Скидка %"]) or df.at[idx, "Скидка %"] == "":
        #             df.at[idx, "Скидка %"] = row["Скидка %"]
        #         if pd.isna(df.at[idx, "Цена на WB без скидки"]) or df.at[idx, "Цена на WB без скидки"] == "":
        #             df.at[idx, "Цена на WB без скидки"] = row["Цена на WB без скидки"]
        #         if pd.isna(df.at[idx, "Комиссия WB"]) or df.at[idx, "Комиссия WB"] == "":
        #             df.at[idx, "Комиссия WB"] = row["Комиссия WB"]
        #         if pd.isna(df.at[idx, "Логистика от склада WB до ПВЗ"]) or df.at[
        #             idx, "Логистика от склада WB до ПВЗ"] == "":
        #             df.at[idx, "Логистика от склада WB до ПВЗ"] = row["Логистика от склада WB до ПВЗ"]
        #
        #         if edit_column_clean is not None:
        #             if edit_column_clean["price_discount"]:
        #                 df.at[idx, 'Установить новую скидку %'] = ""
        #                 df.at[idx, 'Установить новую цену'] = ""
        #
        #             if edit_column_clean["dimensions"]:
        #                 df.at[idx, 'Новая\nДлина (см)'] = ""
        #                 df.at[idx, 'Новая\nШирина (см)'] = ""
        #                 df.at[idx, 'Новая\nВысота (см)'] = ""
        #
        # # """'Установить новую цену', 'Установить новую скидку %',"""
        #
        # # Обновите Google Таблицу только для измененных строк
        # updates = []
        # for index, row in json_df.iterrows():
        #     matching_rows = df[df["Артикул"] == row["Артикул"]].index
        #     for idx in matching_rows:
        #         row_number = idx + 2  # +2 потому что индексация в Google Таблицах начинается с 1, а первая строка - заголовки
        #         updates.append({'range': f'B{row_number}', 'values': [[row["Фото"]]]})
        #         updates.append({'range': f'D{row_number}', 'values': [[row["Предмет"]]]})
        #         updates.append({'range': f'F{row_number}', 'values': [[row["Артикул продавца"]]]})
        #         updates.append({'range': f'P{row_number}', 'values': [[row["Текущая\nДлина (см)"]]]})
        #         updates.append({'range': f'Q{row_number}', 'values': [[row["Текущая\nШирина (см)"]]]})
        #         updates.append({'range': f'R{row_number}', 'values': [[row["Текущая\nВысота (см)"]]]})
        #         updates.append({'range': f'H{row_number}', 'values': [[row["Цена на WB без скидки"]]]})
        #         updates.append({'range': f'J{row_number}', 'values': [[row["Скидка %"]]]})
        #         updates.append({'range': f'V{row_number}', 'values': [[row["Комиссия WB"]]]})
        #         updates.append({'range': f'O{row_number}', 'values': [[row["Логистика от склада WB до ПВЗ"]]]})
        #
        #         # очистка столбца с изменяемыми данными
        #         if edit_column_clean is not None:
        #             if edit_column_clean["price_discount"]:
        #                 updates.append(
        #                     {'range': f'K{row_number}',
        #                      'values': [['']]})  # Очистка столбца 'Установить новую скидку %'
        #                 updates.append(
        #                     {'range': f'I{row_number}',
        #                      'values': [['']]})  # Очистка столбца 'Установить новую цену'
        #             if edit_column_clean["dimensions"]:
        #                 updates.append(
        #                     {'range': f'S{row_number}', 'values': [['']]})
        #                 updates.append(
        #                     {'range': f'T{row_number}', 'values': [['']]})
        #                 updates.append(
        #                     {'range': f'U{row_number}', 'values': [['']]})
        # sheet.batch_update(updates)
        # print("Данные успешно обновлены.")
        # return True
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

        # Обновите Google Таблицу только для измененных строк
        updates = []
        headers = df.columns.tolist()
        for index, row in json_df.iterrows():
            matching_rows = df[df["Артикул"] == row["Артикул"]].index
            for idx in matching_rows:
                row_number = idx + 2  # +2 потому что индексация в Google Таблицах начинается с 1, а первая строка - заголовки
                for column in row.index:
                    if column in headers:
                        column_index = headers.index(
                            column) + 1  # +1 потому что индексация в Google Таблицах начинается с 1
                        updates.append({'range': f'{chr(64 + column_index)}{row_number}', 'values': [[row[column]]]})

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


        sheet.batch_update(updates)
        print("Данные успешно обновлены.")
        return True

    def get_edit_data(self):
        """
        Получает данные с запросом на изменение с таблицы
        """
        client = self.client_init_json()
        spreadsheet = client.open(self.spreadsheet)
        sheet = spreadsheet.worksheet(self.sheet)

        data = sheet.get_all_values()

        # Преобразуйте данные в DataFrame
        df = pd.DataFrame(data[1:], columns=data[0])

        # Определите индексы столбцов по их названиям
        header_indices = {header: df.columns.get_loc(header) for header in df.columns}

        # Инициализация пустого словаря для результата
        result_data = {}

        # Перебор строк DataFrame
        for index, row in df.iterrows():
            article = row['Артикул']
            account = row['ЛК']
            # Пропуск строки, если "ЛК" или "Артикул" пустые
            if pd.isna(article) or pd.isna(article) or article.strip() == '' or article.strip() == '':
                continue
            # Создание словаря для текущего артикула
            article_dict = {
                "price_discount": {'Установить новую цену': row['Установить новую цену'],
                                   'Установить новую скидку %': row['Установить новую скидку %']},
                "dimensions": {
                    'Новая\nДлина (см)': row['Новая\nДлина (см)'],
                    'Новая\nШирина (см)': row['Новая\nШирина (см)'],
                    'Новая\nВысота (см)': row['Новая\nВысота (см)']},
                'Новый остаток': row['Новый остаток'],
                'Артикул продавца': row['Артикул продавца']
            }
            if account not in result_data:
                result_data[account] = {}
            # Добавление словаря в результирующий словарь
            result_data[account][article] = article_dict

        return result_data

    def create_lk_articles_list(self):
        """Создает словарь из ключей кабинета и его Артикулов"""
        client = self.client_init_json()
        spreadsheet = client.open(self.spreadsheet)
        sheet = spreadsheet.worksheet(self.sheet)
        data = sheet.get_all_records()
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

        # Обновление значений в таблице
        updates = []
        for row_index, row in enumerate(all_values[1:], start=2):  # Начинаем с 2, так как первая строка - заголовки
            article = row[article_col_index]
            if article in nm_ids_revenue_data:
                for date_col_name in nm_ids_revenue_data[article]:
                    if date_col_name in header_row:
                        date_col_index = header_row.index(date_col_name)
                        value = nm_ids_revenue_data[article][date_col_name]
                        cell = sheet.cell(row_index, date_col_index + 1)
                        updates.append({
                            'range': cell.address,
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

        # Находим индекс столбца "Артикул" и "03-09-2024"
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
