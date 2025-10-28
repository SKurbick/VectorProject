import asyncio
import json
import time
from datetime import datetime
from pprint import pprint
from typing import List, Dict, Any

import pandas
from gspread.utils import rowcol_to_a1

import gspread
import requests
from gspread import Client, service_account
from utils import get_nm_ids_in_db, column_index_to_letter, can_be_int
import pandas as pd

from logger import app_logger as logger

def safe_batch_update(
        sheet: gspread.Worksheet,
        updates: List[Dict[str, Any]],
        chunk_size: int = 3000,
        max_retries: int = 5,
        start_chunk: int = 1  # Новый параметр - начинать с указанного чанка
) -> None:
    """
    Безопасное массовое обновление данных в Google Sheets с retry-логикой

    Args:
        sheet: Объект gspread Worksheet
        updates: Список словарей с обновлениями формата {'range': 'A1', 'values': [[value]]}
        chunk_size: Размер chunk'а (по умолчанию 3000)
        max_retries: Максимальное количество попыток (по умолчанию 5)
        start_chunk: Номер чанка, с которого начать обновление (по умолчанию 1)
    """
    total_updates = len(updates)
    if total_updates == 0:
        logger.info("Нет обновлений для выполнения")
        return

    total_chunks = (total_updates + chunk_size - 1) // chunk_size

    # Проверяем валидность start_chunk
    if start_chunk < 1:
        start_chunk = 1
    elif start_chunk > total_chunks:
        logger.info(f"start_chunk ({start_chunk}) превышает общее количество чанков ({total_chunks}). Ничего не обновляем.")
        return

    # Вычисляем стартовый индекс для среза updates
    start_index = (start_chunk - 1) * chunk_size

    logger.info(f"Начинаем обновление: {total_updates} ячеек, {total_chunks} chunks, начиная с chunk {start_chunk}")

    for chunk_index in range(start_index, total_updates, chunk_size):
        chunk = updates[chunk_index:chunk_index + chunk_size]
        chunk_number = chunk_index // chunk_size + 1

        for attempt in range(max_retries):
            try:
                sheet.batch_update(chunk)
                logger.info(
                    f"Успешно обновлен chunk {chunk_number}/{total_chunks} "
                    f"({len(chunk)} ячеек, всего {min(chunk_index + chunk_size, total_updates)}/{total_updates})"
                )
                break  # Успех, переходим к следующему chunk

            except gspread.exceptions.APIError as e:
                if '503' in str(e) and attempt < max_retries - 1:
                    wait_time = 10 ** attempt  # Экспоненциальная задержка
                    logger.warning(
                        f"Ошибка 503 в chunk {chunk_number}, "
                        f"попытка {attempt + 1}/{max_retries}, жду {wait_time} сек"
                    )
                    time.sleep(wait_time)
                else:
                    logger.error(
                        f"Не удалось обновить chunk {chunk_number} после {max_retries} попыток: {e}"
                    )
                    raise e
        else:
            logger.error(f"Chunk {chunk_number} не удалось обновить после {max_retries} попыток")
            raise Exception(f"Failed to update chunk {chunk_number} after {max_retries} attempts")

    logger.info(f"Все обновления завершены успешно: {total_updates} ячеек")

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
                print(sheet, "sheet")
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

    def get_article_and_profit_data_alternative(self) -> list[tuple]:
        """
        Альтернативный метод получения данных.
        Более устойчив к небольшим различиям в названиях колонок.
        """
        try:
            # Получаем первую строку с заголовками
            headers = self.sheet.row_values(1)

            # Находим индексы нужных колонок
            article_idx = -1
            profit_idx = -1

            for i, header in enumerate(headers):
                header_lower = header.lower().strip()
                if 'артикул' in header_lower:
                    article_idx = i
                elif 'среднее чп за 23' in header_lower:
                    profit_idx = i

            if article_idx == -1 or profit_idx == -1:
                logger.error("Не найдены нужные колонки в таблице")
                return []

            # Получаем все данные, начиная со второй строки
            all_data = self.sheet.get_all_values()[1:]

            result = []
            for row in all_data:
                if len(row) > max(article_idx, profit_idx):
                    article = row[article_idx].strip()
                    profit = row[profit_idx].strip()

                    # Пропускаем пустые значения
                    if not article or not profit:
                        continue

                    # Преобразуем оба значения в числовой формат
                    try:
                        # Преобразуем артикул в int
                        article_clean = article.replace(' ', '').replace(',', '')
                        article_value = int(article_clean)

                        # Преобразуем profit в int
                        profit_clean = profit.replace(' ', '').replace(',', '.')
                        profit_value = int(float(profit_clean))  # Сначала в float, потом в int для чисел с точкой

                        result.append((article_value, profit_value))

                    except (ValueError, TypeError) as e:
                        # Логируем ошибку преобразования, но пропускаем строку
                        logger.warning(f"Не удалось преобразовать значения: артикул='{article}', прибыль='{profit}'. Ошибка: {e}")
                        continue

            return result

        except Exception as e:
            logger.error(f"Ошибка при получении данных: {e}")
            return []





    def insert_wild_data_correct(self, data_dict: dict,sheet_header="wild") -> None:
        """
        Оптимизированная версия - обновляет данные целыми столбцами.
        """
        try:
            # Получаем заголовки таблицы
            headers = self.sheet.row_values(1)
            # print(headers)
            # Находим индекс колонки wild
            wild_col_idx = None
            for idx, header in enumerate(headers):
                if sheet_header in header.lower():
                    wild_col_idx = idx
                    print(wild_col_idx)

            if wild_col_idx is None:

                logger.error(f"Колонка {sheet_header} не найдена в таблице")
                return

            # Находим индексы и диапазон наших целевых колонок
            # target_headers = list(next(iter(data_dict.values())).keys()) if data_dict else []
            target_headers = list(set().union(*(item.keys() for item in data_dict.values())))
            print(f"Все целевые заголовки: {target_headers}")

            target_indices = []

            for header in target_headers:
                if header in headers:
                    target_indices.append(headers.index(header))

            if not target_indices:
                logger.error("Целевые заголовки не найдены в таблице")
                return

            # Сортируем индексы и проверяем, что они идут подряд
            target_indices.sort()
            is_consecutive = all(target_indices[i] + 1 == target_indices[i + 1]
                                 for i in range(len(target_indices) - 1))

            # Получаем все данные таблицы
            all_data = self.sheet.get_all_values()

            # Создаем матрицу для обновления (строки x колонки)
            updates = []

            if is_consecutive and len(target_indices) > 1:
                # ОПТИМИЗАЦИЯ: обновляем целым диапазоном столбцов
                start_col = target_indices[0]
                end_col = target_indices[-1]

                # ПРАВИЛЬНО формируем диапазон: "AX2:BA5886"
                start_col_letter = self.get_column_letter(start_col + 1)
                end_col_letter = self.get_column_letter(end_col + 1)
                update_range = f"{start_col_letter}2:{end_col_letter}{len(all_data)}"

                logger.info(f"Обновляем диапазон: {update_range}")

                # Создаем матрицу обновлений
                update_matrix = [['' for _ in range(len(target_indices))] for _ in range(len(all_data) - 1)]

                # Заполняем матрицу данными
                for row_idx in range(1, len(all_data)):
                    row = all_data[row_idx]
                    if len(row) > wild_col_idx:
                        current_wild = row[wild_col_idx]
                        if current_wild in data_dict:
                            wild_data = data_dict[current_wild]
                            for i, col_idx in enumerate(target_indices):
                                header = headers[col_idx]
                                if header in wild_data:
                                    update_matrix[row_idx - 1][i] = wild_data[header]
                                else:
                                    # Сохраняем оригинальное значение если нет в словаре
                                    update_matrix[row_idx - 1][i] = row[col_idx] if col_idx < len(row) else ''
                        else:
                            # Сохраняем оригинальные значения для строк без совпадения
                            for i, col_idx in enumerate(target_indices):
                                update_matrix[row_idx - 1][i] = row[col_idx] if col_idx < len(row) else ''
                    else:
                        # Для строк без wild данных
                        for i, col_idx in enumerate(target_indices):
                            update_matrix[row_idx - 1][i] = row[col_idx] if col_idx < len(row) else ''

                updates.append({
                    'range': update_range,
                    'values': update_matrix
                })
            else:
                # Если колонки не подряд, обновляем каждую колонку отдельно
                for col_idx in target_indices:
                    header = headers[col_idx]
                    col_letter = self.get_column_letter(col_idx + 1)
                    # ПРАВИЛЬНЫЙ формат: "AX2:AX5886"
                    col_range = f"{col_letter}2:{col_letter}{len(all_data)}"

                    logger.info(f"Обновляем колонку: {col_range}")

                    # Подготавливаем данные для столбца
                    column_data = []
                    for row_idx in range(1, len(all_data)):
                        row = all_data[row_idx]
                        if len(row) > wild_col_idx:
                            current_wild = row[wild_col_idx]
                            if current_wild in data_dict and header in data_dict[current_wild]:
                                column_data.append([data_dict[current_wild][header]])
                            else:
                                column_data.append([row[col_idx] if col_idx < len(row) else ''])
                        else:
                            column_data.append([row[col_idx] if col_idx < len(row) else ''])

                    updates.append({
                        'range': col_range,
                        'values': column_data
                    })

            # pprint(updates)

            # Выполняем обновление
            if updates:
                for i, update in enumerate(updates):
                    try:
                        self.sheet.update(update['range'], update['values'], value_input_option='USER_ENTERED')
                        logger.info(f"Успешно обновлен диапазон {update['range']} ({i + 1}/{len(updates)})")
                    except Exception as e:
                        logger.error(f"Ошибка при обновлении {update['range']}: {e}")
                        # Можно добавить повторные попытки или продолжить

        except Exception as e:
            logger.error(f"Ошибка при вставке данных: {e}")
            raise

    @staticmethod
    def get_column_letter( col_idx: int) -> str:
        """Конвертирует индекс колонки в букву (A, B, C, ...)"""
        result = ""
        while col_idx > 0:
            col_idx, remainder = divmod(col_idx - 1, 26)
            result = chr(65 + remainder) + result
        return result

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

        # self.sheet.batch_update(updates)
        safe_batch_update(
            sheet=self.sheet,
            updates=updates,
            chunk_size=1000,  # Можно настроить под свои needs
            max_retries=5,  # Можно настроить количество попыток
        )
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
        # self.sheet.batch_update(updates)
        safe_batch_update(
            sheet=self.sheet,
            updates=updates,
            chunk_size=1000,  # Можно настроить под свои needs
            max_retries=5  # Можно настроить количество попыток
            # start_chunk=10
        )
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
                if str(min_qty).isdigit() and int(min_qty) != 0 and str(current_qty).isdigit():
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

        # self.sheet.batch_update(updates)
        safe_batch_update(
            sheet=self.sheet,
            updates=updates,
            chunk_size=1000,  # Можно настроить под свои needs
            max_retries=5,  # Можно настроить количество попыток
        )
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
        formulas_to_preserve = df_formulas.iloc[:, 135:].values

        # Смещение заголовков и содержимого столбцов
        header_values = df_values.columns[105:135].tolist()  # Индексы столбцов
        shifted_header_values = header_values[:29]
        shifted_header_values.insert(0, today)
        # Обновление заголовков
        df_values.columns = df_values.columns[:105].tolist() + shifted_header_values + df_values.columns[135:].tolist()
        df_formulas.columns = df_values.columns  # Обновляем заголовки в формулах
        # Смещение содержимого столбцов
        df_values.iloc[:, 106:135] = df_values.iloc[:, 105:134].values
        df_values.iloc[:, 105] = ""  # Очистка первого столбца

        # Восстанавливаем формулы в столбцах, которые не попадают в диапазон смещения
        df_formulas.iloc[:, 106:135] = df_formulas.iloc[:, 105:134].values
        df_formulas.iloc[:, 105] = ""  # Очистка первого столбца
        df_formulas.iloc[:, 135:] = formulas_to_preserve

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
            # self.sheet.batch_update(updates)
            safe_batch_update(
                sheet=self.sheet,
                updates=updates,
                chunk_size=1000,  # Можно настроить под свои needs
                max_retries=5,  # Можно настроить количество попыток
            )
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

        # self.sheet.batch_update(updates)
        safe_batch_update(
            sheet=self.sheet,
            updates=updates,
            chunk_size=1000,  # Можно настроить под свои needs
            max_retries=5,  # Можно настроить количество попыток
        )

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



    def insert_wild_data_correct(self, data_dict: dict,sheet_header="wild") -> None:
        """
        Оптимизированная версия - обновляет данные целыми столбцами.
        """
        try:
            # Получаем заголовки таблицы
            headers = self.sheet.row_values(1)
            print(headers)
            # Находим индекс колонки wild
            wild_col_idx = None
            for idx, header in enumerate(headers):
                if sheet_header in header.lower():
                    wild_col_idx = idx
                    print(wild_col_idx)

            if wild_col_idx is None:

                logger.error(f"Колонка {sheet_header} не найдена в таблице")
                return

            # Находим индексы и диапазон наших целевых колонок
            target_headers = list(next(iter(data_dict.values())).keys()) if data_dict else []
            target_indices = []

            for header in target_headers:
                if header in headers:
                    target_indices.append(headers.index(header))

            if not target_indices:
                logger.error("Целевые заголовки не найдены в таблице")
                return

            # Сортируем индексы и проверяем, что они идут подряд
            target_indices.sort()
            is_consecutive = all(target_indices[i] + 1 == target_indices[i + 1]
                                 for i in range(len(target_indices) - 1))

            # Получаем все данные таблицы
            all_data = self.sheet.get_all_values()

            # Создаем матрицу для обновления (строки x колонки)
            updates = []

            if is_consecutive and len(target_indices) > 1:
                # ОПТИМИЗАЦИЯ: обновляем целым диапазоном столбцов
                start_col = target_indices[0]
                end_col = target_indices[-1]

                # ПРАВИЛЬНО формируем диапазон: "AX2:BA5886"
                start_col_letter = self.get_column_letter(start_col + 1)
                end_col_letter = self.get_column_letter(end_col + 1)
                update_range = f"{start_col_letter}2:{end_col_letter}{len(all_data)}"

                logger.info(f"Обновляем диапазон: {update_range}")

                # Создаем матрицу обновлений
                update_matrix = [['' for _ in range(len(target_indices))] for _ in range(len(all_data) - 1)]

                # Заполняем матрицу данными
                for row_idx in range(1, len(all_data)):
                    row = all_data[row_idx]
                    if len(row) > wild_col_idx:
                        current_wild = row[wild_col_idx]
                        if current_wild in data_dict:
                            wild_data = data_dict[current_wild]
                            for i, col_idx in enumerate(target_indices):
                                header = headers[col_idx]
                                if header in wild_data:
                                    update_matrix[row_idx - 1][i] = wild_data[header]
                                else:
                                    # Сохраняем оригинальное значение если нет в словаре
                                    update_matrix[row_idx - 1][i] = row[col_idx] if col_idx < len(row) else ''
                        else:
                            # Сохраняем оригинальные значения для строк без совпадения
                            for i, col_idx in enumerate(target_indices):
                                update_matrix[row_idx - 1][i] = row[col_idx] if col_idx < len(row) else ''
                    else:
                        # Для строк без wild данных
                        for i, col_idx in enumerate(target_indices):
                            update_matrix[row_idx - 1][i] = row[col_idx] if col_idx < len(row) else ''

                updates.append({
                    'range': update_range,
                    'values': update_matrix
                })
            else:
                # Если колонки не подряд, обновляем каждую колонку отдельно
                for col_idx in target_indices:
                    header = headers[col_idx]
                    col_letter = self.get_column_letter(col_idx + 1)
                    # ПРАВИЛЬНЫЙ формат: "AX2:AX5886"
                    col_range = f"{col_letter}2:{col_letter}{len(all_data)}"

                    logger.info(f"Обновляем колонку: {col_range}")

                    # Подготавливаем данные для столбца
                    column_data = []
                    for row_idx in range(1, len(all_data)):
                        row = all_data[row_idx]
                        if len(row) > wild_col_idx:
                            current_wild = row[wild_col_idx]
                            if current_wild in data_dict and header in data_dict[current_wild]:
                                column_data.append([data_dict[current_wild][header]])
                            else:
                                column_data.append([row[col_idx] if col_idx < len(row) else ''])
                        else:
                            column_data.append([row[col_idx] if col_idx < len(row) else ''])

                    updates.append({
                        'range': col_range,
                        'values': column_data
                    })

            pprint(updates)

            # Выполняем обновление
            if updates:
                for i, update in enumerate(updates):
                    try:
                        self.sheet.update(update['range'], update['values'], value_input_option='USER_ENTERED')
                        logger.info(f"Успешно обновлен диапазон {update['range']} ({i + 1}/{len(updates)})")
                    except Exception as e:
                        logger.error(f"Ошибка при обновлении {update['range']}: {e}")
                        # Можно добавить повторные попытки или продолжить

        except Exception as e:
            logger.error(f"Ошибка при вставке данных: {e}")
            raise

    @staticmethod
    def get_column_letter( col_idx: int) -> str:
        """Конвертирует индекс колонки в букву (A, B, C, ...)"""
        result = ""
        while col_idx > 0:
            col_idx, remainder = divmod(col_idx - 1, 26)
            result = chr(65 + remainder) + result
        return result








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
        # self.sheet.batch_update(updates)
        safe_batch_update(
            sheet=self.sheet,
            updates=updates,
            chunk_size=1000,  # Можно настроить под свои needs
            max_retries=5,  # Можно настроить количество попыток
        )
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
        # self.sheet.batch_update(updates, value_input_option="USER_ENTERED")
        safe_batch_update(
            sheet=self.sheet,
            updates=updates,
            chunk_size=1000,  # Можно настроить под свои needs
            max_retries=5,  # Можно настроить количество попыток
        )
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

        # self.sheet.batch_update(updates)
        safe_batch_update(
            sheet=self.sheet,
            updates=updates,
            chunk_size=1000,  # Можно настроить под свои needs
            max_retries=5,  # Можно настроить количество попыток
        )
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

        # self.sheet.batch_update(updates)
        safe_batch_update(
            sheet=self.sheet,
            updates=updates,
            chunk_size=1000,  # Можно настроить под свои needs
            max_retries=5,  # Можно настроить количество попыток
        )

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
        # self.sheet.batch_update(updates)
        safe_batch_update(
            sheet=self.sheet,
            updates=updates,
            chunk_size=1000,  # Можно настроить под свои needs
            max_retries=5,  # Можно настроить количество попыток
        )
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
