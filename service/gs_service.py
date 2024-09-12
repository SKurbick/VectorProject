import datetime
import time
from pprint import pprint

import gspread.exceptions

from APIGoogleSheet.googlesheet import GoogleSheetServiceRevenue, GoogleSheet
from APIWildberries.analytics import AnalyticsNMReport
from APIWildberries.content import ListOfCardsContent
from APIWildberries.prices_and_discounts import ListOfGoodsPricesAndDiscounts
from APIWildberries.tariffs import CommissionTariffs
from settings import get_wb_tokens
from utils import add_orders_data, calculate_sum_for_logistic, merge_dicts, validate_data, add_nm_ids_in_db


class ServiceGoogleSheet:
    def __init__(self, token, spreadsheet: str, sheet: str, creds_json='creds.json'):
        self.wb_api_token = token
        self.gs_connect = GoogleSheet(creds_json=creds_json, spreadsheet=spreadsheet, sheet=sheet)
        # self.commission_traffics = CommissionTariffs(token=self.wb_api_token)
        # self.wb_api_content = ListOfCardsContent(token=self.wb_api_token)
        self.database = ...
        # self.analytics = AnalyticsNMReport(token=self.wb_api_token)
        # self.wb_api_price_and_discount = ListOfGoodsPricesAndDiscounts(token=self.wb_api_token)
        self.gs_service_revenue_connect = GoogleSheetServiceRevenue(creds_json=creds_json, spreadsheet=spreadsheet,
                                                                    sheet=sheet)
        self.sheet = sheet
        self.spreadsheet = spreadsheet
        self.creds_json = creds_json

    def add_revenue_for_new_nm_ids(self, lk_articles: dict):
        """ Добавление выручки по новым артикулам за 7 последних дней (сегоднешний не учитывается)"""
        nm_ids_revenue_data = {}
        for account, articles in lk_articles.items():
            # получаем токен и корректируем регистр для чтения из файла
            token = get_wb_tokens()[account.capitalize()]
            nm_ids_result = self.gs_connect.check_new_nm_ids(account=account, nm_ids=articles)
            if len(nm_ids_result) > 0:
                analytics = AnalyticsNMReport(token=token)
                revenue_data_by_article = analytics.get_last_days_revenue(nm_ids=articles,
                                                                          begin_date=datetime.date.today() - datetime.timedelta(
                                                                              days=7),
                                                                          end_date=datetime.date.today() - datetime.timedelta(
                                                                              days=1))
                """добавляет данные по выручке в БД"""
                add_orders_data(revenue_data_by_article)
                nm_ids_revenue_data.update(revenue_data_by_article)
        return nm_ids_revenue_data
        # self.gs_service_revenue_connect.add_for_all_new_nm_id_revenue(nm_ids_revenue_data=nm_ids_revenue_data)
        # todo сделать database class для add_orders_data
        # """добавляет данные по выручке в БД"""
        # add_orders_data(nm_ids_revenue_data)

    def add_new_data_from_table(self, lk_articles, edit_column_clean=None, only_edits_data=False,
                                add_data_in_db=True) -> dict:
        """Функция была изменена. Теперь она просто выдает данные на добавления в таблицу, а не изменяет внутри"""

        result_nm_ids_data = {}
        for account, nm_ids in lk_articles.items():
            token = get_wb_tokens()[account.capitalize()]

            print("поиск всех артикулов которых нет в БД")
            nm_ids_result = self.gs_connect.check_new_nm_ids(account=account, nm_ids=nm_ids)
            if len(nm_ids_result) > 0:
                """Обновление/добавление данных по артикулам в гугл таблицу с WB api"""
                wb_api_content = ListOfCardsContent(token=token)
                wb_api_price_and_discount = ListOfGoodsPricesAndDiscounts(token=token)
                card_from_nm_ids_filter = wb_api_content.get_list_of_cards(nm_ids_list=nm_ids, limit=100,
                                                                           only_edits_data=only_edits_data)
                goods_nm_ids = wb_api_price_and_discount.get_log_for_nm_ids(filter_nm_ids=nm_ids)
                commission_traffics = CommissionTariffs(token=token)
                # объединяем полученные данные
                merge_json_data = merge_dicts(goods_nm_ids, card_from_nm_ids_filter)

                subject_names = set()  # итог всех полученных с карточек предметов
                current_tariffs_data = commission_traffics.get_tariffs_box_from_marketplace()

                for i in merge_json_data.values():
                    subject_names.add(i["Предмет"])  # собираем множество с предметами

                    result_log_value = calculate_sum_for_logistic(  # на лету считаем "Логистика от склада WB до ПВЗ"
                        for_one_liter=int(current_tariffs_data["boxDeliveryBase"]),
                        next_liters=int(current_tariffs_data["boxDeliveryLiter"]),
                        height=int(i['Текущая\nВысота (см)']),
                        length=int(i['Текущая\nДлина (см)']),
                        width=int(i['Текущая\nШирина (см)']), )
                    i[
                        "Логистика от склада WB до ПВЗ"] = result_log_value  # добавляем результат вычислений в итоговые данные

                # получение комиссии WB
                subject_commissions = commission_traffics.get_commission_on_subject(subject_names=subject_names)

                # добавляем данные в merge_json_data
                for sc in subject_commissions.items():
                    for result_card in merge_json_data.values():
                        if sc[0] == result_card["Предмет"]:
                            result_card['Комиссия WB'] = sc[1]

                result_nm_ids_data.update(merge_json_data)

                if add_data_in_db is True:
                    """добавляем артикулы в БД"""
                    add_nm_ids_in_db(account=account, new_nm_ids=nm_ids)

        return result_nm_ids_data
        # """обновляем/добавляем данные по артикулам"""
        # self.gs_connect.update_rows(data_json=result_nm_ids_data, edit_column_clean=edit_column_clean)

    def change_cards_and_tables_data(self, edit_data_from_table):
        sheet_statuses = ServiceGoogleSheet.check_status()
        net_profit_status = sheet_statuses['Отрицательная \nЧП']
        price_discount_edit_status = sheet_statuses['Цены/Скидки']
        dimensions_edit_status = sheet_statuses['Габариты']
        updates_nm_ids_data = {}

        print("Получил данные по ячейкам на изменение товара")
        for account, nm_ids_data in edit_data_from_table.items():
            valid_data_result = validate_data(nm_ids_data)
            # пройдет если данные будут валидны для изменения

            if len(valid_data_result) > 0:
                print("Данные валидны")
                token = get_wb_tokens()[account.capitalize()]
                wb_api_price_and_discount = ListOfGoodsPricesAndDiscounts(token=token)
                wb_api_content = ListOfCardsContent(token=token)

                size_edit_data = []  # данные с артикулами на изменение габаритов
                price_discount_data = []  # данные с артикулами на изменение цены и/или цены

                for nm_id, data in valid_data_result.items():
                    # статус на изменение цены\скидки должен быть активным
                    if "price_discount" in data and price_discount_edit_status:
                        # если "Чистая прибыль" > выходит больше 0 или если статус редактирование по
                        # отрицательному ЧП стоит 1,то артикул, с запросом на изменение цены или скидки, будет добавлен
                        if data['net_profit'] >= 0 or net_profit_status:
                            price_discount_data.append(
                                {
                                    "nmID": nm_id,
                                    **data["price_discount"]
                                }
                            )
                    # статус на изменение габаритов должен быть активным
                    if "sizes" and "dimensions" in data and dimensions_edit_status:
                        size_edit_data.append(
                            {
                                "nmID": nm_id,
                                "vendorCode": data["vendorCode"],
                                "sizes": data["sizes"],
                                "dimensions": data["dimensions"]
                            }
                        )

                """запрос на изменение цены и/или скидки по артикулу"""
                # edit_column_clean = {"price_discount": True, "dimensions": False}
                print("price_discount_data", price_discount_data)
                if len(price_discount_data) > 0:
                    pd_bool_result = wb_api_price_and_discount.add_new_price_and_discount(price_discount_data)
                    # edit_column_clean["price_discount"] = pd_bool_result

                """Запрос на изменение габаритов товара по артикулу и vendorCode(артикул продавца)"""
                if len(size_edit_data) > 0:
                    c_bool_result = wb_api_content.size_edit(size_edit_data)
                    # edit_column_clean["dimensions"] = c_bool_result
                """Перезаписываем данные в таблице после их изменений на WB"""
                nm_ids_result = [int(nm_ids_str) for nm_ids_str in valid_data_result.keys()]
                updates_nm_ids_data.update({account: nm_ids_result})
        # если хоть по одному артикулу данные будут валидны...
        if len(updates_nm_ids_data) > 0:
            return self.add_new_data_from_table(lk_articles=updates_nm_ids_data,
                                                only_edits_data=True, add_data_in_db=False)
        return updates_nm_ids_data

    def add_new_day_revenue_to_table(self):
        last_day_bad_format = datetime.date.today() - datetime.timedelta(days=1)
        last_day = datetime.datetime.strftime(last_day_bad_format, "%d-%m-%Y")
        """Добавление нового дня в заголовки таблицы и выручки по этим дням и сдвиг последних шести дней влево"""
        # проверяем нет ли вчерашнего дня в заголовках таблицы
        print(f"проверяем {last_day}")
        if self.gs_service_revenue_connect.check_last_day_header_from_table(last_day=last_day):
            print(last_day, "заголовка нет в таблице. Будет добавлен включая выручку под дню")
            # сначала сдвигаем колонки с выручкой
            self.gs_service_revenue_connect.shift_revenue_columns_to_the_left()
            lk_articles = self.gs_connect.create_lk_articles_list()

            # собираем выручку по всем артикулам аккаунтов
            all_accounts_new_revenue_data = {}
            for account, articles in lk_articles.items():
                token = get_wb_tokens()[account.capitalize()]
                analytics = AnalyticsNMReport(token=token)
                # получаем данные по выручке с апи ВБ
                revenue_data_by_article = analytics.get_last_days_revenue(nm_ids=articles)
                all_accounts_new_revenue_data.update(revenue_data_by_article)
                pprint(revenue_data_by_article)
                # добавляем их таблицу
            self.gs_service_revenue_connect.add_last_day_revenue(nm_ids_revenue_data=all_accounts_new_revenue_data,
                                                                 last_day=last_day)
            print(f"добавлена выручка по новом заголовку {last_day} по всем артикулам")
            """добавляет данные по выручке в БД"""
            print("Выручка добавлена в БД")
            add_orders_data(all_accounts_new_revenue_data)

    @staticmethod
    def check_status():
        for i in range(10):
            try:

                sheet_status = GoogleSheet(creds_json="creds.json",
                                           spreadsheet="START Курбан", sheet="ВКЛ/ВЫКЛ Бот")
                return sheet_status.check_status_service_sheet()
            except gspread.exceptions.APIError as e:
                print(f"попытка {i}", e, "следующая попытка через 75 секунд")
                time.sleep(75)

        return False

    def add_actually_data_to_table(self):
        if ServiceGoogleSheet.check_status()['ВКЛ - 1 /ВЫКЛ - 0']:
            print("[INFO]", datetime.datetime.now(), "актуализируем данные в таблице")
            """
            Обновление данных по артикулам в гугл таблицу с WB api.
            Задумана, чтобы использовать в schedule.
            """
            gs_connect = GoogleSheet(creds_json=self.creds_json, spreadsheet=self.spreadsheet, sheet=self.sheet)
            lk_articles = gs_connect.create_lk_articles_list()
            print(lk_articles)
            result_updates_rows = {}
            for account, articles in lk_articles.items():
                print(account, articles)
                token = get_wb_tokens()[account.capitalize()]
                wb_api_content = ListOfCardsContent(token=token)
                wb_api_price_and_discount = ListOfGoodsPricesAndDiscounts(token=token)
                commission_traffics = CommissionTariffs(token=token)

                card_from_nm_ids_filter = wb_api_content.get_list_of_cards(nm_ids_list=articles, limit=100,
                                                                           only_edits_data=True, add_data_in_db=False)
                goods_nm_ids = wb_api_price_and_discount.get_log_for_nm_ids(filter_nm_ids=articles)
                # объединяем полученные данные
                merge_json_data = merge_dicts(goods_nm_ids, card_from_nm_ids_filter)
                subject_names = set()  # итог всех полученных с карточек предметов
                current_tariffs_data = commission_traffics.get_tariffs_box_from_marketplace()

                if len(merge_json_data) == 0:
                    print(f"По токену {account} не получили Артикулы с данным с API WB")
                    print(f"Артикулы:{articles}")
                    print(f"Результат с API WB {merge_json_data}")
                    continue  # пропускаем этот артикул
                for i in merge_json_data.values():
                    subject_names.add(i["Предмет"])  # собираем множество с предметами

                    result_log_value = calculate_sum_for_logistic(  # на лету считаем "Логистика от склада WB до ПВЗ"
                        for_one_liter=int(current_tariffs_data["boxDeliveryBase"]),
                        next_liters=int(current_tariffs_data["boxDeliveryLiter"]),
                        height=int(i['Текущая\nВысота (см)']),
                        length=int(i['Текущая\nДлина (см)']),
                        width=int(i['Текущая\nШирина (см)']), )
                    i[
                        "Логистика от склада WB до ПВЗ"] = result_log_value  # добавляем результат вычислений в итоговые данные

                # получение комиссии WB
                subject_commissions = commission_traffics.get_commission_on_subject(subject_names=subject_names)

                # добавляем данные в merge_json_data
                for sc in subject_commissions.items():
                    for result_card in merge_json_data.values():
                        if sc[0] == result_card["Предмет"]:
                            result_card['Комиссия WB'] = sc[1]

                result_updates_rows.update(merge_json_data)
                """обновляем данные по артикулам"""
            gs_connect.update_rows(data_json=result_updates_rows)
