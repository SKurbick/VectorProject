import datetime
from pprint import pprint

from APIGoogleSheet.googlesheet import GoogleSheetServiceRevenue, GoogleSheet
from APIWildberries.analytics import AnalyticsNMReport
from APIWildberries.content import ListOfCardsContent
from APIWildberries.prices_and_discounts import ListOfGoodsPricesAndDiscounts
from APIWildberries.tariffs import CommissionTariffs
from settings import get_wb_tokens
from utils import add_orders_data, calculate_sum_for_logistic, merge_dicts


class ServiceGoogleSheet:
    def __init__(self, token, spreadsheet: str, sheet: str, creds_json='creds.json'):
        self.wb_api_token = token
        self.gs_connect = GoogleSheet(creds_json=creds_json, spreadsheet=spreadsheet, sheet=sheet)
        self.commission_traffics = CommissionTariffs(token=self.wb_api_token)
        self.wb_api_content = ListOfCardsContent(token=self.wb_api_token)
        self.database = ...
        self.analytics = AnalyticsNMReport(token=self.wb_api_token)
        self.wb_api_price_and_discount = ListOfGoodsPricesAndDiscounts(token=self.wb_api_token)
        self.gs_service_revenue_connect = GoogleSheetServiceRevenue(creds_json=creds_json, spreadsheet=spreadsheet,
                                                                    sheet=sheet)
        self.sheet = sheet
        self.spreadsheet = spreadsheet
        self.creds_json = creds_json

    def add_revenue_for_new_nm_ids(self, nm_ids: list):
        """ Добавление выручки по новым артикулам за 7 последних дней (сегоднешний не учитывается)"""

        nm_ids_revenue_data = self.analytics.get_last_days_revenue(nm_ids=nm_ids,
                                                                   begin_date=datetime.date.today() - datetime.timedelta(
                                                                       days=7),
                                                                   end_date=datetime.date.today() - datetime.timedelta(
                                                                       days=1))
        """добавляет данные по выручке в БД"""
        add_orders_data(nm_ids_revenue_data)
        return nm_ids_revenue_data
        # self.gs_service_revenue_connect.add_for_all_new_nm_id_revenue(nm_ids_revenue_data=nm_ids_revenue_data)
        # todo сделать database class для add_orders_data
        # """добавляет данные по выручке в БД"""
        # add_orders_data(nm_ids_revenue_data)

    def add_new_data_from_table(self, nm_ids, edit_column_clean=None, only_edits_data=False) -> dict:
        """Обновление/добавление данных по артикулам в гугл таблицу с WB api"""
        card_from_nm_ids_filter = self.wb_api_content.get_list_of_cards(nm_ids_list=nm_ids, limit=100,
                                                                        only_edits_data=only_edits_data)
        goods_nm_ids = self.wb_api_price_and_discount.get_log_for_nm_ids(filter_nm_ids=nm_ids)

        # объединяем полученные данные
        merge_json_data = merge_dicts(goods_nm_ids, card_from_nm_ids_filter)

        subject_names = set()  # итог всех полученных с карточек предметов
        current_tariffs_data = self.commission_traffics.get_tariffs_box_from_marketplace()

        for i in merge_json_data.values():
            subject_names.add(i["Предмет"])  # собираем множество с предметами

            result_log_value = calculate_sum_for_logistic(  # на лету считаем "Логистика от склада WB до ПВЗ"
                for_one_liter=int(current_tariffs_data["boxDeliveryBase"]),
                next_liters=int(current_tariffs_data["boxDeliveryLiter"]),
                height=int(i['Текущая\nВысота (см)']),
                length=int(i['Текущая\nДлина (см)']),
                width=int(i['Текущая\nШирина (см)']), )
            i["Логистика от склада WB до ПВЗ"] = result_log_value  # добавляем результат вычислений в итоговые данные

        # получение комиссии WB
        subject_commissions = self.commission_traffics.get_commission_on_subject(subject_names=subject_names)

        # добавляем данные в merge_json_data
        for sc in subject_commissions.items():
            for result_card in merge_json_data.values():
                if sc[0] == result_card["Предмет"]:
                    result_card['Комиссия WB'] = sc[1]

        return merge_json_data
        # """обновляем/добавляем данные по артикулам"""
        # self.gs_connect.update_rows(data_json=merge_json_data, edit_column_clean=edit_column_clean)

    def change_cards_and_tables_data(self, valid_data):

        size_edit_data = []  # данные с артикулами на изменение габаритов
        price_discount_data = []  # данные с артикулами на изменение цены и/или цены

        for nm_id, data in valid_data.items():

            if "price_discount" in data:
                price_discount_data.append(
                    {
                        "nmID": nm_id,
                        **data["price_discount"]
                    }
                )
            if "sizes" and "dimensions" in data:
                size_edit_data.append(
                    {
                        "nmID": nm_id,
                        "vendorCode": data["vendorCode"],
                        "sizes": data["sizes"],
                        "dimensions": data["dimensions"]
                    }
                )

        """запрос на изменение цены и/или скидки по артикулу"""
        edit_column_clean = {"price_discount": False, "dimensions": False}
        if len(price_discount_data) > 0:
            pd_bool_result = self.wb_api_price_and_discount.add_new_price_and_discount(price_discount_data)
            edit_column_clean["price_discount"] = pd_bool_result

        """Запрос на изменение габаритов товара по артикулу и vendorCode(артикул продавца)"""
        if len(size_edit_data) > 0:
            c_bool_result = self.wb_api_content.size_edit(size_edit_data)
            edit_column_clean["dimensions"] = c_bool_result
        """Перезаписываем данные в таблице после их изменений на WB"""
        nm_ids_result = [int(nm_ids_str) for nm_ids_str in valid_data.keys()]
        return self.add_new_data_from_table(nm_ids=nm_ids_result, edit_column_clean=edit_column_clean, only_edits_data=True)

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
                print("Добавляем данные за вчерашний день выручки со сдвигом колонок")
                # получаем данные по выручке с апи ВБ
                revenue_data_by_article = analytics.get_last_days_revenue(nm_ids=articles)
                all_accounts_new_revenue_data.update(revenue_data_by_article)
                pprint(revenue_data_by_article)
                # добавляем их таблицу
            self.gs_service_revenue_connect.add_last_day_revenue(nm_ids_revenue_data=all_accounts_new_revenue_data,
                                                                 last_day=last_day)
            print(f"добавлена выручка по новом заголовку {last_day} по всем артикулам")

    def add_actually_data_to_table(self):
        print("[INFO]",datetime.datetime.now(),"актуализируем данные в таблице")
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
