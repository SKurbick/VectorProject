import datetime
import time
from pprint import pprint

import gspread.exceptions

from APIGoogleSheet.googlesheet import GoogleSheetServiceRevenue, GoogleSheet, GoogleSheetSopostTable
from APIWildberries.analytics import AnalyticsNMReport
from APIWildberries.content import ListOfCardsContent
from APIWildberries.marketplace import WarehouseMarketplaceWB, LeftoversMarketplace
from APIWildberries.prices_and_discounts import ListOfGoodsPricesAndDiscounts
from APIWildberries.tariffs import CommissionTariffs
from settings import get_wb_tokens
from utils import add_orders_data, calculate_sum_for_logistic, merge_dicts, validate_data, add_nm_ids_in_db, \
    new_merge_dicts


class ServiceGoogleSheet:
    def __init__(self, token, spreadsheet: str, sheet: str, creds_json='creds.json'):
        self.wb_api_token = token
        self.gs_connect = GoogleSheet(creds_json=creds_json, spreadsheet=spreadsheet, sheet=sheet)
        self.database = ...
        self.gs_service_revenue_connect = GoogleSheetServiceRevenue(creds_json=creds_json, spreadsheet=spreadsheet,
                                                                    sheet=sheet)
        self.sheet = sheet
        self.spreadsheet = spreadsheet
        self.creds_json = creds_json

    def add_revenue_for_new_nm_ids(self, lk_articles: dict):
        """ Добавление выручки по новым артикулам за 7 последних дней (сегодняшний не учитывается)"""
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
                """добавляет данные по ежедневной выручке в БД"""
                add_orders_data(revenue_data_by_article)

                revenue_week_data_by_article = analytics.get_last_week_revenue(week_count=4, nm_ids=articles)

                nm_ids_revenue_data.update(revenue_data_by_article)

                for nm_id in revenue_week_data_by_article:
                    if nm_id in nm_ids_revenue_data:
                        nm_ids_revenue_data[nm_id].update(revenue_week_data_by_article[nm_id])

                """добавляем артикулы в БД"""
                # артикулы добавляем после получения выручки
                add_nm_ids_in_db(account=account, new_nm_ids=nm_ids_result)
        return nm_ids_revenue_data

    def add_new_data_from_table(self, lk_articles, edit_column_clean=None, only_edits_data=False,
                                add_data_in_db=True, check_nm_ids_in_db=True):
        """Функция была изменена. Теперь она просто выдает данные на добавления в таблицу, а не добавляет таблицу внутри функции"""

        nm_ids_photo = {}
        result_nm_ids_data = {}
        for account, nm_ids in lk_articles.items():
            token = get_wb_tokens()[account.capitalize()]
            nm_ids_result = nm_ids
            if check_nm_ids_in_db:
                "поиск всех артикулов которых нет в БД"
                nm_ids_result = self.gs_connect.check_new_nm_ids(account=account, nm_ids=nm_ids)
                if len(nm_ids_result) > 0:
                    print("КАБИНЕТ: ", account)
                    print("новые артикулы в таблице", nm_ids_result)

            if len(nm_ids_result) > 0:
                """Обновление/добавление данных по артикулам в гугл таблицу с WB api"""
                wb_api_content = ListOfCardsContent(token=token)
                wb_api_price_and_discount = ListOfGoodsPricesAndDiscounts(token=token)
                warehouses = WarehouseMarketplaceWB(token=token)
                barcodes_quantity = LeftoversMarketplace(token=token)
                card_from_nm_ids_filter = wb_api_content.get_list_of_cards(nm_ids_list=nm_ids_result, limit=100,
                                                                           only_edits_data=only_edits_data,
                                                                           account=account)
                goods_nm_ids = wb_api_price_and_discount.get_log_for_nm_ids(filter_nm_ids=nm_ids_result)
                commission_traffics = CommissionTariffs(token=token)
                # объединяем полученные данные
                merge_json_data = merge_dicts(card_from_nm_ids_filter, goods_nm_ids)
                # merge_json_data = new_merge_dicts(card_from_nm_ids_filter,goods_nm_ids)

                # print("MERGE JSON DATA")
                # pprint(merge_json_data)
                subject_names = set()  # итог предметов со всех карточек
                account_barcodes = []
                current_tariffs_data = commission_traffics.get_tariffs_box_from_marketplace()

                for i in merge_json_data.values():
                    # собираем и удаляем фото
                    if only_edits_data is False:
                        nm_ids_photo[int(i["Артикул"])] = i.pop("Фото", "НЕТ")
                        if i["wild"] != "не найдено":
                            subject_names.add(i["Предмет"])  # собираем множество с предметами
                            account_barcodes.append(i["Баркод"])
                            result_log_value = calculate_sum_for_logistic(
                                # на лету считаем "Логистика от склада WB до ПВЗ"
                                for_one_liter=int(current_tariffs_data["boxDeliveryBase"]),
                                next_liters=int(current_tariffs_data["boxDeliveryLiter"]),
                                height=int(i['Текущая\nВысота (см)']),
                                length=int(i['Текущая\nДлина (см)']),
                                width=int(i['Текущая\nШирина (см)']), )
                            # добавляем результат вычислений в итоговые данные
                            i["Логистика от склада WB до ПВЗ"] = result_log_value
                barcodes_quantity_result = []
                for warehouse_id in warehouses.get_account_warehouse():
                    bqs_result = barcodes_quantity.get_amount_from_warehouses(
                        warehouse_id=warehouse_id['id'],
                        barcodes=account_barcodes)
                    barcodes_quantity_result.extend(bqs_result)

                # получение комиссии WB
                subject_commissions = commission_traffics.get_commission_on_subject(subject_names=subject_names)

                for card in merge_json_data.values():
                    for sc in subject_commissions.items():
                        if sc[0] == card["Предмет"]:
                            card["Комиссия WB"] = sc[1]
                    for bq_result in barcodes_quantity_result:
                        if bq_result["Баркод"] == card["Баркод"]:
                            card["Текущий остаток"] = bq_result["остаток"]

                result_nm_ids_data.update(merge_json_data)

                if add_data_in_db is True:
                    """добавляем артикулы в БД"""
                    add_nm_ids_in_db(account=account, new_nm_ids=nm_ids_result)
        self.gs_connect.add_photo(nm_ids_photo)
        return result_nm_ids_data

    def change_cards_and_tables_data(self, edit_data_from_table):
        sheet_statuses = ServiceGoogleSheet.check_status()
        net_profit_status = sheet_statuses['Отрицательная \nЧП']
        price_discount_edit_status = sheet_statuses['Цены/Скидки']
        dimensions_edit_status = sheet_statuses['Габариты']
        quantity_edit_status = sheet_statuses['Остаток']
        updates_nm_ids_data = {}
        edit_column_clean = {"price_discount": True, "dimensions": False, "qty": False}

        print("Получил данные по ячейкам на изменение товара")
        for account, nm_ids_data in edit_data_from_table["nm_ids_edit_data"].items():
            valid_data_result = validate_data(nm_ids_data)
            token = get_wb_tokens()[account.capitalize()]
            warehouses = WarehouseMarketplaceWB(token=token)
            warehouses_qty_edit = LeftoversMarketplace(token=token)

            # пройдет если данные будут валидны для изменения
            if len(valid_data_result) > 0:
                print("Данные валидны")
                wb_api_price_and_discount = ListOfGoodsPricesAndDiscounts(token=token)
                wb_api_content = ListOfCardsContent(token=token)

                size_edit_data = []  # данные с артикулами на изменение габаритов
                price_discount_data = []  # данные с артикулами на изменение цены и/или цены
                quantity_edit_data = []
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
                if len(price_discount_data) > 0:
                    pd_bool_result = wb_api_price_and_discount.add_new_price_and_discount(price_discount_data)
                    edit_column_clean["price_discount"] = True

                """Запрос на изменение габаритов товара по артикулу и vendorCode (wild)"""
                if len(size_edit_data) > 0:
                    c_bool_result = wb_api_content.size_edit(size_edit_data)
                    edit_column_clean["dimensions"] = True
                """Перезаписываем данные в таблице после их изменений на WB"""
                nm_ids_result = [int(nm_ids_str) for nm_ids_str in valid_data_result.keys()]
                updates_nm_ids_data.update({account: nm_ids_result})

            if len(edit_data_from_table["qty_edit_data"][account]["stocks"]) > 0 and quantity_edit_status:
                "изменение остатков на всех складах продавца"
                for warehouse in warehouses.get_account_warehouse():
                    warehouses_qty_edit.edit_amount_from_warehouses(warehouse_id=warehouse["id"],
                                                                    edit_barcodes_list=
                                                                    edit_data_from_table["qty_edit_data"][
                                                                        account]["stocks"])
                edit_column_clean["qty"] = True
                # добавляем артикул для обновления данных
                if account not in updates_nm_ids_data:
                    updates_nm_ids_data[account] = []
                updates_nm_ids_data[account].append(*edit_data_from_table["qty_edit_data"][account]["nm_ids"])

        # если хоть по одному артикулу данные будут валидны...
        if len(updates_nm_ids_data):
            print(updates_nm_ids_data)
            # todo реализовать словарь для очистки столбцов на изменение данных для каждого столбца отдельно
            return self.add_new_data_from_table(lk_articles=updates_nm_ids_data,
                                                only_edits_data=True, add_data_in_db=False, check_nm_ids_in_db=False)
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
                revenue_data_by_article = analytics.get_last_days_revenue(nm_ids=articles,
                                                                          begin_date=last_day_bad_format,
                                                                          end_date=last_day_bad_format)

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
            result_updates_rows = {}
            for account, articles in lk_articles.items():
                token = get_wb_tokens()[account.capitalize()]
                wb_api_content = ListOfCardsContent(token=token)
                wb_api_price_and_discount = ListOfGoodsPricesAndDiscounts(token=token)
                warehouses = WarehouseMarketplaceWB(token=token)
                barcodes_quantity = LeftoversMarketplace(token=token)
                commission_traffics = CommissionTariffs(token=token)

                card_from_nm_ids_filter = wb_api_content.get_list_of_cards(nm_ids_list=articles, limit=100,
                                                                           only_edits_data=True, add_data_in_db=False)
                goods_nm_ids = wb_api_price_and_discount.get_log_for_nm_ids(filter_nm_ids=articles)
                # объединяем полученные данные
                merge_json_data = merge_dicts(goods_nm_ids, card_from_nm_ids_filter)
                subject_names = set()  # итог всех полученных с карточек предметов
                account_barcodes = []
                current_tariffs_data = commission_traffics.get_tariffs_box_from_marketplace()

                # если мы не получил данные по артикулам, то аккаунт будет пропущен
                if len(card_from_nm_ids_filter) == 0:
                    print(f"По токену {account} не получили Артикулы с данными с API WB")
                    print(f"Артикулы:{articles}")
                    print(f"Результат с API WB {merge_json_data}")
                    continue  # пропускаем этот аккаунт
                for i in merge_json_data.values():
                    if i["wild"] != "не найдено":
                        account_barcodes.append(i["Баркод"])
                        subject_names.add(i["Предмет"])  # собираем множество с предметами
                        result_log_value = calculate_sum_for_logistic(
                            # на лету считаем "Логистика от склада WB до ПВЗ"
                            for_one_liter=int(current_tariffs_data["boxDeliveryBase"]),
                            next_liters=int(current_tariffs_data["boxDeliveryLiter"]),
                            height=int(i['Текущая\nВысота (см)']),
                            length=int(i['Текущая\nДлина (см)']),
                            width=int(i['Текущая\nШирина (см)']), )
                        # добавляем результат вычислений в итоговые данные
                        i["Логистика от склада WB до ПВЗ"] = result_log_value

                barcodes_quantity_result = []
                for warehouse_id in warehouses.get_account_warehouse():
                    bqs_result = barcodes_quantity.get_amount_from_warehouses(
                        warehouse_id=warehouse_id['id'],
                        barcodes=account_barcodes)
                    barcodes_quantity_result.extend(bqs_result)

                # получение комиссии WB
                subject_commissions = commission_traffics.get_commission_on_subject(subject_names=subject_names)

                # добавляем данные в merge_json_data
                for card in merge_json_data.values():
                    for sc in subject_commissions.items():
                        if sc[0] == card["Предмет"]:
                            card["Комиссия WB"] = sc[1]
                    for bq_result in barcodes_quantity_result:
                        if bq_result["Баркод"] == card["Баркод"]:
                            card["Текущий остаток"] = bq_result["остаток"]

                result_updates_rows.update(merge_json_data)
                """обновляем данные по артикулам"""
            gs_connect.update_rows(data_json=result_updates_rows)

    def check_quantity_flag(self):
        print("Проверка остатков по лимитам из столбца 'Минимальный остаток'")
        status_limit_edit = ServiceGoogleSheet.check_status()["Добавить если"]
        print("статус проверки: ", status_limit_edit)
        low_limit_qty_data = self.gs_connect.get_data_quantity_limit()
        sopost_data = GoogleSheetSopostTable().wild_quantity()
        nm_ids_for_update_data = {}
        if len(low_limit_qty_data) > 0 and status_limit_edit:
            print("Есть остатки ниже установленного флага")
            for account, edit_data in low_limit_qty_data.items():
                update_qty_data = []
                token = get_wb_tokens()[account.capitalize()]
                warehouses = WarehouseMarketplaceWB(token=token).get_account_warehouse()
                qty_edit = LeftoversMarketplace(token=token)

                for qty_data in edit_data["qty"]:
                    if sopost_data[qty_data["wild"]] and str(sopost_data[qty_data["wild"]]) != "0":
                        update_qty_data.append(
                            {
                                "sku": qty_data["sku"],
                                "amount": int(sopost_data[qty_data["wild"]])
                            }
                        )

                for warehouse_id in warehouses:
                    qty_edit.edit_amount_from_warehouses(warehouse_id=warehouse_id["id"],
                                                         edit_barcodes_list=update_qty_data)

                if account not in nm_ids_for_update_data:
                    nm_ids_for_update_data[account] = []
                nm_ids_for_update_data[account].append(*low_limit_qty_data[account]['nm_ids'])

        nm_ids_data_json = self.add_new_data_from_table(lk_articles=nm_ids_for_update_data,
                                                        only_edits_data=True, add_data_in_db=False,
                                                        check_nm_ids_in_db=False)
        self.gs_connect.update_rows(data_json=nm_ids_data_json,
                                    edit_column_clean={"qty": True, "price_discount": False, "dimensions": False})
