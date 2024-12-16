import asyncio
import datetime
import time
from pprint import pprint

import gspread.exceptions
import requests

import settings
from APIGoogleSheet.googlesheet import GoogleSheetServiceRevenue, GoogleSheet, GoogleSheetSopostTable, PCGoogleSheet, \
    update_columns_in_purchase_calculation
from APIWildberries.analytics import AnalyticsNMReport, AnalyticsWarehouseLimits
from APIWildberries.content import ListOfCardsContent
from APIWildberries.marketplace import WarehouseMarketplaceWB, LeftoversMarketplace
from APIWildberries.prices_and_discounts import ListOfGoodsPricesAndDiscounts
from APIWildberries.tariffs import CommissionTariffs
from database.postgresql.repositories.accurate_net_profit_data import AccurateNetProfitTable
from database.postgresql.repositories.accurate_npd_purchase_calculation import AccurateNetProfitPCTable
from database.postgresql.repositories.article import ArticleTable
from settings import get_wb_tokens
from settings import Setting
from utils import add_orders_data, calculate_sum_for_logistic, merge_dicts, validate_data, add_nm_ids_in_db, \
    get_last_weeks_dates

from database.postgresql.database import Database, Database1


class ServiceGoogleSheet:
    def __init__(self, token, spreadsheet: str, sheet: str, creds_json='creds.json'):
        self.wb_api_token = token
        self.gs_connect = GoogleSheet(creds_json=creds_json, spreadsheet=spreadsheet, sheet=sheet)
        self.gs_connect_pc = PCGoogleSheet(creds_json=creds_json, spreadsheet=Setting().PC_SPREADSHEET,
                                           sheet=Setting().PC_SHEET)
        self.gs_service_revenue_connect = GoogleSheetServiceRevenue(creds_json=creds_json, spreadsheet=spreadsheet,
                                                                    sheet=sheet)
        self.sheet = sheet
        self.spreadsheet = spreadsheet
        self.creds_json = creds_json

    async def add_revenue_for_new_nm_ids(self, lk_articles: dict):
        """ Добавление выручки по новым артикулам за 7 последних дней (сегодняшний не учитывается)"""
        print("Смотрим новые артикулы для добавления выручки")
        all_accounts_new_revenue_data = {}
        tasks1 = []
        tasks2 = []

        for account, articles in lk_articles.items():
            # получаем токен и корректируем регистр для чтения из файла
            token = get_wb_tokens()[account.capitalize()]

            nm_ids_result = self.gs_connect.check_new_nm_ids(account=account, nm_ids=articles)
            if len(nm_ids_result) > 0:
                print(f"account: {account} | собираем выручку по новым артикулам")
                analytics = AnalyticsNMReport(token=token)

                task1 = asyncio.create_task(
                    analytics.get_last_days_revenue(nm_ids=nm_ids_result,
                                                    begin_date=datetime.date.today() - datetime.timedelta(days=7),
                                                    end_date=datetime.date.today()))

                task2 = asyncio.create_task(
                    analytics.get_last_week_revenue(week_count=4, nm_ids=nm_ids_result)
                )

                tasks1.append(task1)
                tasks2.append(task2)

                """добавляем артикулы в БД"""
                # артикулы добавляем после получения выручки
                # в текущей реализации новые артикулы будут добавлены в БД до выгрузки данных по артикулу в таблицу
                add_nm_ids_in_db(account=account, new_nm_ids=nm_ids_result)

        results_day_revenue = await asyncio.gather(*tasks1, return_exceptions=True)
        results_week_revenue = await asyncio.gather(*tasks2, return_exceptions=True)

        for res_day in results_day_revenue:
            if isinstance(res_day, Exception):
                print(f"Ошибка при получении ежедневной выручки: {res_day}")
            else:
                all_accounts_new_revenue_data.update(res_day["result_data"])
                """добавляет данные по ежедневной выручке в БД"""
                add_orders_data(res_day["result_data"])

        for res_week in results_week_revenue:
            if isinstance(res_week, Exception):
                print(f"Ошибка при получении недельной выручки: {res_week}")
            else:
                for nm_id in res_week:
                    if nm_id in all_accounts_new_revenue_data:
                        all_accounts_new_revenue_data[nm_id].update(res_week[nm_id])

        return all_accounts_new_revenue_data

    async def add_new_data_from_table(self, lk_articles, edit_column_clean=None, only_edits_data=False,
                                      add_data_in_db=True, check_nm_ids_in_db=True):
        """Функция была изменена. Теперь она просто выдает данные на добавления в таблицу, а не добавляет таблицу внутри функции"""

        nm_ids_photo = {}
        result_nm_ids_data = {}
        filter_nm_ids_data = []
        for account, nm_ids in lk_articles.items():
            token = get_wb_tokens()[account.capitalize()]
            nm_ids_result = nm_ids

            if check_nm_ids_in_db:
                "поиск всех артикулов которых нет в БД"
                nm_ids_result = self.gs_connect.check_new_nm_ids(account=account, nm_ids=nm_ids)

                if len(nm_ids_result) > 0:
                    print("КАБИНЕТ: ", account)
                    print("новые артикулы в таблице", nm_ids_result)

            # собираем артиклы с таблицы для добавления psql
            filter_nm_ids_data.extend(nm_ids_result)

            if len(nm_ids_result) > 0:
                """Обновление/добавление данных по артикулам в гугл таблицу с WB api"""
                wb_api_content = ListOfCardsContent(token=token)
                wb_api_price_and_discount = ListOfGoodsPricesAndDiscounts(token=token)
                warehouses = WarehouseMarketplaceWB(token=token)
                barcodes_quantity = LeftoversMarketplace(token=token)
                card_from_nm_ids_filter = wb_api_content.get_list_of_cards(nm_ids_list=nm_ids_result, limit=100,
                                                                           only_edits_data=only_edits_data,
                                                                           account=account)
                goods_nm_ids = await wb_api_price_and_discount.get_log_for_nm_ids_async(filter_nm_ids=nm_ids_result,
                                                                                        account=account)
                commission_traffics = CommissionTariffs(token=token)
                wh_analytics = AnalyticsWarehouseLimits(token=token)
                # print(f"{account}","card_from_nm_ids_filter",card_from_nm_ids_filter)
                # объединяем полученные данные
                merge_json_data = merge_dicts(card_from_nm_ids_filter, goods_nm_ids)

                subject_names = set()  # итог предметов со всех карточек
                account_barcodes = []
                current_tariffs_data = commission_traffics.get_tariffs_box_from_marketplace()

                for i in merge_json_data.values():
                    # собираем и удаляем фото
                    if "wild" in i and i["wild"] != "не найдено":
                        subject_names.add(i["Предмет"])  # собираем множество с предметами
                        account_barcodes.append(i["Баркод"])
                        result_log_value = calculate_sum_for_logistic(
                            # на лету считаем "Логистика от склада WB до ПВЗ"
                            for_one_liter=float(current_tariffs_data["boxDeliveryBase"].replace(',', '.')),
                            next_liters=float(current_tariffs_data["boxDeliveryLiter"].replace(',', '.')),
                            height=int(i['Текущая\nВысота (см)']),
                            length=int(i['Текущая\nДлина (см)']),
                            width=int(i['Текущая\nШирина (см)']), )
                        # добавляем результат вычислений в итоговые данные
                        i["Логистика от склада WB до ПВЗ"] = result_log_value
                    if only_edits_data is False:
                        nm_ids_photo[int(i["Артикул"])] = i.pop("Фото")

                # собираем остатки со складов продавца
                barcodes_quantity_result = []
                for warehouse_id in warehouses.get_account_warehouse():
                    bqs_result = barcodes_quantity.get_amount_from_warehouses(
                        warehouse_id=warehouse_id['id'],
                        barcodes=account_barcodes)
                    barcodes_quantity_result.extend(bqs_result)

                # собираем остатки со складов WB
                # barcodes_qty_wb = {}
                # task_id = wh_analytics.create_report()
                # wb_warehouse_qty = await wh_analytics.check_data_by_task_id(task_id=task_id)
                # if task_id is not None and len(wb_warehouse_qty) > 0:
                #     for wh_data in wb_warehouse_qty:
                #         if wh_data["barcode"] in account_barcodes:
                #             barcodes_qty_wb[wh_data["barcode"]] = wh_data["quantityWarehousesFull"]

                subject_commissions = None
                try:
                    # получение комиссии WB
                    subject_commissions = commission_traffics.get_commission_on_subject(subject_names=subject_names)
                except (Exception, requests.exceptions.ConnectionError, requests.exceptions.HTTPError) as e:
                    print("[ERROR] Запрос получения комиссии по предметам завершился ошибкой:", e)
                for card in merge_json_data.values():
                    if subject_commissions is not None:
                        for sc in subject_commissions.items():
                            if "Предмет" in card and sc[0] == card["Предмет"]:
                                card["Комиссия WB"] = sc[1]
                    for bq_result in barcodes_quantity_result:
                        if "Баркод" in card and bq_result["Баркод"] == card["Баркод"]:
                            # card["Текущий остаток"] = bq_result["остаток"]
                            card["ФБС"] = bq_result["остаток"]

                    # if len(barcodes_qty_wb) > 0:
                    #     if "Баркод" in card and card["Баркод"] in barcodes_qty_wb.keys():
                    #         # card["Текущий остаток\nСклады WB"] = barcodes_qty_wb[card["Баркод"]]
                    #         card["ФБО"] = barcodes_qty_wb[card["Баркод"]]

                result_nm_ids_data.update(merge_json_data)

                if add_data_in_db is True:
                    """добавляем артикулы в БД"""
                    add_nm_ids_in_db(account=account, new_nm_ids=nm_ids_result)
        if len(nm_ids_photo) > 0:
            self.gs_connect.add_photo(nm_ids_photo)

        # добавляем данные артикулов в psql в таблицу article
        if len(result_nm_ids_data) > 0:
            # db = self.database()
            try:
                async with Database1() as connection:
                    # async with self.database().acquire() as connection:
                    psql_article = ArticleTable(db=connection)
                    # psql_article = ArticleTable(db=connection)

                    # ограничение функции: добавляет данные в psql, но только если их не было в бд json
                    filter_nm_ids = await psql_article.check_nm_ids(account="None", nm_ids=filter_nm_ids_data)
                    if filter_nm_ids:
                        print("filter_nm_ids", filter_nm_ids)
                        await psql_article.update_articles(data=result_nm_ids_data, filter_nm_ids=filter_nm_ids)
                    print("данные по артикулам добавлены в таблицу article psql")
            except Exception as e:
                print(e)

        return result_nm_ids_data

    async def change_cards_and_tables_data(self, edit_data_from_table):
        sheet_statuses = ServiceGoogleSheet.check_status()
        net_profit_status = sheet_statuses['Отрицательная \nЧП']
        price_discount_edit_status = sheet_statuses['Цены/Скидки']
        dimensions_edit_status = sheet_statuses['Габариты']
        quantity_edit_status = sheet_statuses['Остаток']
        updates_nm_ids_data = {}
        edit_column_clean = {"price_discount": False, "dimensions": False, "qty": False}

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
                    # статус на изменение габаритов должен быть активным (вкл/выкл бот)
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
                    wb_api_price_and_discount.add_new_price_and_discount(price_discount_data)
                    edit_column_clean["price_discount"] = True

                """Запрос на изменение габаритов товара по артикулу и vendorCode (wild)"""
                if len(size_edit_data) > 0:
                    wb_api_content.size_edit(size_edit_data)
                    edit_column_clean["dimensions"] = True
                """Перезаписываем данные в таблице после их изменений на WB"""
                nm_ids_result = [int(nm_ids_str) for nm_ids_str in valid_data_result.keys()]
                updates_nm_ids_data.update({account: nm_ids_result})

            if account in edit_data_from_table["qty_edit_data"]:
                if len(edit_data_from_table["qty_edit_data"][account]["stocks"]) > 0 and quantity_edit_status:
                    "изменение остатков на всех складах продавца"
                    print("изменение остатков на всех складах продавца")
                    for warehouse in warehouses.get_account_warehouse():
                        warehouses_qty_edit.edit_amount_from_warehouses(warehouse_id=warehouse["id"],
                                                                        edit_barcodes_list=
                                                                        edit_data_from_table["qty_edit_data"][
                                                                            account]["stocks"])
                    edit_column_clean["qty"] = True
                    # добавляем артикул для обновления данных
                    if account not in updates_nm_ids_data:
                        updates_nm_ids_data[account] = []
                    updates_nm_ids_data[account].extend(edit_data_from_table["qty_edit_data"][account]["nm_ids"])

        # если хоть по одному артикулу данные будут валидны...
        if len(updates_nm_ids_data):
            # print("updates_nm_ids_data",updates_nm_ids_data)
            time.sleep(5)
            result = await self.add_new_data_from_table(lk_articles=updates_nm_ids_data,
                                                        only_edits_data=True, add_data_in_db=False,
                                                        check_nm_ids_in_db=False)
            return result

        return updates_nm_ids_data

    async def add_new_day_revenue_to_table(self):
        start = datetime.datetime.now()
        statuses = ServiceGoogleSheet.check_status()
        if statuses['ВКЛ - 1 /ВЫКЛ - 0']:
            begin_date = datetime.date.today()
            end_date = datetime.date.today()
            last_day = end_date.strftime("%d-%m-%Y")
            """Добавление нового дня в заголовки таблицы и выручки по этим дням и сдвиг последних шести дней влево"""
            # проверяем нет ли вчерашнего дня в заголовках таблицы
            print(f"Актуализируем выручку по текущему дню: {last_day}")
            if self.gs_service_revenue_connect.check_last_day_header_from_table(header=last_day):
                # По умолчанию begin_date - дата сегодняшнего дня. Если будет смещение, то begin_date будет форматирован
                # на вчерашний, чтобы актуализировать выручку за вчерашний день так же
                begin_date = datetime.date.today() - datetime.timedelta(days=1)
                print(last_day, "заголовка нет в таблице. Будет добавлен включая выручка по дню")
                # сначала сдвигаем колонки с выручкой
                self.gs_service_revenue_connect.shift_revenue_columns_to_the_left(last_day=last_day)

            lk_articles = self.gs_connect.create_lk_articles_dict()
            # собираем выручку по всем артикулам аккаунтов
            all_accounts_new_revenue_data = {}
            tasks = []
            nm_ids_table_data = {}
            current_time = datetime.datetime.now().time()  # время для ЧП
            for account, nm_ids_data in lk_articles.items():
                nm_ids = list(nm_ids_data.keys())
                nm_ids_table_data.update(nm_ids_data)
                # nm_ids_pc_table_data.update(lk_articles_pc[account])
                token = get_wb_tokens()[account.capitalize()]
                anal_revenue = AnalyticsNMReport(token=token)
                task = asyncio.create_task(
                    anal_revenue.get_last_days_revenue(begin_date=begin_date,
                                                       end_date=end_date,
                                                       nm_ids=nm_ids, account=account, orders_db_ad=True))
                tasks.append(task)

            # Ждем завершения всех задач
            results = await asyncio.gather(*tasks, return_exceptions=True)
            # Коллекция для обновления кол. заказов и выручки в бд
            psql_data_update = {}
            for res in results:
                if isinstance(res, Exception):
                    print(f"Ошибка при получении недельной выручки: {res}")

                else:
                    all_accounts_new_revenue_data.update(res["result_data"])

                    account_data_for_days = res["all_data"]
                    for day, account_data in account_data_for_days.items():
                        if day not in psql_data_update:
                            psql_data_update[day] = {}
                        psql_data_update[day].update(account_data)

                add_orders_data(res["result_data"])
            print("Выручка добавлена в БД")
            # добавляем их таблицу

            # проверяем заголовок прошлой недели
            week_date = list(get_last_weeks_dates().keys())
            if self.gs_service_revenue_connect.check_last_day_header_from_table(header=week_date[0]):
                print(f"Заголовка {week_date[0]} нет в таблице, будет добавлен со смещением столбцов")
                self.gs_service_revenue_connect.shift_week_revenue_columns_to_the_left(last_week=week_date[0])
                for account, nm_ids_data in lk_articles.items():
                    nm_ids = list(nm_ids_data.keys())
                    token = get_wb_tokens()[account.capitalize()]
                    anal_revenue = AnalyticsNMReport(token=token)
                    revenue_week_data_by_article = await anal_revenue.get_last_week_revenue(week_count=1, nm_ids=nm_ids)

                    for nm_id in revenue_week_data_by_article:
                        if nm_id in all_accounts_new_revenue_data:
                            all_accounts_new_revenue_data[nm_id].update(revenue_week_data_by_article[nm_id])

            # добавляем выручку в таблицу
            print("Собрали выручку по всем кабинетам timer:", datetime.datetime.now() - start)
            # print("all_accounts_new_revenue_data", all_accounts_new_revenue_data)
            self.gs_service_revenue_connect.update_revenue_rows(data_json=all_accounts_new_revenue_data)
            print(f"Выручка в таблице актуализирована по всем артикулам.")
            start = datetime.datetime.now()
            # актуализация информация по заказам в листах таблицы
            await self.add_orders_data_in_table()
            try:
                print("Актуализируем данные в бд таблицы accurate_net_profit_data")
                await self.add_data_in_db_psql(psql_data_update=psql_data_update, net_profit_time=current_time,
                                               nm_ids_table_data=nm_ids_table_data)
                # print("Актуализируем данные в бд таблицы accurate_npd_purchase_calculation")
                # await self.add_data_in_db_psql_purchase_calculation(psql_data_update=psql_data_update,
                #                                                     net_profit_time=current_time,
                #                                                     nm_ids_table_data=nm_ids_pc_table_data)
            except asyncio.TimeoutError as e:
                # print("поймали исключение при (add_data_in_db_psql, add_data_in_db_psql_purchase_calculation):", e)
                print(e)
                print("повторная попытка: Актуализируем данные в бд таблицы accurate_net_profit_data")
                await self.add_data_in_db_psql(psql_data_update=psql_data_update, net_profit_time=current_time,
                                               nm_ids_table_data=nm_ids_table_data)
                # print("повторная попытка: Актуализируем данные в бд таблицы accurate_npd_purchase_calculation")
                # await self.add_data_in_db_psql_purchase_calculation(psql_data_update=psql_data_update,
                #                                                     net_profit_time=current_time,
                #                                                     nm_ids_table_data=nm_ids_pc_table_data)

            print("Функция актуализации timer:", datetime.datetime.now() - start)

    @staticmethod
    def check_status():
        for i in range(10):
            try:
                sheet_status = GoogleSheet(creds_json="creds.json",
                                           spreadsheet="UNIT 2.0 (tested)", sheet="ВКЛ/ВЫКЛ Бот")
                return sheet_status.check_status_service_sheet()
            except gspread.exceptions.APIError as e:
                print(f"попытка {i}", e, "следующая попытка через 75 секунд")
                time.sleep(75)

        return False

    async def add_actually_data_to_table(self):
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
                wh_analytics = AnalyticsWarehouseLimits(token=token)
                card_from_nm_ids_filter = wb_api_content.get_list_of_cards(nm_ids_list=articles, limit=100,
                                                                           only_edits_data=True, add_data_in_db=False,
                                                                           account=account)
                goods_nm_ids = await wb_api_price_and_discount.get_log_for_nm_ids_async(filter_nm_ids=articles,
                                                                                        account=account)

                # объединяем полученные данные
                merge_json_data = merge_dicts(goods_nm_ids, card_from_nm_ids_filter)
                subject_names = set()  # итог всех полученных с карточек предметов
                account_barcodes = []
                current_tariffs_data = commission_traffics.get_tariffs_box_from_marketplace()

                # если мы не получили данные по артикулам, то аккаунт будет пропущен
                if len(card_from_nm_ids_filter) == 0:
                    print(f"По токену {account} не получили Артикулы с данными с API WB")
                    print(f"Артикулы:{articles}")
                    print(f"Результат с API WB {merge_json_data}")
                    continue  # пропускаем этот аккаунт
                for i in merge_json_data.values():
                    if "wild" in i and i["wild"] != "не найдено":
                        account_barcodes.append(i["Баркод"])
                        subject_names.add(i["Предмет"])  # собираем множество с предметами
                        result_log_value = calculate_sum_for_logistic(
                            # на лету считаем "Логистика от склада WB до ПВЗ"
                            for_one_liter=float(current_tariffs_data["boxDeliveryBase"].replace(',', '.')),
                            next_liters=float(current_tariffs_data["boxDeliveryLiter"].replace(',', '.')),
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

                # собираем остатки со складов WB
                # barcodes_qty_wb = {}
                # task_id = await wh_analytics.create_report()
                # wb_warehouse_qty = await wh_analytics.check_data_by_task_id(task_id=task_id)
                # if task_id is not None and len(wb_warehouse_qty) > 0:
                #     if wb_warehouse_qty:
                #         for wh_data in wb_warehouse_qty:
                #             if wh_data["barcode"] in account_barcodes:
                #                 barcodes_qty_wb[wh_data["barcode"]] = wh_data["quantityWarehousesFull"]

                subject_commissions = None
                try:
                    # получение комиссии WB
                    subject_commissions = commission_traffics.get_commission_on_subject(subject_names=subject_names)
                except (Exception, requests.exceptions.ConnectionError, requests.exceptions.HTTPError) as e:
                    print("[ERROR] Запрос получения комиссии по предметам завершился ошибкой:", e)
                # добавляем данные в merge_json_data
                for card in merge_json_data.values():
                    if subject_commissions is not None:
                        for sc in subject_commissions.items():
                            if "Предмет" in card and sc[0] == card["Предмет"]:
                                card["Комиссия WB"] = sc[1]
                    for bq_result in barcodes_quantity_result:
                        if "Баркод" in card and bq_result["Баркод"] == card["Баркод"]:
                            # card["Текущий остаток"] = bq_result["остаток"]
                            card["ФБС"] = bq_result["остаток"]

                    # if len(barcodes_qty_wb) > 0:
                    #     if "Баркод" in card and card["Баркод"] in barcodes_qty_wb.keys():
                    #         # card["Текущий остаток\nСклады WB"] = barcodes_qty_wb[card["Баркод"]]
                    #         card["ФБО"] = barcodes_qty_wb[card["Баркод"]]

                result_updates_rows.update(merge_json_data)
                """обновляем данные по артикулам"""
            gs_connect.update_rows(data_json=result_updates_rows)

    async def get_actually_data_by_qty(self):

        if ServiceGoogleSheet.check_status()['ВКЛ - 1 /ВЫКЛ - 0']:
            print("[INFO]", datetime.datetime.now(), "актуализируем данные по остаткам в таблице")

            gs_connect_main = GoogleSheet(creds_json=self.creds_json, spreadsheet=self.spreadsheet, sheet=self.sheet)
            lk_articles = await gs_connect_main.create_lk_barcodes_articles()
            gs_connect_warehouses_info = GoogleSheet(creds_json=self.creds_json, spreadsheet=self.spreadsheet,
                                                     sheet="Склады ИНФ")

            # словарь с регионом и группой складов
            warehouses_info = await gs_connect_warehouses_info.get_warehouses_info()
            print("warehouse_info", warehouses_info)
            tasks = []
            for account, data in lk_articles.items():
                task = asyncio.create_task(
                    self.get_qty_data_by_account(account=account, data=data, warehouses_info=warehouses_info))

                tasks.append(task)
            together_result = await asyncio.gather(*tasks)
            articles_qty_wb = {}  # результат данных по отслеживаемым регионам\складам
            untracked_warehouses = {}  # результат данных по неотслеживаемым складам и сумма остаток
            for tr in together_result:
                articles_qty_wb.update(tr['articles_qty_wb'])
                for key, value in tr['untracked_warehouses'].items():
                    # untracked_warehouses[key] = untracked_warehouses.get(key, 0) + value
                    if key not in untracked_warehouses:
                        untracked_warehouses[key] = {"ОСТАТКИ": value}
                    untracked_warehouses[key]["ОСТАТКИ"] += value

            # update по остаткам в sheet UNIT
            await gs_connect_main.update_qty_by_reg(update_data=articles_qty_wb)
            # update по неотслеживаемым складам для sheet 'Склады ИНФ'
            await gs_connect_warehouses_info.update_untracked_warehouses_quantity(update_data=untracked_warehouses)
            print("Данные по остаткам обновлены в таблице.")

    async def get_qty_data_by_account(self, account, data, warehouses_info):
        token = get_wb_tokens()[account.capitalize()]
        wh_analytics = AnalyticsWarehouseLimits(token=token)

        barcodes_set = set(data.keys())  # баркоды по аккаунту с таблицы

        articles_qty_wb = {}
        untracked_warehouses = {}
        task_id = await wh_analytics.create_report()
        wb_warehouse_qty = await wh_analytics.check_data_by_task_id(task_id=task_id)
        if task_id is not None and len(wb_warehouse_qty) > 0:
            if wb_warehouse_qty:  # собираем остатки со складов WB
                for qty_data in wb_warehouse_qty:
                    if qty_data['barcode'] in barcodes_set:
                        barcode = qty_data['barcode']
                        article = data[barcode]
                        articles_qty_wb[article] = {
                            "ФБО": qty_data['quantityWarehousesFull'],
                            # "ФБО": 0,
                        }
                        warehouses = qty_data['warehouses']

                        if len(warehouses) > 1:
                            for wh_data in warehouses:
                                warehouse_name = wh_data["warehouseName"]
                                if warehouse_name == "Всего находится на складах":
                                    articles_qty_wb[article]['ФБО'] += wh_data['quantity']

                                if warehouse_name in warehouses_info:
                                    region_name_by_warehouse = warehouses_info[warehouse_name]
                                    # по задумке должен суммировать остатки всех закрепленных регионов к складам
                                    if warehouses_info[warehouse_name] not in articles_qty_wb[article]:
                                    # if articles_qty_wb[article][region_name_by_warehouse] == "":
                                        articles_qty_wb[article][region_name_by_warehouse] = 0
                                    articles_qty_wb[article][region_name_by_warehouse] += wh_data["quantity"]

                                else:
                                    # сбор данных по остаткам складов которые не отслеживаются по регионам
                                    # if warehouse_name in ("Всего находится на складах", "В пути возвраты на склад WB",
                                    #                       "В пути до получателей"):
                                    #     continue
                                    if warehouse_name not in untracked_warehouses:
                                        untracked_warehouses[warehouse_name] = 0
                                    untracked_warehouses[warehouse_name] += wh_data["quantity"]

                        # clean_data = {"Центральный": "",
                        #               "Южный": "",
                        #               "Северо-Кавказский": "",
                        #               "Приволжский": ""}
                        #
                        # for cd in clean_data:
                        #     if cd not in articles_qty_wb[article]:
                        #         articles_qty_wb[article].update({cd: ""})

        return {"articles_qty_wb": articles_qty_wb, "untracked_warehouses": untracked_warehouses}

    async def check_quantity_flag(self):
        print(datetime.datetime.now(), "Проверка остатков по лимитам из столбца 'Минимальный остаток'")
        status = ServiceGoogleSheet.check_status()
        status_limit_edit = status["Добавить если"]
        status_open_close_fbs = status["ОТКРЫТИЕ/ЗАКРЫТИЕ ФБС"]
        status_min_qty = status['минимальный\nостаток']
        add_qty = status['повышение\n остатков']
        status_average_orders_percent = status['среднее арифм. \nот заказов (%)']
        status_off_on_service = bool(int(status["ВКЛ - 1 /ВЫКЛ - 0"]))
        bot_status = {"status_min_qty": bool(int(status_min_qty)),
                      "status_open_close_fbs": bool(int(status_open_close_fbs))}
        print(bot_status)
        # если сервис включен
        if status_off_on_service:
            # если включены флаги на изменение остатков
            if bot_status["status_min_qty"] or bot_status["status_open_close_fbs"]:
                low_limit_qty_data = self.gs_connect.get_data_quantity_limit(status_min_qty=status_min_qty,
                                                                             add_qty=add_qty,
                                                                             status_average_orders_percent=status_average_orders_percent,
                                                                             bot_status=bot_status)

                sopost_data = GoogleSheetSopostTable().wild_quantity()
                nm_ids_for_update_data = {}
                if len(low_limit_qty_data["result_data"]) > 0 or len(low_limit_qty_data["edit_fbc_qty_data"]) > 0:
                    print("Есть остатки ниже установленного флага")
                    for account, edit_data in low_limit_qty_data["result_data"].items():
                        update_qty_data = []
                        token = get_wb_tokens()[account.capitalize()]
                        warehouses = WarehouseMarketplaceWB(token=token).get_account_warehouse()
                        qty_edit = LeftoversMarketplace(token=token)

                        if len(edit_data["qty"]) > 0:
                            for qty_data in edit_data["qty"]:
                                if str(sopost_data[qty_data["wild"]]).isdigit() and int(
                                        sopost_data[qty_data["wild"]]) != 0:
                                    update_qty_data.append(
                                        {
                                            "sku": qty_data["sku"],
                                            "amount": int(sopost_data[qty_data["wild"]])
                                        }
                                    )

                        # добавляет баркоды с новыми остатками для запроса на изменение остатков
                        if account in low_limit_qty_data["edit_fbc_qty_data"] and len(
                                low_limit_qty_data["edit_fbc_qty_data"][account]) > 0:
                            update_qty_data.extend(low_limit_qty_data["edit_fbc_qty_data"][account])

                        if len(update_qty_data):
                            for warehouse_id in warehouses:
                                qty_edit.edit_amount_from_warehouses(warehouse_id=warehouse_id["id"],
                                                                     edit_barcodes_list=update_qty_data)

                            if account not in nm_ids_for_update_data:
                                nm_ids_for_update_data[account] = []
                            print("nm_ids_for_update_data")
                            pprint(nm_ids_for_update_data)
                            nm_ids_for_update_data[account].extend(low_limit_qty_data["result_data"][account]['nm_ids'])
                if len(nm_ids_for_update_data) > 0:
                    print(nm_ids_for_update_data)
                    nm_ids_data_json = await self.add_new_data_from_table(lk_articles=nm_ids_for_update_data,
                                                                          only_edits_data=True, add_data_in_db=False,
                                                                          check_nm_ids_in_db=False)
                    if len(low_limit_qty_data["edit_min_qty"]) > 0:
                        print(low_limit_qty_data["edit_min_qty"])
                        nm_ids_data_json = merge_dicts(nm_ids_data_json, low_limit_qty_data["edit_min_qty"])

                    self.gs_connect.update_rows(data_json=nm_ids_data_json,
                                                edit_column_clean={"qty": False, "price_discount": False,
                                                                   "dimensions": False})

    async def add_orders_data_in_table(self):

        from settings import settings
        from utils import get_order_data_from_database
        gs_connect = GoogleSheet(sheet="Количество заказов", spreadsheet=settings.SPREADSHEET,
                                 creds_json=settings.CREEDS_FILE_NAME)
        orders_count_data = get_order_data_from_database()
        date_object = datetime.datetime.today()
        today = date_object.strftime("%d.%m")

        # если нет текущего дня
        if gs_connect.check_header(header=today):
            print(f"Нет текущего дня {today} в листах. Сервис сместит данные по дням")
            # сместит заголовки дней в листе "Количество заказов"
            gs_connect.shift_headers_count_list(today)
            # сместит заголовки дней в листе "MAIN"
            self.gs_connect.shift_orders_header(today=today)
        # если есть данные в БД - будут добавлены в лист
        if len(orders_count_data):
            print("актуализируем данные по заказам в таблице")
            gs_connect.add_data_to_count_list(data_json=orders_count_data)

        print("Обновлено количество заказов по дням в MAIN")
        """ Функция добавления количества заказов по дням в таблицу """

    async def add_data_in_db_psql(self, psql_data_update, net_profit_time, nm_ids_table_data):
        print("Открываем pool в подключении к БД")
        async with Database1() as connection:
            accurate_net_profit_table = AccurateNetProfitTable(db=connection)

            print("Смотрим данные в psql_data_update")
            for date, psql_data in psql_data_update.items():
                nm_ids_list = list(psql_data.keys())
                print("Запрос по новым артикулам")
                psql_new_nm_ids = await accurate_net_profit_table.check_nm_ids(nm_ids=nm_ids_list, account=None,
                                                                               date=date)
                print("новые артикулы которых нет в таблице net_profit:", psql_new_nm_ids)
                "Добавляем данные в бд psql"
                if psql_new_nm_ids:  # добавляем новые артикулы в бд psql
                    # если новых артикулов нет в таблице net_profit, то будут добавлены
                    await accurate_net_profit_table.add_new_article_net_profit_data(time=net_profit_time,
                                                                                    data=psql_data,
                                                                                    nm_ids_net_profit=nm_ids_table_data,
                                                                                    new_nm_ids=psql_new_nm_ids)
                    print("артикулы в бд таблицы accurate_net_profit_table актуализированы")

                # актуализируем информацию по полученному с таблицы чп по артикулам
                await accurate_net_profit_table.update_net_profit_data(time=net_profit_time,
                                                                       response_data=psql_data,
                                                                       nm_ids_table_data=nm_ids_table_data,
                                                                       date=date)
                print("актуализированы данные в бд таблицы accurate_net_profit_data")

                print("[INFO] Получаем данные с бд с таблицы accurate_net_profit")
                result_som_net_profit_data = await accurate_net_profit_table.get_net_profit_by_date(date=date)

                formatted_result = {}
                for record in result_som_net_profit_data:
                    article_id = record['article_id']
                    sum_value = record['sum_snp']
                    date_obj = datetime.datetime.strptime(date, "%Y-%m-%d")

                    formatted_date_str = date_obj.strftime("%d.%m")
                    formatted_result[article_id] = {formatted_date_str: int(sum_value)}

                print("[INFO] Обновляем данные по сумме ЧП в листе MAIN")
                self.gs_service_revenue_connect.update_revenue_rows(data_json=formatted_result)
                print("данные по ЧП актуализированы")

    async def add_data_in_db_psql_purchase_calculation(self, psql_data_update, net_profit_time, nm_ids_table_data):
        try:
            print("Открываем pool в подключении к БД")
            async with Database1() as connection:
                accurate_net_profit_pc_table = AccurateNetProfitPCTable(db=connection)

                print("Смотрим данные в psql_data_update")
                for date, psql_data in psql_data_update.items():
                    nm_ids_list = list(psql_data.keys())
                    print("Запрос по новым артикулам")
                    psql_new_nm_ids = await accurate_net_profit_pc_table.check_nm_ids(nm_ids=nm_ids_list, account=None,
                                                                                      date=date)
                    print("новые артикулы которых нет в таблице accurate_npd_purchase_calculation:", psql_new_nm_ids)
                    "Добавляем данные в бд psql"
                    if psql_new_nm_ids:  # добавляем новые артикулы в бд psql
                        # если новых артикулов нет в таблице net_profit, то будут добавлены
                        await accurate_net_profit_pc_table.add_new_article_net_profit_data(time=net_profit_time,
                                                                                           data=psql_data,
                                                                                           nm_ids_net_profit=nm_ids_table_data,
                                                                                           new_nm_ids=psql_new_nm_ids)
                        print("артикулы в бд таблицы accurate_npd_purchase_calculation актуализированы")

                    # актуализируем информацию по полученному с таблицы чп по артикулам
                    await accurate_net_profit_pc_table.update_net_profit_data(time=net_profit_time,
                                                                              response_data=psql_data,
                                                                              nm_ids_table_data=nm_ids_table_data,
                                                                              date=date)
                    print("актуализированы данные в бд таблицы accurate_npd_purchase_calculation")
        except KeyError as e:
            print("[ERROR] (func) add_data_in_db_psql_purchase_calculation, KeyError", e)

    async def update_purchase_calculation_data(self):
        gs_pc_service = PCGoogleSheet(creds_json=Setting().CREEDS_FILE_NAME, sheet=Setting().PC_SHEET,
                                      spreadsheet=Setting().PC_SPREADSHEET)
        # result = gs_pc_service.create_lk_articles_dict()
        # pprint(result)
        date_object = datetime.datetime.today() - datetime.timedelta(days=1)
        yesterday = date_object.strftime("%d.%m")
        print(yesterday)
        if gs_pc_service.check_last_day_header_from_table(yesterday):
            gs_pc_service.shift_orders_header(day=yesterday)
            print(f"Добавлен новый заголовок: {yesterday}")
        print("Актуализируем данные по артикулам из листа MAIN в лист ПРОДАЖИ")
        update_columns_in_purchase_calculation()

        print(" актуализируем данные по продажам из бд в таблицу ПРОДАЖИ")
        async with Database1() as connection:
            purchase_calculation_table = AccurateNetProfitPCTable(db=connection)
            database_result = await purchase_calculation_table.get_net_profit_by_latest_dates()
            edit_result = {}
            for res in database_result:
                article_id = res['article_id']
                snp = int(res['snp'])
                date = res['date']
                if article_id not in edit_result:
                    edit_result[article_id] = {}
                edit_result[article_id][date] = snp

            print(edit_result)
        gs_pc_service.update_revenue_rows(edit_result)

    async def add_data_by_net_profit(self):
        """Функция для актуализации ЧП за конкретный день.(Если данные за день вдруг не подгрузились в таблице)"""
        async with Database1() as connection:
            date = '2024-12-11'
            accurate_net_profit_table = AccurateNetProfitTable(db=connection)
            result_som_net_profit_data = await accurate_net_profit_table.get_net_profit_by_date(date=date)

            formatted_result = {}
            for record in result_som_net_profit_data:
                article_id = record['article_id']
                sum_value = record['sum_snp']
                date_obj = datetime.datetime.strptime(date, "%Y-%m-%d")

                formatted_date_str = date_obj.strftime("%d.%m")
                formatted_result[article_id] = {formatted_date_str: int(sum_value)}

            print("[INFO] Обновляем данные по сумме ЧП в листе MAIN")
            pprint(formatted_result)
            self.gs_service_revenue_connect.update_revenue_rows(data_json=formatted_result)
            print("данные по ЧП актуализированы")
