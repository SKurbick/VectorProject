import asyncio
import datetime
import time
from pprint import pformat, pprint
from typing import Tuple, List, Dict, Any, Set

import gspread.exceptions
import requests

import settings
from APIGoogleSheet.googlesheet import GoogleSheetServiceRevenue, GoogleSheet, GoogleSheetSopostTable, PCGoogleSheet, \
    update_columns_in_purchase_calculation
from APIWildberries.analytics import AnalyticsNMReport, AnalyticsWarehouseLimits
from APIWildberries.content import ListOfCardsContent
from APIWildberries.marketplace import WarehouseMarketplaceWB, LeftoversMarketplace
from APIWildberries.prices_and_discounts import ListOfGoodsPricesAndDiscounts
from APIWildberries.statistics import Statistic
from APIWildberries.tariffs import CommissionTariffs
from database.postgresql.models.inventory_turnover_by_reg import QuantityAndSupply
from database.postgresql.repositories.accurate_net_profit_data import AccurateNetProfitTable
from database.postgresql.repositories.accurate_npd_purchase_calculation import AccurateNetProfitPCTable
from database.postgresql.repositories.article import ArticleTable
from database.postgresql.repositories.inventory_turnover_by_reg import InventoryTurnoverByRegTable
from database.postgresql.repositories.orders_by_federal_district import OrdersByFederalDistrict
from database.postgresql.repositories.orders_revenues import OrdersRevenuesTable
from settings import get_wb_tokens
from settings import Setting
from utils import add_orders_data, calculate_sum_for_logistic, merge_dicts, validate_data, add_nm_ids_in_db, \
    get_last_weeks_dates, create_valid_data_from_db
from database.postgresql.repositories.card_data import CardData

from database.postgresql.database import Database1
from settings import settings
from logger import app_logger as logger


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

    async def check_headers(self):
        start = datetime.datetime.now()
        statuses = ServiceGoogleSheet.check_status()
        if statuses['ВКЛ - 1 /ВЫКЛ - 0']:
            end_date = datetime.date.today()
            last_day = end_date.strftime("%d-%m-%Y")
            """Добавление нового дня в заголовки таблицы и выручки по этим дням и сдвиг последних шести дней влево"""
            # проверяем нет ли вчерашнего дня в заголовках таблицы
            logger.info(f"Актуализируем выручку по текущему дню: {last_day}")
            if self.gs_service_revenue_connect.check_last_day_header_from_table(header=last_day):
                logger.info(f"{last_day}, заголовка нет в таблице. Будет добавлен включая выручка по дню")
                #  сдвигаем колонки с выручкой и добавляем заголовок нового дня
                self.gs_service_revenue_connect.shift_revenue_columns_to_the_left(last_day=last_day)

            # проверяем заголовок прошлой недели
            week_date = list(get_last_weeks_dates().keys())
            if self.gs_service_revenue_connect.check_last_day_header_from_table(
                    header=week_date[0]):  # перенести функционал по актуализации недельной выручки
                all_accounts_new_revenue_data = {}
                logger.info(f"Заголовка {week_date[0]} нет в таблице, будет добавлен со смещением столбцов")
                self.gs_service_revenue_connect.shift_week_revenue_columns_to_the_left(last_week=week_date[0])
                tasks = []
                lk_articles = self.gs_connect.create_lk_articles_dict()
                for account, nm_ids_data in lk_articles.items():
                    nm_ids = list(nm_ids_data.keys())
                    token = get_wb_tokens()[account.capitalize()]
                    anal_revenue = AnalyticsNMReport(token=token)
                    task = asyncio.create_task(anal_revenue.get_last_week_revenue(week_count=1, nm_ids=nm_ids))
                    tasks.append(task)

                together_result = await asyncio.gather(*tasks)
                for res in together_result:
                    all_accounts_new_revenue_data.update(res)
                # добавляем выручку в таблицу
                logger.info(f"Собрали недельную выручку по всем кабинетам timer: {datetime.datetime.now() - start}")
                self.gs_service_revenue_connect.update_revenue_rows(data_json=all_accounts_new_revenue_data)
        gs_connect = GoogleSheet(sheet="Количество заказов", spreadsheet=settings.SPREADSHEET,
                                 creds_json=settings.CREEDS_FILE_NAME)
        date_object = datetime.datetime.today()
        today = date_object.strftime("%d.%m")
        # если нет текущего дня
        if gs_connect.check_header(header=today):
            logger.info(f"Нет текущего дня {today} в листах. Сервис сместит данные по дням")
            # сместит заголовки дней в листе "Количество заказов"
            gs_connect.shift_headers_count_list(today)
        if self.gs_connect.check_header(header=today):
            # сместит заголовки дней в листе "MAIN"
            self.gs_connect.shift_orders_header(today=today)

    async def get_actually_revenues_orders_and_net_profit_data(self):

        gs_connect_orders_sheet = GoogleSheetServiceRevenue(creds_json=self.creds_json, spreadsheet=self.spreadsheet,
                                                            sheet="Количество заказов")
        # todo так же можно добавить актуализацию по другим данным с бд таблицы
        current_date = datetime.datetime.today().date() - datetime.timedelta(days=0)
        revenue_date_header = current_date.strftime("%d-%m-%Y")
        orders_and_np_date_header = current_date.strftime("%d.%m")
        logger.info(current_date)
        data_to_update_main = {}
        data_to_update_orders = {}
        async with Database1() as connection:
            accurate_net_profit_table = AccurateNetProfitTable(db=connection)
            orders_revenues = OrdersRevenuesTable(db=connection)
            orders_revenues_db_data = await orders_revenues.get_data_by_date(date=current_date)
            for res in orders_revenues_db_data:
                article_id = res['article_id']
                revenues = res['orders_sum_rub']
                orders = res['orders_count']
                data_to_update_main[article_id] = {revenue_date_header: revenues}
                data_to_update_orders[article_id] = {orders_and_np_date_header: orders}

            result_som_net_profit_data = await accurate_net_profit_table.get_net_profit_by_date(date=str(current_date))
            for record in result_som_net_profit_data:
                article_id = record['article_id']
                sum_value = int(record['sum_snp'])
                if article_id not in data_to_update_main:
                    data_to_update_main[article_id] = {}
                data_to_update_main[article_id].update({orders_and_np_date_header: sum_value})

        try:
            self.gs_service_revenue_connect.update_revenue_rows(data_json=data_to_update_main)
            logger.info("Данные по чп и выручке в Unit актуализированы")
        except Exception as e:
            logger.error(
                f"{e} Ошибка при актуализации информации по выручке и чп в main. Повторная попытка 36 sec")
            await asyncio.sleep(36)
            self.gs_service_revenue_connect.update_revenue_rows(data_json=data_to_update_main)
            logger.info("Данные по чп и выручке в Unit актуализированы")
        #
        try:
            await gs_connect_orders_sheet.add_data_to_count_list(data_json=data_to_update_orders)
            logger.info("Данные по количеству заказов актуализированы")
        except Exception as e:
            logger.error(
                f"{e} Ошибка при актуализации информации в Количество заказов. Повторная попытка 36 sec")
            await asyncio.sleep(36)
            await gs_connect_orders_sheet.add_data_to_count_list(data_json=data_to_update_orders)
            logger.info("Данные по количеству заказов актуализированы")

    async def add_revenue_for_new_nm_ids(self, lk_articles: dict):
        """ Добавление выручки по новым артикулам за 7 последних дней (сегодняшний не учитывается)"""
        logger.info("Смотрим новые артикулы для добавления выручки")
        all_accounts_new_revenue_data = {}
        tasks1 = []
        tasks2 = []

        for account, articles in lk_articles.items():
            # получаем токен и корректируем регистр для чтения из файла
            token = get_wb_tokens()[account.capitalize()]

            nm_ids_result = self.gs_connect.check_new_nm_ids(account=account, nm_ids=articles)
            if len(nm_ids_result) > 0:
                logger.info(f"account: {account} | собираем выручку по новым артикулам")
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
                logger.info(f"Ошибка при получении ежедневной выручки: {res_day}")
            else:
                all_accounts_new_revenue_data.update(res_day["result_data"])
                """добавляет данные по ежедневной выручке в БД"""
                add_orders_data(res_day["result_data"])

        for res_week in results_week_revenue:
            if isinstance(res_week, Exception):
                logger.info(f"Ошибка при получении недельной выручки: {res_week}")
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
                    logger.info(f"КАБИНЕТ: {account}")
                    logger.info(f"новые артикулы в таблице {nm_ids_result}")

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
                        # result_log_value = calculate_sum_for_logistic(
                        #     # на лету считаем "Логистика от склада WB до ПВЗ"
                        #     for_one_liter=float(current_tariffs_data["boxDeliveryBase"].replace(',', '.')),
                        #     next_liters=float(current_tariffs_data["boxDeliveryLiter"].replace(',', '.')),
                        #     height=int(i['Текущая\nВысота (см)']),
                        #     length=int(i['Текущая\nДлина (см)']),
                        #     width=int(i['Текущая\nШирина (см)']), )
                        # # добавляем результат вычислений в итоговые данные
                        # i["Логистика от склада WB до ПВЗ"] = result_log_value

                    if only_edits_data is False:
                        try:
                            nm_ids_photo[int(i["Артикул"])] = i.pop("Фото")
                        except KeyError as e:
                            logger.info(f"не получено фото из массива")

                # собираем остатки со складов продавца
                barcodes_quantity_result = []
                for warehouse_id in warehouses.get_account_warehouse():
                    bqs_result = barcodes_quantity.get_amount_from_warehouses(
                        warehouse_id=warehouse_id['id'],
                        barcodes=account_barcodes)
                    barcodes_quantity_result.extend(bqs_result)

                subject_commissions = None
                try:
                    # получение комиссии WB
                    subject_commissions = commission_traffics.get_commission_on_subject(subject_names=subject_names)
                except (Exception, requests.exceptions.ConnectionError, requests.exceptions.HTTPError) as e:
                    logger.info(f"[ERROR]  Запрос получения комиссии по предметам завершился ошибкой: {e}")
                for card in merge_json_data.values():
                    if subject_commissions is not None:
                        for sc in subject_commissions.items():
                            if "Предмет" in card and sc[0] == card["Предмет"]:
                                card["Комиссия WB"] = sc[1]
                    for bq_result in barcodes_quantity_result:
                        if "Баркод" in card and bq_result["Баркод"] == card["Баркод"]:
                            # card["Текущий остаток"] = bq_result["остаток"]
                            card["ФБС"] = bq_result["остаток"]

                result_nm_ids_data.update(merge_json_data)

                if add_data_in_db is True:
                    """добавляем артикулы в БД"""
                    add_nm_ids_in_db(account=account, new_nm_ids=nm_ids_result)
        if nm_ids_photo:
            self.gs_connect.add_photo(nm_ids_photo)

        # добавляем данные артикулов в psql в таблицу article
        if result_nm_ids_data:
            # db = self.database()
            try:
                async with Database1() as connection:
                    psql_article = ArticleTable(db=connection)

                    # ограничение функции: добавляет данные в psql, но только если их не было в бд json
                    filter_nm_ids = await psql_article.check_nm_ids(account="None", nm_ids=filter_nm_ids_data)
                    if filter_nm_ids:
                        logger.info(f"filter_nm_ids {filter_nm_ids}")
                        await psql_article.update_articles(data=result_nm_ids_data, filter_nm_ids=filter_nm_ids)
                    logger.info("данные по артикулам добавлены в таблицу article psql")
            except Exception as e:
                logger.exception(e)

        return result_nm_ids_data

    async def change_cards_and_tables_data(self, db_nm_ids_data, edit_data_from_table):
        sheet_statuses = ServiceGoogleSheet.check_status()
        net_profit_status = sheet_statuses['Отрицательная \nЧП']
        price_discount_edit_status = sheet_statuses['Цены/Скидки']
        dimensions_edit_status = sheet_statuses['Габариты']
        quantity_edit_status = sheet_statuses['Остаток']
        updates_nm_ids_data = {}
        edit_column_clean = {"price_discount": False, "dimensions": False, "qty": False}

        logger.info("Получил данные по ячейкам на изменение товара")
        for account, nm_ids_data in edit_data_from_table["nm_ids_edit_data"].items():
            valid_data_result = await validate_data(db_nm_ids_data, nm_ids_data)
            token = get_wb_tokens()[account.capitalize()]
            warehouses = WarehouseMarketplaceWB(token=token)
            warehouses_qty_edit = LeftoversMarketplace(token=token)

            # пройдет если данные будут валидны для изменения
            if len(valid_data_result) > 0:
                logger.info("Данные валидны")
                wb_api_price_and_discount = ListOfGoodsPricesAndDiscounts(token=token)
                wb_api_content = ListOfCardsContent(token=token)

                size_edit_data = []  # данные с артикулами на изменение габаритов
                price_discount_data = []  # данные с артикулами на изменение цены и/или цены
                quantity_edit_data = []
                for nm_id, data in valid_data_result.items():
                    # статус на изменение цены\скидки должен быть активным
                    if ("price_discount" in data and price_discount_edit_status and
                            (data['net_profit'] >= 0 or net_profit_status)):
                        price_discount_data.append({"nmID": nm_id, **data["price_discount"]})
                    # статус на изменение габаритов должен быть активным (вкл/выкл бот)
                    if "dimensions" in data and dimensions_edit_status:
                        size_edit_data.append({"nmID": nm_id, "vendorCode": data["vendorCode"],
                                               "sizes": data["sizes"], "dimensions": data["dimensions"]})
                """запрос на изменение цены и/или скидки по артикулу"""
                if price_discount_data:
                    wb_api_price_and_discount.add_new_price_and_discount(price_discount_data)
                    edit_column_clean["price_discount"] = True

                """Запрос на изменение габаритов товара по артикулу и vendorCode (wild)"""
                if size_edit_data:
                    wb_api_content.size_edit(size_edit_data)
                    edit_column_clean["dimensions"] = True
                """Перезаписываем данные в таблице после их изменений на WB"""
                nm_ids_result = [int(nm_ids_str) for nm_ids_str in valid_data_result.keys()]
                updates_nm_ids_data.update({account: nm_ids_result})

            if (account in edit_data_from_table["qty_edit_data"] and
                    (len(edit_data_from_table["qty_edit_data"][account]["stocks"]) > 0 and quantity_edit_status)):
                "изменение остатков на всех складах продавца"
                logger.info("изменение остатков на всех складах продавца")
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
        if updates_nm_ids_data:
            await asyncio.sleep(5)
            return await self.add_new_data_from_table(lk_articles=updates_nm_ids_data,
                                                      only_edits_data=True, add_data_in_db=False,
                                                      check_nm_ids_in_db=False)

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
            logger.info(f"Актуализируем выручку по текущему дню: {last_day}")
            if self.gs_service_revenue_connect.check_last_day_header_from_table(header=last_day):
                # По умолчанию begin_date - дата сегодняшнего дня. Если будет смещение, то begin_date будет форматирован
                # на вчерашний, чтобы актуализировать выручку за вчерашний день так же
                begin_date = datetime.date.today() - datetime.timedelta(days=1)
                logger.info(f" {last_day} заголовка нет в таблице. Будет добавлен включая выручка по дню")
                # сначала сдвигаем колонки с выручкой и добавляем заголовок нового дня
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
                    logger.info(f"Ошибка при получении недельной выручки: {res}")

                else:
                    all_accounts_new_revenue_data.update(res["result_data"])

                    account_data_for_days = res["all_data"]
                    for day, account_data in account_data_for_days.items():
                        if day not in psql_data_update:
                            psql_data_update[day] = {}
                        psql_data_update[day].update(account_data)

                add_orders_data(res["result_data"])
            logger.info("Выручка добавлена в БД")
            # добавляем их таблицу

            # проверяем заголовок прошлой недели
            week_date = list(get_last_weeks_dates().keys())
            if self.gs_service_revenue_connect.check_last_day_header_from_table(header=week_date[0]):
                logger.info(f"Заголовка {week_date[0]} нет в таблице, будет добавлен со смещением столбцов")
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
            logger.info("fСобрали выручку по всем кабинетам timer: {datetime.datetime.now() - start}")
            # print("all_accounts_new_revenue_data", all_accounts_new_revenue_data)
            self.gs_service_revenue_connect.update_revenue_rows(data_json=all_accounts_new_revenue_data)
            logger.info(f"Выручка в таблице актуализирована по всем артикулам.")
            start = datetime.datetime.now()
            # актуализация информация по заказам в листах таблицы
            await self.add_orders_data_in_table()
            try:
                logger.info("Актуализируем данные в бд таблицы accurate_net_profit_data")
                await self.add_data_in_db_psql(psql_data_update=psql_data_update, net_profit_time=current_time,
                                               nm_ids_table_data=nm_ids_table_data)

            except asyncio.TimeoutError as e:
                logger.error(f"[ERROR] {e}")
                logger.info("повторная попытка: Актуализируем данные в бд таблицы accurate_net_profit_data")
                await self.add_data_in_db_psql(psql_data_update=psql_data_update, net_profit_time=current_time,
                                               nm_ids_table_data=nm_ids_table_data)

            logger.info(f"Функция актуализации timer: {datetime.datetime.now() - start}")

    @staticmethod
    def check_status():
        for i in range(10):
            try:
                sheet_status = GoogleSheet(creds_json="creds.json",
                                           spreadsheet="UNIT 2.0 (tested)", sheet="ВКЛ/ВЫКЛ Бот")
                return sheet_status.check_status_service_sheet()
            except gspread.exceptions.APIError as e:
                logger.error(f"попытка {i} {e} следующая попытка через 75 секунд")
                time.sleep(75)

        return False

    @staticmethod
    def __validate_value(value: str, type_value: type) -> Any:
        try:
            return type_value(value)
        except (ValueError, TypeError):
            return

    def __convert_to_dict(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Преобразует данные из базы данных в словарь с удобочитаемыми ключами.
        Аргументы:
            data (dict): Словарь с данными из БД о товаре
        Возвращает:
            dict: Словарь с преобразованными ключами и обработанными значениями
        """
        return {
            "Артикул": self.__validate_value(data["article_id"], int),  # int
            "Предмет": self.__validate_value(data["subject_name"], str),  # str
            "Скидка %": self.__validate_value(data["discount"], int),  # int
            "Текущая\nДлина (см)": self.__validate_value(data["length"], int),  # int
            "Текущая\nШирина (см)": self.__validate_value(data["width"], int),  # int
            "Текущая\nВысота (см)": self.__validate_value(data["height"], int),  # int
            "Баркод": self.__validate_value(data["barcode"], str),  # str
            "Логистика от склада WB до ПВЗ": self.__validate_value(data["logistic_from_wb_wh_to_opp"], float),  # float
            "Комиссия WB": self.__validate_value(data["commission_wb"], float),  # float
            "Цена на WB без скидки": self.__validate_value(data["price"], float),  # float
            "Рейтинг": self.__validate_value(data["rating"], float),  # float
            "wild": self.__validate_value(data["local_vendor_code"], str),  # str
            "Фото": self.__validate_value(data["photo_link"], str)  # str
        }

    async def get_actually_data_from_db(self, db: Database1, article_ids: Set[int]) -> Dict[int, Dict[str, Any]]:
        """
        Асинхронно получает актуальные данные из базы данных по указанным артикулам.
        Аргументы:
            article_ids (set): Множество артикулов, по которым нужно получить данные
        Возвращает:
            dict: Словарь, где ключ - артикул товара, значение - данные о товаре в формате словаря
        """
        card_data_result = await CardData(db).get_actual_information_to_db(article_ids)
        return {data["article_id"]: self.__convert_to_dict(data)
                for data in card_data_result}

    async def get_actually_data_to_table_refactor(self, db: Database1) -> tuple[Any]:
        """
        Асинхронно собирает актуальные данные из базы данных для всех артикулов из гугл-таблицы.
        Создает и выполняет асинхронные задачи для каждого аккаунта.
        Возвращает:
            list: Список словарей с актуальными данными по артикулам
        """
        logger.info("Получение артикулов из гугл-таблицы")
        lk_articles = self.gs_connect.create_lk_articles_list()
        tasks = []
        logger.info("Получение актуальных данных из базы данных")
        for account, articles in lk_articles.items():
            task = self.get_actually_data_from_db(db, set(articles))
            tasks.append(task)
        return await asyncio.gather(*tasks)

    @staticmethod
    def _get_photos_and_filter_empty_value(article_id_to_update: List[Dict[str, Any]]) -> Tuple[
        Dict[str, Any], Dict[str, Any]]:
        """
        Извлекает ссылки на фотографии товаров и удаляет пустые значения из данных.
        Аргументы:
            article_id_to_update (list): Список словарей с данными о товарах
        Возвращает:
            tuple: (отфильтрованные данные, данные о фотографиях)
                - отфильтрованные данные - список словарей с данными товаров без пустых значений
                - данные о фотографиях - список словарей, содержащих только артикулы и ссылки на фото
        """
        logger.info("Извлечение ссылок на фотографии товаров")
        photos = [{k: {"Фото": v.pop('Фото')} for k, v in data.items()} for data in article_id_to_update]
        logger.info("Удаление пустых значений")
        for items in article_id_to_update:
            for k, v in items.items():
                for key, value in list(v.items()):
                    if not value:
                        items[k].pop(key)
        article_id_to_update = {k: v for d in article_id_to_update for k, v in d.items()}
        photos = {k: v for d in photos for k, v in d.items()}
        return article_id_to_update, photos

    async def add_actually_data_to_table(self, db: Database1):
        """
        Обновляет данные в гугл-таблице актуальной информацией из базы данных.
        """
        if ServiceGoogleSheet.check_status()['ВКЛ - 1 /ВЫКЛ - 0']:
            logger.info(f"[INFO] {datetime.datetime.now()} актуализируем данные в таблице")
            article_id_to_update = await self.get_actually_data_to_table_refactor(db=db)
            article_id_to_update, photos = self._get_photos_and_filter_empty_value(article_id_to_update)
            logger.info(f"[INFO] {datetime.datetime.now()} обновляем данные в таблице")
            self.gs_connect.update_rows(data_json=article_id_to_update)
            if len(photos) > 0:
                logger.info(f"[INFO] {datetime.datetime.now()} обновляем данные в таблице ФОТО")
                gs_connect_photo = GoogleSheet(creds_json=self.creds_json, spreadsheet=self.spreadsheet, sheet="ФОТО")
                await gs_connect_photo.add_data_to_count_list(photos)

    async def get_actually_virtual_qty(self, account, data: dict, token):
        articles_qty_data = {}
        warehouses = WarehouseMarketplaceWB(token=token)
        barcodes_quantity = LeftoversMarketplace(token=token)

        for warehouse_id in warehouses.get_account_warehouse():
            bqs_result = barcodes_quantity.get_amount_from_warehouses(
                warehouse_id=warehouse_id['id'],
                barcodes=list(data.keys()))

            for qty_data in bqs_result:
                barcode = qty_data['Баркод']
                qty = qty_data['остаток']
                article = data.get(barcode, None)
                if article is not None:
                    articles_qty_data[article] = {"ФБС": qty}
        return articles_qty_data

    async def get_actually_data_by_qty(self):

        if ServiceGoogleSheet.check_status()['ВКЛ - 1 /ВЫКЛ - 0']:
            logger.info(f"[INFO] {datetime.datetime.now()} актуализируем данные по остаткам в таблице")
            gs_connect_main = GoogleSheet(creds_json=self.creds_json, spreadsheet=self.spreadsheet, sheet=self.sheet)
            lk_articles = await gs_connect_main.create_lk_barcodes_articles()
            gs_connect_warehouses_info = GoogleSheet(creds_json=self.creds_json, spreadsheet=self.spreadsheet,
                                                     sheet="Склады ИНФ")
            # словарь с регионом и группой складов
            warehouses_info = await gs_connect_warehouses_info.get_warehouses_info()
            logger.info(f"warehouse_info : {warehouses_info}")
            tasks_qty = []
            tasks_virtual_qty = []
            tokens = get_wb_tokens()
            for account, data in lk_articles.items():
                token = tokens[account.capitalize()]
                task = asyncio.create_task(
                    self.get_qty_data_by_account(account=account, data=data, warehouses_info=warehouses_info,
                                                 token=token))
                tasks_qty.append(task)

                task_2 = asyncio.create_task(
                    self.get_actually_virtual_qty(account=account, data=data, token=token)
                )
                tasks_virtual_qty.append(task_2)
            together_qty_result = await asyncio.gather(*tasks_qty, return_exceptions=True)
            together_virtual_qty_result = await asyncio.gather(*tasks_virtual_qty, return_exceptions=True)
            articles_qty_wb = {}  # результат данных по отслеживаемым регионам\складам
            untracked_warehouses = {}  # результат данных по неотслеживаемым складам и сумма остаток
            for tr in together_qty_result:
                articles_qty_wb.update(tr['articles_qty_wb'])
                for key, value in tr['untracked_warehouses'].items():
                    # untracked_warehouses[key] = untracked_warehouses.get(key, 0) + value
                    if key not in untracked_warehouses:
                        untracked_warehouses[key] = {"ОСТАТКИ": value}
                    untracked_warehouses[key]["ОСТАТКИ"] += value

            virtual_qty_data = {}
            for tr_virtual in together_virtual_qty_result:
                virtual_qty_data.update(tr_virtual)

            merge_qty_data = merge_dicts(articles_qty_wb, virtual_qty_data)
            # update по остаткам в sheet UNIT
            await gs_connect_main.update_qty_by_reg(update_data=merge_qty_data)
            # update по неотслеживаемым складам для sheet 'Склады ИНФ'
            await gs_connect_warehouses_info.update_untracked_warehouses_quantity(update_data=untracked_warehouses)
            logger.info("Данные по остаткам обновлены в таблице.")

    async def get_qty_data_by_account(self, account, data, token, warehouses_info):
        wh_analytics = AnalyticsWarehouseLimits(token=token)

        barcodes_set = set(data.keys())  # баркоды по аккаунту с таблицы
        articles_qty_wb = {}
        untracked_warehouses = {}
        task_id = await wh_analytics.create_report()
        wb_warehouse_qty = await wh_analytics.check_data_by_task_id(task_id=task_id)

        if task_id is not None and len(wb_warehouse_qty) > 0:
            if wb_warehouse_qty:  # собираем остатки со складов WB
                return self.get_articles_qty_wb_new_method(account=account, barcodes_set=barcodes_set, wb_warehouse_qty=wb_warehouse_qty,
                                                           warehouses_info=warehouses_info, data=data)
        return {"articles_qty_wb": articles_qty_wb, "untracked_warehouses": untracked_warehouses}

    @staticmethod
    def get_articles_qty_wb(wb_warehouse_qty, barcodes_set, data, warehouses_info, account):
        articles_qty_wb = {}
        untracked_warehouses = {}
        for qty_data in wb_warehouse_qty:
            try:  # почему-то начал отображать в данные по артикулам без ключа barcode
                if qty_data['barcode'] in barcodes_set:
                    barcode = qty_data['barcode']
                    article = data[barcode]
                    articles_qty_wb[article] = {
                        "ФБО": qty_data['quantityWarehousesFull'],
                    }
                    warehouses = qty_data['warehouses']

                    if len(warehouses) > 0:
                        for wh_data in warehouses:
                            warehouse_name = wh_data["warehouseName"]

                            if warehouse_name in warehouses_info:
                                region_name_by_warehouse = warehouses_info[warehouse_name]
                                # по задумке должен суммировать остатки всех закрепленных регионов к складам
                                if region_name_by_warehouse not in articles_qty_wb[article]:
                                    articles_qty_wb[article][region_name_by_warehouse] = 0
                                articles_qty_wb[article][region_name_by_warehouse] += wh_data["quantity"]

                            else:
                                if warehouse_name not in untracked_warehouses:
                                    untracked_warehouses[warehouse_name] = 0
                                untracked_warehouses[warehouse_name] += wh_data["quantity"]

                    clean_data = {"Центральный": "",
                                  "Южный": "",
                                  "Северо-Кавказский": "",
                                  "Приволжский": ""}

                    for cd in clean_data:
                        if cd not in articles_qty_wb[article]:
                            articles_qty_wb[article].update({cd: ""})
            except KeyError as e:
                logger.error(e)
                logger.info(account)
                logger.info(qty_data)

        return {"articles_qty_wb": articles_qty_wb, "untracked_warehouses": untracked_warehouses}

    @staticmethod
    def get_articles_qty_wb_new_method(wb_warehouse_qty, barcodes_set, data, warehouses_info, account):

        articles_qty_wb = {}
        untracked_warehouses = {}
        for qty_data in wb_warehouse_qty:
            try:  # почему-то начал отображать в данные по артикулам без ключа barcode
                if qty_data['barcode'] in barcodes_set:
                    barcode = qty_data['barcode']
                    article = data[barcode]
                    articles_qty_wb[article] = {
                        "ФБО": 0,
                        "Центральный": "",
                        "Южный": "",
                        "Северо-Кавказский": "",
                        "Приволжский": ""
                    }
                    warehouses = qty_data['warehouses']

                    if len(warehouses) > 1:
                        for wh_data in warehouses:
                            warehouse_name = wh_data["warehouseName"]
                            if warehouse_name == "Всего находится на складах":
                                articles_qty_wb[article]['ФБО'] += wh_data['quantity']

                            if warehouse_name in warehouses_info:
                                region_name_by_warehouse = warehouses_info[wh_data["warehouseName"]]

                                if articles_qty_wb[article][region_name_by_warehouse] == "":
                                    articles_qty_wb[article][region_name_by_warehouse] = 0
                                articles_qty_wb[article][region_name_by_warehouse] += wh_data["quantity"]

                            else:
                                # сбор данных по остаткам складов которые не отслеживаются по регионам
                                if warehouse_name in ("Всего находится на складах", "В пути возвраты на склад WB",
                                                      "В пути до получателей"):
                                    continue
                                if warehouse_name not in untracked_warehouses:
                                    untracked_warehouses[warehouse_name] = 0
                                untracked_warehouses[warehouse_name] += wh_data["quantity"]
            except KeyError as e:
                logger.error(e)
                logger.info(account)
                logger.info(qty_data)
        return {"articles_qty_wb": articles_qty_wb, "untracked_warehouses": untracked_warehouses}

    async def check_quantity_flag(self):
        logger.info("Проверка остатков по лимитам из столбца 'Минимальный остаток'")
        status = ServiceGoogleSheet.check_status()
        status_limit_edit = int(status["Добавить если"])
        status_off_on_service = bool(int(status["ВКЛ - 1 /ВЫКЛ - 0"]))
        logger.info(status_limit_edit)
        # если сервис включен
        if status_off_on_service and status_limit_edit:  # если включены флаги на изменение остатков
            low_limit_qty_data = self.gs_connect.get_data_quantity_limit()
            pprint(low_limit_qty_data)
            sopost_data = GoogleSheetSopostTable().wild_quantity()
            pprint(sopost_data)
            nm_ids_for_update_data = {}
            if len(low_limit_qty_data) > 0:
                logger.info(f"Есть остатки ниже установленного флага: {low_limit_qty_data}")
                for account, edit_data in low_limit_qty_data.items():
                    update_qty_data = []
                    token = get_wb_tokens()[account.capitalize()]
                    warehouses = WarehouseMarketplaceWB(token=token).get_account_warehouse()
                    qty_edit = LeftoversMarketplace(token=token)

                    if len(edit_data["qty"]) > 0:
                        for qty_data in edit_data["qty"]:
                            add_qty = str(sopost_data[qty_data["wild"]]).replace("\xa0", "")
                            if add_qty.isdigit() and int(add_qty) != 0:
                                update_qty_data.append(
                                    {
                                        "sku": qty_data["sku"],
                                        "amount": int(add_qty)
                                    }
                                )
                    print(account)
                    pprint(update_qty_data)
                    if len(update_qty_data):
                        for warehouse_id in warehouses:
                            qty_edit.edit_amount_from_warehouses(warehouse_id=warehouse_id["id"],
                                                                 edit_barcodes_list=update_qty_data)

                        if account not in nm_ids_for_update_data:
                            nm_ids_for_update_data[account] = []
                        logger.info("nm_ids_for_update_data")
                        logger.info(nm_ids_for_update_data)
                        nm_ids_for_update_data[account].extend(low_limit_qty_data[account]['nm_ids'])
            if len(nm_ids_for_update_data) > 0:
                logger.info(nm_ids_for_update_data)
                nm_ids_data_json = await self.add_new_data_from_table(lk_articles=nm_ids_for_update_data,
                                                                      only_edits_data=True, add_data_in_db=False,
                                                                      check_nm_ids_in_db=False)
                # if len(low_limit_qty_data["edit_min_qty"]) > 0:
                #     logger.info(low_limit_qty_data["edit_min_qty"])
                #     nm_ids_data_json = merge_dicts(nm_ids_data_json, low_limit_qty_data["edit_min_qty"])

                self.gs_connect.update_rows(data_json=nm_ids_data_json,
                                            edit_column_clean={"qty": False, "price_discount": False,
                                                               "dimensions": False})

    async def add_orders_data_in_table(self):
        from utils import get_order_data_from_database
        gs_connect = GoogleSheet(sheet="Количество заказов", spreadsheet=settings.SPREADSHEET,
                                 creds_json=settings.CREEDS_FILE_NAME)
        orders_count_data = get_order_data_from_database()
        date_object = datetime.datetime.today()
        today = date_object.strftime("%d.%m")
        # если нет текущего дня
        if gs_connect.check_header(header=today):
            logger.info(f"Нет текущего дня {today} в листах. Сервис сместит данные по дням")
            # сместит заголовки дней в листе "Количество заказов"
            gs_connect.shift_headers_count_list(today)
            # сместит заголовки дней в листе "MAIN"
            self.gs_connect.shift_orders_header(today=today)
        # если есть данные в БД - будут добавлены в лист
        if len(orders_count_data):
            logger.info("актуализируем данные по заказам в таблице")
            await gs_connect.add_data_to_count_list(data_json=orders_count_data)

        logger.info("Обновлено количество заказов по дням в MAIN")
        """ Функция добавления количества заказов по дням в таблицу """

    async def add_data_in_db_psql(self, psql_data_update, net_profit_time, nm_ids_table_data):
        logger.info("Открываем pool в подключении к БД")
        async with Database1() as connection:
            accurate_net_profit_table = AccurateNetProfitTable(db=connection)

            logger.info("Смотрим данные в psql_data_update")
            for date, psql_data in psql_data_update.items():
                nm_ids_list = list(psql_data.keys())
                logger.info("Запрос по новым артикулам")
                psql_new_nm_ids = await accurate_net_profit_table.check_nm_ids(nm_ids=nm_ids_list, account=None,
                                                                               date=date)
                logger.info("новые артикулы которых нет в таблице accurate_net_profit_data:", psql_new_nm_ids)
                "Добавляем данные в бд psql"
                if psql_new_nm_ids:  # добавляем новые артикулы в бд psql
                    # если новых артикулов нет в таблице net_profit, то будут добавлены
                    await accurate_net_profit_table.add_new_article_net_profit_data(time=net_profit_time,
                                                                                    data=psql_data,
                                                                                    nm_ids_net_profit=nm_ids_table_data,
                                                                                    new_nm_ids=psql_new_nm_ids)
                    logger.info("артикулы в бд таблицы accurate_net_profit_data актуализированы")

                # актуализируем информацию по полученному с таблицы чп по артикулам
                await accurate_net_profit_table.update_net_profit_data(time=net_profit_time,
                                                                       response_data=psql_data,
                                                                       nm_ids_table_data=nm_ids_table_data,
                                                                       date=date)
                logger.info("актуализированы данные в бд таблицы accurate_net_profit_data")

                logger.info("[INFO] Получаем данные с бд с таблицы accurate_net_profit_data")
                result_som_net_profit_data = await accurate_net_profit_table.get_net_profit_by_date(date=date)

                formatted_result = {}
                for record in result_som_net_profit_data:
                    article_id = record['article_id']
                    sum_value = record['sum_snp']
                    date_obj = datetime.datetime.strptime(date, "%Y-%m-%d")

                    formatted_date_str = date_obj.strftime("%d.%m")
                    formatted_result[article_id] = {formatted_date_str: int(sum_value)}

                logger.info("[INFO] Обновляем данные по сумме ЧП в листе MAIN")
                self.gs_service_revenue_connect.update_revenue_rows(data_json=formatted_result)
                logger.info("данные по ЧП актуализированы")

    async def add_data_in_db_psql_purchase_calculation(self, psql_data_update, net_profit_time, nm_ids_table_data):
        try:
            logger.info("Открываем pool в подключении к БД")
            async with Database1() as connection:
                accurate_net_profit_pc_table = AccurateNetProfitPCTable(db=connection)

                logger.info("Смотрим данные в psql_data_update")
                for date, psql_data in psql_data_update.items():
                    nm_ids_list = list(psql_data.keys())
                    logger.info("Запрос по новым артикулам")
                    psql_new_nm_ids = await accurate_net_profit_pc_table.check_nm_ids(nm_ids=nm_ids_list, account=None,
                                                                                      date=date)
                    logger.info("новые артикулы которых нет в таблице accurate_npd_purchase_calculation:",
                                psql_new_nm_ids)
                    "Добавляем данные в бд psql"
                    if psql_new_nm_ids:  # добавляем новые артикулы в бд psql
                        # если новых артикулов нет в таблице net_profit, то будут добавлены
                        await accurate_net_profit_pc_table.add_new_article_net_profit_data(time=net_profit_time,
                                                                                           data=psql_data,
                                                                                           nm_ids_net_profit=nm_ids_table_data,
                                                                                           new_nm_ids=psql_new_nm_ids)
                        logger.info("артикулы в бд таблицы accurate_npd_purchase_calculation актуализированы")

                    # актуализируем информацию по полученному с таблицы чп по артикулам
                    await accurate_net_profit_pc_table.update_net_profit_data(time=net_profit_time,
                                                                              response_data=psql_data,
                                                                              nm_ids_table_data=nm_ids_table_data,
                                                                              date=date)
                    logger.info("актуализированы данные в бд таблицы accurate_npd_purchase_calculation")
        except KeyError as e:
            logger.info(f"[ERROR] (func) add_data_in_db_psql_purchase_calculation, KeyError {e}")

    async def update_purchase_calculation_data(self):
        gs_pc_service = PCGoogleSheet(creds_json=Setting().CREEDS_FILE_NAME, sheet=Setting().PC_SHEET,
                                      spreadsheet=Setting().PC_SPREADSHEET)
        # result = gs_pc_service.create_lk_articles_dict()
        # pprint(result)
        date_object = datetime.datetime.today() - datetime.timedelta(days=1)
        yesterday = date_object.strftime("%d.%m")
        logger.info(yesterday)
        if gs_pc_service.check_last_day_header_from_table(yesterday):
            gs_pc_service.shift_orders_header(day=yesterday)
            logger.info(f"Добавлен новый заголовок: {yesterday}")
        logger.info("Актуализируем данные по артикулам из листа MAIN в лист ПРОДАЖИ")
        update_columns_in_purchase_calculation()

        logger.info(" актуализируем данные по продажам из бд в таблицу ПРОДАЖИ")
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

        gs_pc_service.update_revenue_rows(edit_result)

    async def add_data_by_net_profit(self):
        """Функция для актуализации ЧП за конкретный день.(Если данные за день вдруг не подгрузились в таблице)"""
        async with Database1() as connection:
            date = '2024-12-16'
            accurate_net_profit_table = AccurateNetProfitTable(db=connection)
            result_som_net_profit_data = await accurate_net_profit_table.get_net_profit_by_date(date=date)

            formatted_result = {}
            for record in result_som_net_profit_data:
                article_id = record['article_id']
                sum_value = record['sum_snp']
                date_obj = datetime.datetime.strptime(date, "%Y-%m-%d")

                formatted_date_str = date_obj.strftime("%d.%m")
                formatted_result[article_id] = {formatted_date_str: int(sum_value)}

            logger.info("[INFO] Обновляем данные по сумме ЧП в листе MAIN")
            self.gs_service_revenue_connect.update_revenue_rows(data_json=formatted_result)
            logger.info("данные по ЧП актуализированы")

    async def turnover_of_goods(self):
        yesterday = str(datetime.datetime.today().date() - datetime.timedelta(days=1))
        logger.info(yesterday)
        """Оборачиваемость товара по оценке остатков. Вычисление среднего за день и за неделю"""
        gs_connect_main = GoogleSheet(creds_json=self.creds_json, spreadsheet=self.spreadsheet, sheet=self.sheet)
        lk_articles = await gs_connect_main.create_lk_barcodes_articles()
        gs_connect_warehouses_info = GoogleSheet(creds_json=self.creds_json, spreadsheet=self.spreadsheet,
                                                 sheet="Склады ИНФ")
        warehouses_info = await gs_connect_warehouses_info.get_warehouses_info()
        logger.info(f"warehouse_info {warehouses_info}")
        tasks_by_qty = []
        tasks_by_supplies = []
        for account, data in lk_articles.items():
            token = get_wb_tokens()[account.capitalize()]
            statistic = Statistic(token=token)
            task_by_supplies = asyncio.create_task(statistic.get_supplies_data(date=yesterday))
            task_by_qty = asyncio.create_task(
                self.get_qty_data_by_account_for_turnover(account=account, token=token, data=data, warehouses_info=warehouses_info))
            tasks_by_qty.append(task_by_qty)
            tasks_by_supplies.append(task_by_supplies)

        results = await asyncio.gather(
            asyncio.gather(*tasks_by_qty),
            asyncio.gather(*tasks_by_supplies)
        )
        gather_result_by_qty, gather_result_by_supplies = results
        ready_qty_data = {}
        plus_supply = {}
        for qty in gather_result_by_qty:
            ready_qty_data.update(qty['articles_qty_wb'])
        for supplies in gather_result_by_supplies:
            if len(supplies) > 0:
                for data in supplies:
                    nm_id = data['nmId']
                    date_close = data['dateClose'].split('T')[0]
                    warehouse_name = data['warehouseName']
                    status = data['status']
                    supply_qty = data['quantity']
                    barcode = data['barcode']
                    if warehouse_name in warehouses_info:
                        if nm_id in ready_qty_data and status == "Принято" and yesterday == date_close:
                            district_by_warehouse = warehouses_info[warehouse_name]
                            logger.info(f"ready_qty_data[nm_id] {ready_qty_data[nm_id]}")

                            if nm_id in plus_supply:
                                if district_by_warehouse in plus_supply[nm_id]:
                                    plus_supply[nm_id][district_by_warehouse]['supply_qty'] += supply_qty
                                    plus_supply[nm_id][district_by_warehouse]['supply_count'] += 1
                                else:
                                    district_qty = ready_qty_data[nm_id].pop(district_by_warehouse)

                                    plus_supply[nm_id][district_by_warehouse] = {
                                        "barcode": barcode, "quantity": district_qty, "supply_qty": supply_qty,
                                        "supply_count": 1
                                    }
                            else:
                                district_qty = ready_qty_data[nm_id].pop(district_by_warehouse)
                                plus_supply[nm_id] = {
                                    district_by_warehouse: {"barcode": barcode, "quantity": district_qty,
                                                            "supply_qty": supply_qty, "supply_count": 1}
                                }

        logger.info(ready_qty_data)
        logger.info(plus_supply)
        async with Database1() as connection:
            inventory_turnover_by_reg = InventoryTurnoverByRegTable(db=connection)
            await inventory_turnover_by_reg.update_stock_balances(yesterday, ready_qty_data, plus_supply)
        logger.info("[INFO] Завершили актуализацию остатков")

    async def get_qty_data_by_account_for_turnover(self, account, token, data, warehouses_info):
        wh_analytics = AnalyticsWarehouseLimits(token=token)

        barcodes_set = set(data.keys())  # баркоды по аккаунту с таблицы
        articles_qty_wb = {}
        untracked_warehouses = {}
        task_id = await wh_analytics.create_report()
        wb_warehouse_qty = await wh_analytics.check_data_by_task_id(task_id=task_id)

        if task_id is not None and len(wb_warehouse_qty) > 0:
            if wb_warehouse_qty:  # собираем остатки со складов WB

                for qty_data in wb_warehouse_qty:
                    try:  # почему-то начал отображать в данные по артикулам без ключа barcode

                        if qty_data['barcode'] in barcodes_set:
                            barcode = qty_data['barcode']
                            article = data[barcode]
                            articles_qty_wb[article] = {
                                # "ФБО": qty_data['quantityWarehousesFull'],
                                "barcode": barcode
                            }
                            warehouses = qty_data['warehouses']

                            if len(warehouses) > 0:
                                for wh_data in warehouses:
                                    warehouse_name = wh_data["warehouseName"]
                                    if warehouse_name in warehouses_info:
                                        region_name_by_warehouse = warehouses_info[warehouse_name]
                                        # по задумке должен суммировать остатки всех закрепленных регионов к складам
                                        if region_name_by_warehouse not in articles_qty_wb[article]:
                                            articles_qty_wb[article][region_name_by_warehouse] = 0
                                        articles_qty_wb[article][region_name_by_warehouse] += wh_data["quantity"]

                                    else:
                                        if warehouse_name not in untracked_warehouses:
                                            untracked_warehouses[warehouse_name] = 0
                                        untracked_warehouses[warehouse_name] += wh_data["quantity"]

                            clean_data = {"Центральный": "",
                                          "Южный": "",
                                          "Северо-Кавказский": "",
                                          "Приволжский": ""}

                            for cd in clean_data:
                                if cd not in articles_qty_wb[article]:
                                    articles_qty_wb[article][cd] = 0
                    except KeyError as e:
                        logger.error(e)
                        logger.info(account)
                        logger.info(qty_data)
        return {"articles_qty_wb": articles_qty_wb, "untracked_warehouses": untracked_warehouses}

    async def find_out_orders_by_balances(self):
        date1 = '2024-12-19'  # example
        date2 = '2024-12-20'  # example
        yesterday = str(datetime.datetime.today().date() - datetime.timedelta(days=1))  # use
        previous_day = str(datetime.datetime.today().date() - datetime.timedelta(days=2))  # use
        async with Database1() as connection:
            inventory_turnover_by_reg = InventoryTurnoverByRegTable(db=connection)

            data_to_update = []
            last_day_dict = await create_valid_data_from_db(await inventory_turnover_by_reg.get_data_by_day(date2))
            previous_day_dict = await create_valid_data_from_db(await inventory_turnover_by_reg.get_data_by_day(date1))
            # pprint(last_day_dict)
            error_article = []
            for article, district_data in last_day_dict.items():
                #
                try:
                    previous_dist_data = previous_day_dict[article]
                    for district, last_data in district_data.items():
                        previous_data = previous_dist_data[district]
                        orders_per_day = last_data['supply_qty'] + previous_data['quantity'] - last_data['quantity']
                        if orders_per_day < 0:
                            logger.info(
                                f"article: {article} {last_data['supply_qty']}, {previous_data['quantity']}, {last_data['quantity']}")
                            logger.info(orders_per_day)
                        data_to_update.append(
                            (article, district, orders_per_day, datetime.datetime.strptime(date2, "%Y-%m-%d"))
                        )
                except KeyError as e:
                    error_article.append(article)
                    logger.error(f" KeyError {e}")
            # await inventory_turnover_by_reg.update_orders(data=data_to_update)
            logger.info("Данные по заказам ФБО обновлено в бд таблицы 'inventory_turnover_by_reg'")

    async def get_data_by_supplies(self):
        # search_date = str(datetime.datetime.today().date() - datetime.timedelta(days=1))
        search_date = '2024-12-19'
        search_date_format = datetime.datetime.strptime(search_date, "%Y-%m-%d")
        gs_connect_main = GoogleSheet(creds_json=self.creds_json, spreadsheet=self.spreadsheet, sheet=self.sheet)
        lk_articles = await gs_connect_main.create_lk_barcodes_articles()
        gs_connect_warehouses_info = GoogleSheet(creds_json=self.creds_json, spreadsheet=self.spreadsheet,
                                                 sheet="Склады ИНФ")
        warehouses_info = await gs_connect_warehouses_info.get_warehouses_info()
        logger.info(f"warehouse_info : {warehouses_info}")
        tasks_by_supplies = []
        articles = []  # для сверки из полученных данных по поставкам
        for account, data in lk_articles.items():
            articles.extend(list(data.values()))
            token = get_wb_tokens()[account.capitalize()]
            statistic = Statistic(token=token)
            task_by_supplies = asyncio.create_task(statistic.get_supplies_data(date=search_date))
            tasks_by_supplies.append(task_by_supplies)

        # (article, date, federal_district, supply_qty, supply_count)
        result_dict_data = {}
        gather_results = await asyncio.gather(*tasks_by_supplies)
        for results in gather_results:
            for data in results:
                if len(data) > 0:
                    # for data in res:
                    nm_id = data['nmId']
                    date = data['date'].split('T')[0]
                    warehouse_name = data['warehouseName']
                    supply_qty = data['quantity']
                    if warehouse_name in warehouses_info and date == search_date and nm_id in articles:
                        logger.info(data)

                        district_by_warehouse = warehouses_info[warehouse_name]
                        if nm_id in result_dict_data:
                            if district_by_warehouse in result_dict_data[nm_id]:
                                result_dict_data[nm_id][district_by_warehouse]['supply_qty'] += supply_qty
                                result_dict_data[nm_id][district_by_warehouse]['supply_count'] += 1
                            else:

                                result_dict_data[nm_id][district_by_warehouse] = {
                                    "date": date, "article_id": nm_id, "supply_qty": supply_qty, "supply_count": 1
                                }
                        else:
                            result_dict_data[nm_id] = {
                                district_by_warehouse: {"date": date, "article_id": nm_id, "supply_qty": supply_qty,
                                                        "supply_count": 1}
                            }
        logger.info(result_dict_data)
        prepare_data_for_db = []

        for article, district_data in result_dict_data.items():
            for district, data in district_data.items():
                prepare_data_for_db.append(
                    (article, district, search_date_format, data['supply_qty'], data['supply_count'])
                )

        logger.info(prepare_data_for_db)
        async with Database1() as connection:
            inventory_turnover_by_reg = InventoryTurnoverByRegTable(db=connection)
            await inventory_turnover_by_reg.update_supplies(data=prepare_data_for_db)
        logger.info("end test")

    async def get_orders_by_federal_district(self):
        logger.info("Актуализируем данные по заказам по всем кабинетам в бд")
        gs_connect_warehouses_info = GoogleSheet(creds_json=self.creds_json, spreadsheet=self.spreadsheet,
                                                 sheet="Склады ИНФ")
        warehouses_info = await gs_connect_warehouses_info.get_warehouses_info()

        date = str(datetime.datetime.today().date() - datetime.timedelta(days=1))
        date_obj = datetime.datetime.strptime(date, "%Y-%m-%d")

        tasks = []
        for account, token in get_wb_tokens().items():
            statistics = Statistic(token=token)
            task = asyncio.create_task(statistics.get_orders_data(date=date))
            tasks.append(task)

        together_result = await asyncio.gather(*tasks)
        results = []
        for t_res in together_result:
            results.extend(t_res)

        sum_order_data_by_fbo = {}
        for res in results:
            warehouse = res['warehouseName']

            if res['warehouseType'] == 'Склад WB' and warehouse in warehouses_info:
                federal_district = warehouses_info[warehouse]
                article_id = res['nmId']
                barcode = res['barcode']
                vendor_code = res['supplierArticle']

                if article_id not in sum_order_data_by_fbo:
                    sum_order_data_by_fbo[article_id] = {
                        "federal_district": {},  # количество заказов
                        "barcode": barcode,
                        "date": date_obj,
                        "vendor_code": vendor_code
                    }
                if federal_district not in sum_order_data_by_fbo[article_id]["federal_district"]:
                    sum_order_data_by_fbo[article_id]["federal_district"].update({federal_district: 0})

                sum_order_data_by_fbo[article_id]["federal_district"][federal_district] += 1

        records_data = []
        for article, data in sum_order_data_by_fbo.items():
            for district_name, count in data['federal_district'].items():
                records_data.append(
                    (article, date_obj, district_name, count, int(data['barcode']), data['vendor_code'])
                )

        logger.info(f"Records Data:\n{pformat(records_data, width=100, compact=True)}")
        async with Database1() as connection:
            orders_by_federal_district = OrdersByFederalDistrict(db=connection)
            await orders_by_federal_district.add_orders_data(records_data=records_data)
            logger.info("Данные по заказам актуализированы в БД")

    async def get_awg_data_by_orders(self):
        start_day = datetime.datetime.today().date() - datetime.timedelta(days=7)
        end_day = datetime.datetime.today().date() - datetime.timedelta(days=1)
        async with Database1() as connection:
            orders_by_federal_district = OrdersByFederalDistrict(db=connection)
            avg_data = await orders_by_federal_district.get_awg_orders_per_days(start_day=start_day, end_day=end_day)
            headers_by_district = {"Центральный": "СВД - Ц",
                                   "Южный": "СВД - Ю",
                                   "Северо-Кавказский": "СВД - СК",
                                   "Приволжский": "СВД - П"}
            avg_data_for_update = {}
            for record in avg_data:
                article_id = record['article_id']
                avg_header_name = headers_by_district[record['federal_district']]
                avg_orders_result = record['avg_opd']
                if article_id not in avg_data_for_update:
                    avg_data_for_update[article_id] = {}
                avg_data_for_update[article_id].update({
                    avg_header_name: float(avg_orders_result)
                })
        logger.info("Получили avg данные c бд по заказам ")
        return avg_data_for_update

    async def actualize_avg_orders_data_in_table(self):
        # актуализация по заказам в бд
        await self.get_orders_by_federal_district()
        # получили средн. арифм. за последние 7 дней по заказам с каждого склада (фед округа)
        avg_data_by_orders = await self.get_awg_data_by_orders()
        # актуализируем данные в таблице
        await self.gs_connect.update_qty_by_reg(update_data=avg_data_by_orders)
        logger.info("Усредненные данные по заказам со складов\регионов актуализированы в Таблице")
