import datetime
from pprint import pprint

from APIGoogleSheet.googlesheet import GoogleSheetServiceRevenue, GoogleSheet
from APIWildberries.analytics import AnalyticsNMReport
from APIWildberries.content import ListOfCardsContent
from APIWildberries.prices_and_discounts import ListOfGoodsPricesAndDiscounts
from APIWildberries.tariffs import CommissionTariffs
from utils import calculate_sum_for_logistic, add_orders_data, merge_dicts, validate_data

test_creds_json = "creds.json"
test_spreadsheet = "test START"
test_sheet = "Лист3"


def add_info_for_articles(token, creds_json, spreadsheet, sheet, nm_ids_result: list = 0):
    gs_connect = GoogleSheet(creds_json=creds_json,
                             spreadsheet=spreadsheet, sheet=sheet)
    if len(nm_ids_result) > 0:
        print("есть новые артикулы в таблице")
        print(nm_ids_result)
        wb_list_of_cards = ListOfCardsContent(token=token)

        card_from_nm_ids_filter = wb_list_of_cards.get_list_of_cards(nm_ids_list=nm_ids_result, limit=100)
        goods_from_nm_ids_filter = ListOfGoodsPricesAndDiscounts(token=token)
        goods_nm_ids = goods_from_nm_ids_filter.get_log_for_nm_ids(filter_nm_ids=nm_ids_result)

        merge_json_data = merge_dicts(goods_nm_ids, card_from_nm_ids_filter)
        # pprint(result_json_data)

        subject_names = set()  # итог всех полученных с карточек предметов
        commission_traffics = CommissionTariffs(token=token)
        current_tariffs_data = commission_traffics.get_tariffs_box_from_marketplace()

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
        subject_commissions = commission_traffics.get_commission_on_subject(subject_names=subject_names)

        for sc in subject_commissions.items():
            for result_card in merge_json_data.values():
                if sc[0] == result_card["Предмет"]:
                    result_card['Комиссия WB'] = sc[1]

        gs_connect.update_rows(data_json=merge_json_data)

        """ Добавление выручки по новым артикулам"""
        analytics_connect = AnalyticsNMReport(token=token)

        nm_ids_revenue_data = analytics_connect.get_last_day_revenue(nm_ids=nm_ids_result,
                                                                     begin_date=datetime.date.today() - datetime.timedelta(
                                                                         days=7),
                                                                     end_date=datetime.date.today() - datetime.timedelta(
                                                                         days=1))

        gs_service_revenue_connect = GoogleSheetServiceRevenue(creds_json="creds.json",
                                                               spreadsheet="test START", sheet="Лист3")

        gs_service_revenue_connect.add_for_all_new_nm_id_revenue(nm_ids_revenue_data=nm_ids_revenue_data)
        add_orders_data(nm_ids_revenue_data)
        pprint(nm_ids_revenue_data)

        # Добавление в бд новых артикулов
        # add_new_nm_ids_in_db(nm_ids_result)
        print("Упали в ожидание")
    #
    else:
        print("Все Артикулы из столбца таблицы уже добавлены")
        print("Упали в ожидание")


def change_card_and_tables_data(spreadsheet, sheet, creds_json, token):
    gs_connect = GoogleSheet(spreadsheet=test_spreadsheet, sheet=test_sheet, creds_json=test_creds_json)
    change_data_result = gs_connect.get_edit_data()
    valid_data_result = validate_data(change_data_result)
    if len(valid_data_result) > 0:
        size_edit_data = []  # данные  с артикулами на изменение габаритов
        price_discount_data = []  # данные с артикулами на изменение цены и/или цены

        for nm_id, data in valid_data_result.items():

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

        wb_api_price_and_discount = ListOfGoodsPricesAndDiscounts(token=token)
        wb_api_content = ListOfCardsContent(token=token)
        """запрос на изменение цены и/или скидки по артикулу"""
        if len(price_discount_data)>0:
            wb_api_price_and_discount.add_new_price_and_discount(price_discount_data)

        """Запрос на изменение габаритов товара по артикулу и vendorCode(артикул продавца)"""
        if len(size_edit_data)> 0:
            wb_api_content.size_edit(size_edit_data)

        """Перезаписываем данные в таблице после их изменений на WB"""
        # todo исправить избыточное обращение к АПИ, если по данным не было изменений

        nm_ids_result = [int(nm_ids_str) for nm_ids_str in valid_data_result.keys()]

        card_from_nm_ids_filter = wb_api_content.get_list_of_cards(nm_ids_list=nm_ids_result, limit=100)
        goods_nm_ids = wb_api_price_and_discount.get_log_for_nm_ids(filter_nm_ids=nm_ids_result)

        merge_json_data = merge_dicts(goods_nm_ids, card_from_nm_ids_filter)

        subject_names = set()  # итог всех полученных с карточек предметов
        commission_traffics = CommissionTariffs(token=token)
        current_tariffs_data = commission_traffics.get_tariffs_box_from_marketplace()

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
        subject_commissions = commission_traffics.get_commission_on_subject(subject_names=subject_names)

        for sc in subject_commissions.items():
            for result_card in merge_json_data.values():
                if sc[0] == result_card["Предмет"]:
                    result_card['Комиссия WB'] = sc[1]

        gs_connect.update_rows(data_json=merge_json_data)
