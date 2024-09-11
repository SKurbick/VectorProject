import datetime
import time
from pprint import pprint

from settings import get_wb_tokens
from APIGoogleSheet.googlesheet import GoogleSheet, GoogleSheetServiceRevenue
from utils import validate_data, add_nm_ids_in_db
from service.gs_service import ServiceGoogleSheet
import schedule

test_creds_json = "creds.json"
test_spreadsheet = "START Курбан"
test_sheet = "Тестовая версия"


def gs_connection():
    return GoogleSheet(creds_json=test_creds_json,
                       spreadsheet=test_spreadsheet, sheet=test_sheet)


def gs_service_for_schedule_connection():
    return ServiceGoogleSheet(
        token=None, sheet=test_sheet, spreadsheet=test_spreadsheet, creds_json=test_creds_json)


def gs_service_revenue_connection():
    return GoogleSheetServiceRevenue(sheet=test_sheet, spreadsheet=test_spreadsheet, creds_json=test_creds_json)


def main():
    # если сервис включен (1), то пройдет проверка
    if ServiceGoogleSheet.check_status():
        print("Сервис АКТИВЕН. Смотрим в таблицу.")
        gs_connect = gs_connection()

        # получение словаря с ключом ЛК и его Артикулами
        lk_articles = gs_connect.create_lk_articles_list()
        if len(lk_articles) > 0:
            service_gs_table = ServiceGoogleSheet(
                token=None, sheet=test_sheet, spreadsheet=test_spreadsheet, creds_json=test_creds_json)

            result_data_for_update_rows = service_gs_table.add_new_data_from_table(lk_articles=lk_articles)
            revenue_data_for_update_rows = service_gs_table.add_revenue_for_new_nm_ids(lk_articles=lk_articles)

            "Добавляем данные по выручке и инфо по полученным артикулам"
            if len(result_data_for_update_rows) > 0:
                gs_connection().update_rows(data_json=result_data_for_update_rows)
            if len(revenue_data_for_update_rows) > 0:
                """Добавление информации по выручкам за последние 7 дней"""
                gs_service_revenue_connection().add_for_all_new_nm_id_revenue(
                    nm_ids_revenue_data=revenue_data_for_update_rows)

            """Получение данных с запросом на изменение с таблицы.
            Условие сработает если данные с запросом на изменение с таблицы будут валидны и артикулы с таблицы будут в БД"""
            edit_data_from_table = gs_connect.get_edit_data()
            print("Смотрим в таблицу. Оцениваем ячейки по изменениям товара")
            if len(edit_data_from_table) > 0:
                service_gs_table = ServiceGoogleSheet(
                    token=None, sheet=test_sheet, spreadsheet=test_spreadsheet, creds_json=test_creds_json)

                edit_nm_ids_data = service_gs_table.change_cards_and_tables_data(
                    edit_data_from_table=edit_data_from_table)
                if len(edit_nm_ids_data) > 0:
                    gs_connection().update_rows(data_json=edit_nm_ids_data,
                                                edit_column_clean={"price_discount": True, "dimensions": True})
        print("Упали в ожидание")
    else:
        print("СЕРВИС ОТКЛЮЧЕН (0)")


"""Актуализация информации по ценам, скидкам, габаритам, комиссии, логистики от склада WB до ПВЗ"""
schedule.every(300).seconds.do(gs_service_for_schedule_connection().add_actually_data_to_table)

"""Смотрит в таблицу, оценивает изменения"""
schedule.every(180).seconds.do(main)

"""Сдвигает таблицы по выручам. Условие должно работать раз в день каждые 5 утра"""
schedule.every().day.at("09:40").do(gs_service_for_schedule_connection().add_new_day_revenue_to_table)

if __name__ == '__main__':
    print("СЕРВИС ЗАПУЩЕН")
    while True:
        schedule.run_pending()
        time.sleep(2)
