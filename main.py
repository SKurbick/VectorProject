import datetime
import time
from pprint import pprint

from settings import settings
from APIGoogleSheet.googlesheet import GoogleSheet, GoogleSheetServiceRevenue
from service.gs_service import ServiceGoogleSheet
import schedule

creds_json = settings.CREEDS_FILE_NAME
spreadsheet = settings.SPREADSHEET
sheet = settings.SHEET
print(settings.SHEET)
print(settings.SPREADSHEET)


def gs_connection():
    return GoogleSheet(creds_json=creds_json,
                       spreadsheet=spreadsheet, sheet=sheet)


def gs_service_for_schedule_connection():
    return ServiceGoogleSheet(
        token=None, sheet=sheet, spreadsheet=spreadsheet, creds_json=creds_json)


def gs_service_revenue_connection():
    return GoogleSheetServiceRevenue(sheet=sheet, spreadsheet=spreadsheet, creds_json=creds_json)


def check_new_nm_ids():
    statuses = ServiceGoogleSheet.check_status()
    # если сервис включен (1), то пройдет проверка
    if statuses['ВКЛ - 1 /ВЫКЛ - 0']:
        print("Сервис АКТИВЕН. Смотрим в таблицу.")
        gs_connect = gs_connection()

        # получение словаря с ключом ЛК и его Артикулами
        lk_articles = gs_connect.create_lk_articles_list()
        if len(lk_articles) > 0:
            service_gs_table = ServiceGoogleSheet(
                token=None, sheet=sheet, spreadsheet=spreadsheet, creds_json=creds_json)

            result_data_for_update_rows = service_gs_table.add_new_data_from_table(lk_articles=lk_articles,
                                                                                   add_data_in_db=False)
            revenue_data_for_update_rows = service_gs_table.add_revenue_for_new_nm_ids(lk_articles=lk_articles)

            "Добавляем данные по выручке и инфо по полученным артикулам"
            if len(result_data_for_update_rows) > 0:
                gs_connection().update_rows(data_json=result_data_for_update_rows, edit_column_clean=None)
            if len(revenue_data_for_update_rows) > 0:
                print("Добавляем выручку в таблицу")
                """Добавление информации по выручкам за последние 7 дней"""
                gs_service_revenue_connection().update_revenue_rows(
                    data_json=revenue_data_for_update_rows)

        print("Упали в ожидание")
    else:
        print("СЕРВИС ОТКЛЮЧЕН (0)")


def check_edits_columns():
    statuses = ServiceGoogleSheet.check_status()
    if statuses['ВКЛ - 1 /ВЫКЛ - 0']:
        print("СЕРВИС АКТИВЕН. Смотрим в таблицу. Оцениваем ячейки по изменениям товара")
        gs_connect = gs_connection()
        if statuses["Остаток"] or statuses["Цены/Скидки"] or statuses["Габариты"]:
            edit_statuses = ServiceGoogleSheet.check_status()

            edit_data_from_table = gs_connect.get_edit_data(dimension_status=edit_statuses["Габариты"],
                                                            price_and_discount_status=edit_statuses["Цены/Скидки"],
                                                            qty_status=edit_statuses["Остаток"])
            if len(edit_data_from_table) > 0:
                service_gs_table = ServiceGoogleSheet(
                    token=None, sheet=sheet, spreadsheet=spreadsheet, creds_json=creds_json)

                edit_nm_ids_data = service_gs_table.change_cards_and_tables_data(
                    edit_data_from_table=edit_data_from_table)
                if len(edit_nm_ids_data) > 0:
                    gs_connection().update_rows(data_json=edit_nm_ids_data,
                                                edit_column_clean={"price_discount": statuses['Цены/Скидки'],
                                                                   "dimensions": statuses['Габариты'],
                                                                   "qty": statuses["Остаток"]})

        else:
            print("Сервис заблокирован на изменения: (Цены/Скидки, Остаток, Габариты)")

# """Актуализация информации по ценам, скидкам, габаритам, комиссии, логистики от склада WB до ПВЗ"""
schedule.every(300).seconds.do(gs_service_for_schedule_connection().add_actually_data_to_table)
#
# """Смотрит в таблицу, оценивает новые nm_ids"""
schedule.every(15).seconds.do(check_new_nm_ids)

# """Смотрит в таблицу, оценивает изменения"""
schedule.every(180).seconds.do(check_edits_columns)


# """Сдвигает таблицы по выручкам. Условие должно работать раз в день каждые 5 утра"""
schedule.every().day.at("09:20").do(gs_service_for_schedule_connection().add_new_day_revenue_to_table)
# проверяет остатки
schedule.every(1).hours.do(gs_service_for_schedule_connection().check_quantity_flag)

if __name__ == '__main__':
    print("СЕРВИС ЗАПУЩЕН")
    while True:
        schedule.run_pending()
        time.sleep(2)
