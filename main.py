import datetime
import time
from pprint import pprint

from settings import get_wb_tokens
from APIGoogleSheet.googlesheet import GoogleSheet
from utils import validate_data, add_nm_ids_in_db
from service.gs_service import ServiceGoogleSheet
import schedule

test_creds_json = "creds.json"
test_spreadsheet = "START Курбан"
test_sheet = "Тестовая версия"


def main():
    print("Смотрим в таблицу. Оцениваем добавление новых артикулов")
    gs_connect = GoogleSheet(creds_json=test_creds_json,
                             spreadsheet=test_spreadsheet, sheet=test_sheet)

    # получение словаря с ключом ЛК и его Артикулами
    lk_articles = gs_connect.create_lk_articles_list()
    if len(lk_articles) > 0:
        for account, articles in lk_articles.items():
            print(account)
            # получаем токен и корректируем регистр для чтения из файла
            token = get_wb_tokens()[account.capitalize()]
            service_gs_table = ServiceGoogleSheet(
                token=token, sheet=test_sheet, spreadsheet=test_spreadsheet, creds_json=test_creds_json)
            # поиск всех "Артикул" которых нет в бд
            nm_ids_result = gs_connect.check_new_nm_ids(account=account, nm_ids=articles)
            if len(nm_ids_result) > 0:
                print("В таблице найдены новые артикулы которых нет в БД")
                """Если есть новые артикулы в таблице - будут добавлены данные по ним"""
                service_gs_table.add_new_data_from_table(nm_ids=nm_ids_result)

                """Добавление информации по выручкам за последние 7 дней"""
                service_gs_table.add_revenue_for_new_nm_ids(nm_ids=nm_ids_result)

                """добавляем артикулы в БД"""
                add_nm_ids_in_db(account=account, new_nm_ids=nm_ids_result)

        """Получение данных с запросом на изменение с таблицы.
        Условие сработает если данные с запросом на изменение с таблицы будут валидны и артикулы с таблицы будут в БД"""
        edit_data_from_table = gs_connect.get_edit_data()
        print("Смотрим в таблицу. Оцениваем ячейки по изменениям товара")
        if len(edit_data_from_table) > 0:
            print("Получил данные по ячейкам на изменение товара")
            for account, nm_ids_data in edit_data_from_table.items():
                pprint(nm_ids_data)
                valid_data_result = validate_data(nm_ids_data)
                # пройдет если данные будут валидны для изменения
                if len(valid_data_result) > 0:
                    print("Данные валидны")
                    token = get_wb_tokens()[account.capitalize()]
                    service_gs_table = ServiceGoogleSheet(
                        token=token, sheet=test_sheet, spreadsheet=test_spreadsheet, creds_json=test_creds_json)

                    service_gs_table.change_cards_and_tables_data(valid_data=valid_data_result)
    print("Упали в ожидание")

gs_service_for_schedule = ServiceGoogleSheet(
    token=None, sheet=test_sheet, spreadsheet=test_spreadsheet, creds_json=test_creds_json)
"""Актуализация информации по ценам, скидкам, габаритам, комиссии, логистики от склада WB до ПВЗ"""
schedule.every(300).seconds.do(gs_service_for_schedule.add_actually_data_to_table)

"""Смотрит в таблицу, оценивает изменения"""
schedule.every(30).seconds.do(main)

"""Сдвигает таблицы по выручам. Условие должно работать раз в день каждые 5 утра"""
schedule.every().day.at("05:00").do(gs_service_for_schedule.add_new_day_revenue_to_table)

if __name__ == '__main__':
    print("СЕРВИС ЗАПУЩЕН")
    while True:
        schedule.run_pending()