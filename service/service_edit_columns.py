import datetime
from typing import Dict, Any, Set

from APIGoogleSheet.googlesheet import GoogleSheet, GoogleSheetServiceRevenue
from database.postgresql.database import Database1
from database.postgresql.repositories.article import ArticleTable
from service.gs_service import ServiceGoogleSheet
from logger import app_logger as logger
from settings import settings

creds_json = settings.CREEDS_FILE_NAME
spreadsheet = settings.SPREADSHEET
sheet = settings.SHEET
logger.info(settings.SHEET)
logger.info(settings.SPREADSHEET)
logger.info("time to start:", datetime.datetime.now().time().strftime("%H:%M:%S"))


def gs_connection():
    return GoogleSheet(creds_json=creds_json,
                       spreadsheet=spreadsheet, sheet=sheet)


def gs_service_for_schedule_connection():
    return ServiceGoogleSheet(
        token=None, sheet=sheet, spreadsheet=spreadsheet, creds_json=creds_json)


def gs_service_revenue_connection():
    return GoogleSheetServiceRevenue(sheet=sheet, spreadsheet=spreadsheet, creds_json=creds_json)


retry_to_check_new_nm_ids = True


def create_lk_articles(edit_nm_ids_data: Dict[str, Any]) -> dict[Any, set[str]]:
    result = {}
    for k, v in edit_nm_ids_data.items():
        if v["account"] not in result:
            result[v["account"]] = {k}
        else:
            result[v["account"]].add(k)
    return result


async def check_new_nm_ids():
    global retry_to_check_new_nm_ids
    statuses = ServiceGoogleSheet.check_status()
    # если сервис включен (1), то пройдет проверка
    if statuses['ВКЛ - 1 /ВЫКЛ - 0']:
        logger.info("смотрим retry_to_check_new_nm_ids", retry_to_check_new_nm_ids)
        if retry_to_check_new_nm_ids:
            logger.info("Сервис АКТИВЕН. Смотрим в таблицу.")
            gs_connect = gs_connection()

            # получение словаря с ключом ЛК и его Артикулами
            lk_articles = gs_connect.create_lk_articles_list()
            if len(lk_articles) > 0:
                service_gs_table = ServiceGoogleSheet(
                    token=None, sheet=sheet, spreadsheet=spreadsheet, creds_json=creds_json)
                # retry_to_check_new_nm_ids = False

                result_data_for_update_rows = await service_gs_table.add_new_data_from_table(lk_articles=lk_articles,
                                                                                             add_data_in_db=False)
                if len(result_data_for_update_rows) > 0:
                    gs_connection().update_rows(data_json=result_data_for_update_rows, edit_column_clean=None)

                    retry_to_check_new_nm_ids = False

                try:
                    revenue_data_for_update_rows = await service_gs_table.add_revenue_for_new_nm_ids(
                        lk_articles=lk_articles)

                    if len(revenue_data_for_update_rows) > 0:
                        logger.info("Добавляем выручку в таблицу")
                        """Добавление информации по выручкам за последние 7 дней"""
                        gs_service_revenue_connection().update_revenue_rows(
                            data_json=revenue_data_for_update_rows)

                except Exception as e:
                    logger.exception(f"Ошибка при выполнении асинхронной функции: {e}")
                    return

                finally:
                    retry_to_check_new_nm_ids = True

            logger.info("Упали в ожидание")

    else:
        logger.info("СЕРВИС ОТКЛЮЧЕН (0)")


async def check_edits_columns(db: Database1):
    service_google_sheet = ServiceGoogleSheet.check_status()
    if service_google_sheet['ВКЛ - 1 /ВЫКЛ - 0']:
        try:
            gs_connect = gs_connection()
            if (service_google_sheet["Остаток"] or service_google_sheet["Цены/Скидки"]
                    or service_google_sheet["Габариты"]):
                logger.info("СЕРВИС РЕДАКТИРОВАНИЯ АКТИВЕН. Оцениваем ячейки по изменениям товара")
                db_nm_ids_data = await ArticleTable(db).get_all_nm_ids()
                edit_data_from_table = await gs_connect.get_edit_data(db_nm_ids_data, service_google_sheet)
                if edit_data_from_table:
                    service_gs_table = ServiceGoogleSheet(
                        token=None, sheet=sheet, spreadsheet=spreadsheet, creds_json=creds_json)

                    edit_nm_ids_data = await service_gs_table.change_cards_and_tables_data(
                        db_nm_ids_data=db_nm_ids_data,
                        edit_data_from_table=edit_data_from_table)
                    if edit_nm_ids_data:
                        gs_connect.update_rows(data_json=edit_nm_ids_data,
                                               edit_column_clean={
                                                   "price_discount": service_google_sheet['Цены/Скидки'],
                                                   "dimensions": service_google_sheet['Габариты'],
                                                   "qty": service_google_sheet["Остаток"]})
                        return create_lk_articles(edit_nm_ids_data)

            else:
                logger.info("Сервис заблокирован на изменения: (Цены/Скидки, Остаток, Габариты)")
        except Exception as e:
            logger.info(f"[ERROR] СЕРВИС РЕДАКТИРОВАНИЯ {e}")
            raise e
