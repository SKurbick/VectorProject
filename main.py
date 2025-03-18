import asyncio
import datetime
import contextlib

import pytz

from database.postgresql.database import Database1
from settings import settings
from logger import app_logger as logger, log_job
from service.gs_service import ServiceGoogleSheet
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from APIGoogleSheet.googlesheet import GoogleSheet, GoogleSheetServiceRevenue, PCGoogleSheet

scheduler = AsyncIOScheduler(job_defaults={'misfire_grace_time': 1000, 'max_instances': 1})

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


# Если значения False, то функция будет прервана
retry_to_check_new_nm_ids = True
retry_to_check_edit_columns = True


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


async def check_edits_columns():
    logger.info("смотрим retry_to_check_edit_columns")
    service_google_sheet = ServiceGoogleSheet.check_status()
    if service_google_sheet['ВКЛ - 1 /ВЫКЛ - 0']:
        try:
            gs_connect = gs_connection()
            if service_google_sheet["Остаток"] or service_google_sheet["Цены/Скидки"] or service_google_sheet["Габариты"]:
                logger.info("СЕРВИС РЕДАКТИРОВАНИЯ АКТИВЕН. Оцениваем ячейки по изменениям товара")
                async with Database1() as db:
                    edit_data_from_table = await gs_connect.get_edit_data(db, service_google_sheet)
                    if len(edit_data_from_table) > 0:
                        service_gs_table = ServiceGoogleSheet(
                            token=None, sheet=sheet, spreadsheet=spreadsheet, creds_json=creds_json)

                        edit_nm_ids_data = await service_gs_table.change_cards_and_tables_data(db=db,
                            edit_data_from_table=edit_data_from_table)
                        if len(edit_nm_ids_data) > 0:
                            gs_connect.update_rows(data_json=edit_nm_ids_data,
                                                   edit_column_clean={
                                                       "price_discount": service_google_sheet['Цены/Скидки'],
                                                       "dimensions": service_google_sheet['Габариты'],
                                                       "qty": service_google_sheet["Остаток"]})

            else:
                logger.info("Сервис заблокирован на изменения: (Цены/Скидки, Остаток, Габариты)")
        except Exception as e:
            logger.info(f"[ERROR] СЕРВИС РЕДАКТИРОВАНИЯ {e}")
            raise e


@scheduler.scheduled_job(IntervalTrigger(minutes=6), coalesce=True)
@log_job
async def get_actually_revenues_orders_and_net_profit_data():
    """Актуализация данных по выручке, заказам и сумме с чистой прибыли"""
    logger.info("Запуск : Актуализация данных по выручке, заказам и сумме с чистой прибыли")
    gs_service = gs_service_for_schedule_connection()
    await gs_service.get_actually_revenues_orders_and_net_profit_data()
    logger.info("Завершение : Актуализация данных по выручке, заказам и сумме с чистой прибыли")


@scheduler.scheduled_job(IntervalTrigger(seconds=800), coalesce=True)
@log_job
async def add_actually_data_to_table():
    """Актуализация информации по ценам, скидкам, габаритам, комиссии, логистики от склада WB до ПВЗ"""
    logger.info(
        "Запуск : Актуализация информации по ценам, скидкам, габаритам, комиссии, логистики от склада WB до ПВЗ")
    gs_service = gs_service_for_schedule_connection()
    await gs_service.add_actually_data_to_table()
    logger.info(
        "Завершение : Актуализация информации по ценам, скидкам, габаритам, комиссии, логистики от склада WB до ПВЗ")


@scheduler.scheduled_job(IntervalTrigger(seconds=300), coalesce=True)
@log_job
async def job_check_new_nm_ids():
    """Смотрит в таблицу, оценивает новые nm_ids"""
    logger.info("Запуск : Смотрит в таблицу, оценивает новые nm_ids")
    await check_new_nm_ids()
    logger.info("Завершение : Смотрит в таблицу, оценивает новые nm_ids")


@scheduler.scheduled_job(IntervalTrigger(seconds=250), coalesce=True)
@log_job
async def job_check_edits_columns():
    """Смотрит в таблицу, оценивает изменения"""
    logger.info("Запуск : Смотрит в таблицу, оценивает изменения")
    await check_edits_columns()
    logger.info("Завершение : Смотрит в таблицу, оценивает изменения")


@scheduler.scheduled_job(IntervalTrigger(minutes=20), coalesce=True)
@log_job
async def check_quantity_flag():
    """Проверяет остатки, обновляет через Сопост"""
    logger.info("Запуск : проверяет остатки, обновляет через Сопост")
    gs_service = gs_service_for_schedule_connection()
    await gs_service.check_quantity_flag()
    logger.info("Завершение : проверяет остатки, обновляет через Сопост")


@scheduler.scheduled_job(CronTrigger(hour=9, minute=30, timezone=pytz.timezone('Europe/Moscow')), coalesce=True)
@log_job
async def actualize_avg_orders_data_in_table():
    """Выгрузка данных по обороту"""
    logger.info("Запуск : Выгрузка данных по обороту")
    gs_service = gs_service_for_schedule_connection()
    await gs_service.actualize_avg_orders_data_in_table()
    logger.info("Завершение : Выгрузка данных по обороту")


@scheduler.scheduled_job(CronTrigger(hour=1, minute=55, timezone=pytz.timezone('Europe/Moscow')))
@log_job
async def turnover_of_goods():
    """Актуализация данных по обороту в БД"""
    logger.info("Запуск : Актуализация данных по обороту в БД")
    gs_service = gs_service_for_schedule_connection()
    await gs_service.turnover_of_goods()
    logger.info("Завершение :  Актуализация данных по обороту в БД")


@scheduler.scheduled_job(IntervalTrigger(minutes=30), coalesce=True)
@log_job
async def check_headers():
    """Смотрим состояние заголовков текущих дней"""
    logger.info("Запуск : Смотрим состояние заголовков текущих дней")
    gs_service = gs_service_for_schedule_connection()
    await gs_service.check_headers()
    logger.info("Завершение : Смотрим состояние заголовков текущих дней")


@scheduler.scheduled_job(IntervalTrigger(minutes=5), coalesce=True)
@log_job
async def get_actually_data_by_qty():
    """Актуализация остатков по регионам в таблице MAIN"""
    logger.info("Запуск : актуализация остатков по регионам в таблице MAIN")
    gs_service = gs_service_for_schedule_connection()
    await gs_service.get_actually_data_by_qty()
    logger.info("Завершение : актуализация остатков по регионам в таблице MAIN")


async def main():
    logger.info("Запуск приложений")
    scheduler.start()
    with contextlib.suppress(KeyboardInterrupt, SystemExit):
        await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
