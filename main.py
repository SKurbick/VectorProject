import html
import time
from pprint import pprint

import pytz
import asyncio
import datetime
import contextlib
from notification import telegram

from database.postgresql.database import Database1

from apscheduler.events import EVENT_JOB_ERROR
from settings import settings
from logger import app_logger as logger, log_job
from service.gs_service import ServiceGoogleSheet
from service.wms_stocks_google_sheet import WMSStocksGoogleSheetService
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from service.service_edit_columns import check_new_nm_ids, check_edits_columns, gs_service_for_schedule_connection
from service.service_update_db import Service

scheduler = AsyncIOScheduler(job_defaults={'misfire_grace_time': 2000, 'max_instances': 1})

def gs_service_new_good_table_connection(sheet, spreadsheet, creds_json):
    return ServiceGoogleSheet(
        token=None, sheet=sheet, spreadsheet=spreadsheet, creds_json=creds_json)


@scheduler.scheduled_job(
    IntervalTrigger(minutes=settings.WMS_STOCKS_SYNC_INTERVAL_MINUTES),
    coalesce=True,
)
@log_job
async def sync_wms_stocks_to_google_sheet():
    """Актуализировать остатки WMS и резервы СЗ в Google Sheets."""
    logger.info("Запуск: актуализация остатков WMS в Google Sheets")
    service = WMSStocksGoogleSheetService(
        wms_url=settings.WMS_BASE_URL,
        spreadsheet=settings.WMS_STOCKS_SPREADSHEET,
        worksheet=settings.WMS_STOCKS_WORKSHEET,
        credentials=settings.CREEDS_FILE_NAME,
    )
    destinations = [
        (settings.WMS_STOCKS_SPREADSHEET, settings.WMS_STOCKS_WORKSHEET),
        ("UNIT 2.0 (tested)", "WMS остатки"),
    ]
    results = await service.sync_many(destinations)
    for (spreadsheet, worksheet), result in zip(destinations, results):
        logger.info(
            "Остатки WMS актуализированы: таблица={!r}, лист={!r}; "
            "зон={}, товаров={}, с резервами={}, обновлено={}, добавлено={}",
            spreadsheet,
            worksheet,
            result.zones,
            result.matrix_products,
            result.reserved_products,
            result.updated_products,
            result.added_products,
        )
    logger.info("Завершение: актуализация остатков WMS в Google Sheets")



@scheduler.scheduled_job(IntervalTrigger(minutes=15), coalesce=True)
@log_job
async def add_stock_data_in_another_good_gs():
    logger.info("Запуск : актуализация СЕРВИСНЫХ остатков")
    gs_service = gs_service_new_good_table_connection(sheet="Остатки по складам", spreadsheet="Новый товар", creds_json="creds.json")
    await gs_service.get_stock_data_on_api(warehouse_ids=[2,4,5,6,7,8], add_missing=True)
    gs_service = gs_service_new_good_table_connection(sheet="Остатки по складам", spreadsheet="Годовой план закупа 2026", creds_json="creds.json")
    await gs_service.get_stock_data_on_api(warehouse_ids=[2,4,5,6,7,8], add_missing=True)


@scheduler.scheduled_job(IntervalTrigger(minutes=15), coalesce=True)
@log_job
async def get_actually_revenues_orders_and_net_profit_data():
    """Актуализация данных по выручке, заказам и сумме с чистой прибыли"""
    logger.info("Запуск : Актуализация данных по выручке, заказам и сумме с чистой прибыли")
    gs_service = gs_service_for_schedule_connection()
    await gs_service.get_actually_revenues_orders_and_net_profit_data()
    logger.info("Завершение : Актуализация данных по выручке, заказам и сумме с чистой прибыли")



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


@scheduler.scheduled_job(IntervalTrigger(minutes=15), coalesce=True)
@log_job
async def get_actually_data_by_qty():
    """Актуализация остатков по регионам в таблице MAIN"""
    logger.info("Запуск : актуализация остатков по регионам в таблице MAIN")
    gs_service = gs_service_for_schedule_connection()
    await gs_service.get_actually_data_by_qty_from_db()
    logger.info("Завершение : актуализация остатков по регионам в таблице MAIN")


@scheduler.scheduled_job(IntervalTrigger(minutes=15), coalesce=True)
@log_job
async def add_stock_data_in_gs():
    logger.info("Запуск : актуализация СЕРВИСНЫХ остатков")
    gs_service = gs_service_new_good_table_connection(
        sheet="остатки старой версии",
        spreadsheet=settings.SPREADSHEET,
        creds_json=settings.CREEDS_FILE_NAME,
    )
    await gs_service.get_stock_data_on_api(add_missing=True)



def job_error_listener(event):
    job = scheduler.get_job(event.job_id)
    if job:
        job_name = job.name if getattr(job, "name", None) else job.func.__name__
    else:
        job_name = event.job_id

    error_message = (
        f"VectorProject: <b><u>{html.escape(str(job_name.upper()))}</u></b>  "
        f"{datetime.datetime.now().strftime('%Y-%m-%d at %H:%M:%S')}  ERROR  "
        f"{html.escape(str(job_name))} - Exception: {html.escape(str(event.exception))}\n"
        f"Traceback:<blockquote expandable>{html.escape(str(event.traceback))}</blockquote>"
    )
    asyncio.create_task(telegram(error_message))


async def main():
    logger.info("Запуск приложения 🦋")
    scheduler.add_listener(job_error_listener, EVENT_JOB_ERROR)
    scheduler.start()
    with contextlib.suppress(KeyboardInterrupt, SystemExit):
        await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
    # asyncio.run(check_quantity_flag())
    # asyncio.run(get_actually_data_by_qty())
    # asyncio.run(job_check_edits_columns_and_add_actually_data_to_table())
    # asyncio.run(job_check_new_nm_ids())
    # asyncio.run(get_actually_revenues_orders_and_net_profit_data())
    # asyncio.run(add_stock_data_in_gs())
