import html
import pytz
import asyncio
import datetime
import contextlib
from notification import telegram
from logger import app_logger as logger, log_job

from database.postgresql.database import Database1

from apscheduler.events import EVENT_JOB_ERROR
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from service.service_update_db import Service
from service.service_edit_columns import check_new_nm_ids, check_edits_columns, gs_service_for_schedule_connection


scheduler = AsyncIOScheduler(job_defaults={'misfire_grace_time': 1000, 'max_instances': 1})


@scheduler.scheduled_job(IntervalTrigger(minutes=6), coalesce=True)
@log_job
async def get_actually_revenues_orders_and_net_profit_data():
    """Актуализация данных по выручке, заказам и сумме с чистой прибыли"""
    logger.info("Запуск : Актуализация данных по выручке, заказам и сумме с чистой прибыли")
    gs_service = gs_service_for_schedule_connection()
    await gs_service.get_actually_revenues_orders_and_net_profit_data()
    logger.info("Завершение : Актуализация данных по выручке, заказам и сумме с чистой прибыли")


@scheduler.scheduled_job(IntervalTrigger(seconds=300), coalesce=True)
@log_job
async def job_check_new_nm_ids():
    """Смотрит в таблицу, оценивает новые nm_ids"""
    logger.info("Запуск : Смотрит в таблицу, оценивает новые nm_ids")
    await check_new_nm_ids()
    logger.info("Завершение : Смотрит в таблицу, оценивает новые nm_ids")


@scheduler.scheduled_job(IntervalTrigger(minutes=6), coalesce=True)
@log_job
async def job_check_edits_columns_and_add_actually_data_to_table():
    logger.info("Запуск :"
                "Актуализация информации по ценам, скидкам, габаритам, комиссии, логистики от склада WB до ПВЗ")
    gs_service = gs_service_for_schedule_connection()
    service = Service()
    async with Database1() as db:
        await gs_service.add_actually_data_to_table(db=db)
        logger.info("Завершение :"
                    "Актуализация информации по ценам, скидкам, габаритам, комиссии, логистики от склада WB до ПВЗ")
        logger.info("Запуск : Смотрит в таблицу, оценивает изменения")
        result = await check_edits_columns(db=db)
        if result:
            logger.info("Завершение : Внесение изменений в таблицу")
            await service.actualize_card_data_in_db(result)


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
    logger.info("Запуск приложений")
    scheduler.add_listener(job_error_listener, EVENT_JOB_ERROR)
    scheduler.start()
    with contextlib.suppress(KeyboardInterrupt, SystemExit):
        await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
