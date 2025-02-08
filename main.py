import asyncio
import datetime
from settings import settings
from APIGoogleSheet.googlesheet import GoogleSheet, GoogleSheetServiceRevenue, PCGoogleSheet
from service.gs_service import ServiceGoogleSheet
import schedule

creds_json = settings.CREEDS_FILE_NAME
spreadsheet = settings.SPREADSHEET
sheet = settings.SHEET
print(settings.SHEET)
print(settings.SPREADSHEET)
print("time to start:", datetime.datetime.today().time().strftime("%H:%M:%S"))


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
        print("смотрим retry_to_check_new_nm_ids", retry_to_check_new_nm_ids)
        if retry_to_check_new_nm_ids:
            print("Сервис АКТИВЕН. Смотрим в таблицу.")
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
                        print("Добавляем выручку в таблицу")
                        """Добавление информации по выручкам за последние 7 дней"""
                        gs_service_revenue_connection().update_revenue_rows(
                            data_json=revenue_data_for_update_rows)

                except Exception as e:
                    print(f"Ошибка при выполнении асинхронной функции: {e}")
                    return

                finally:
                    retry_to_check_new_nm_ids = True

            print("Упали в ожидание")

    else:
        print("СЕРВИС ОТКЛЮЧЕН (0)")


async def check_edits_columns():
    global retry_to_check_edit_columns
    print("смотрим retry_to_check_edit_columns", retry_to_check_new_nm_ids)
    statuses = ServiceGoogleSheet.check_status()
    if statuses['ВКЛ - 1 /ВЫКЛ - 0'] and retry_to_check_edit_columns:
        try:
            gs_connect = gs_connection()
            if statuses["Остаток"] or statuses["Цены/Скидки"] or statuses["Габариты"]:
                retry_to_check_edit_columns = False
                print("СЕРВИС РЕДАКТИРОВАНИЯ АКТИВЕН. Оцениваем ячейки по изменениям товара")
                edit_statuses = ServiceGoogleSheet.check_status()

                edit_data_from_table = gs_connect.get_edit_data(dimension_status=edit_statuses["Габариты"],
                                                                price_and_discount_status=edit_statuses["Цены/Скидки"],
                                                                qty_status=edit_statuses["Остаток"])
                if len(edit_data_from_table) > 0:
                    service_gs_table = ServiceGoogleSheet(
                        token=None, sheet=sheet, spreadsheet=spreadsheet, creds_json=creds_json)

                    edit_nm_ids_data = await service_gs_table.change_cards_and_tables_data(
                        edit_data_from_table=edit_data_from_table)
                    if len(edit_nm_ids_data) > 0:
                        gs_connection().update_rows(data_json=edit_nm_ids_data,
                                                    edit_column_clean={"price_discount": statuses['Цены/Скидки'],
                                                                       "dimensions": statuses['Габариты'],
                                                                       "qty": statuses["Остаток"]})

            else:
                print("Сервис заблокирован на изменения: (Цены/Скидки, Остаток, Габариты)")
        except Exception as e:
            retry_to_check_edit_columns = True
            print("[ERROR] СЕРВИС РЕДАКТИРОВАНИЯ ", e)
            raise e
        finally:
            retry_to_check_edit_columns = True


# async def run_in_executor(func, *args):
#     loop = asyncio.get_event_loop()
#     return await loop.run_in_executor(None, func, *args)


def schedule_tasks():
    gs_service = gs_service_for_schedule_connection()

    "Добавляет выручку и сдвигает столбцы с выручкой по необходимости. Условие должно работать  каждые 25 мин"
    # schedule.every(25).minutes.do(lambda: asyncio.create_task(gs_service.add_new_day_revenue_to_table()))

    """Актуализация информации по ценам, скидкам, габаритам, комиссии, логистики от склада WB до ПВЗ"""
    schedule.every(800).seconds.do(lambda: asyncio.create_task(gs_service.add_actually_data_to_table()))

    """Смотрит в таблицу, оценивает новые nm_ids"""
    schedule.every(300).seconds.do(lambda: asyncio.create_task(check_new_nm_ids()))

    """Смотрит в таблицу, оценивает изменения"""
    schedule.every(250).seconds.do(lambda: asyncio.create_task(check_edits_columns()))

    """проверяет остатки, обновляет через Сопост"""
    schedule.every(20).minutes.do(lambda: asyncio.create_task(gs_service.check_quantity_flag()))

    """актуализация остатков по регионам в таблице MAIN"""
    schedule.every(5).minutes.do(lambda: asyncio.create_task(gs_service.get_actually_data_by_qty()))

    """актуализация заказов в"""
    schedule.every().day.at("00:01:55").do(lambda: asyncio.create_task(gs_service.turnover_of_goods()))
    schedule.every().day.at("09:30:30").do(lambda: asyncio.create_task(gs_service.actualize_avg_orders_data_in_table()))
    """Актуализация данных по выручке, заказам и сумме с чистой прибыли"""
    schedule.every(6).minutes.do(lambda: asyncio.create_task(gs_service.get_actually_revenues_orders_and_net_profit_data()))
    """Смотрим состояние заголовков текущих дней"""
    schedule.every(30).minutes.do(lambda: asyncio.create_task(gs_service.check_headers()))


async def run_scheduler():
    while True:
        schedule.run_pending()
        await asyncio.sleep(1)


async def main():
    schedule_tasks()
    await run_scheduler()


# from apscheduler.schedulers.asyncio import AsyncIOScheduler
#
#
# async def main():
#     gs_service = gs_service_for_schedule_connection()
#
#     # Create an instance of the scheduler
#     scheduler = AsyncIOScheduler()
#
#     # Add jobs with the appropriate triggers and intervals
#     scheduler.add_job(gs_service.add_new_day_revenue_to_table, 'interval', minutes=25)
#     scheduler.add_job(gs_service.add_actually_data_to_table, 'interval', seconds=800)
#     scheduler.add_job(check_new_nm_ids, 'interval', seconds=300)
#     scheduler.add_job(check_edits_columns, 'interval', seconds=250)
#     scheduler.add_job(gs_service.check_quantity_flag, 'interval', minutes=20)
#     # Uncomment the following line to add the daily job
#     # scheduler.add_job(update_purchase_calculation_data, 'cron', hour=9, minute=30)
#
#     # Start the scheduler
#     scheduler.start()
#
#     # Keep the event loop running indefinitely
#     event = asyncio.Event()
#     await event.wait()


if __name__ == "__main__":
    asyncio.run(main())
