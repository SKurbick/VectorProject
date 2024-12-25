import datetime


class OrdersByFederalDistrict:
    def __init__(self, db):
        self.db = db

    # async def add_orders_data(self, records_data):
    #     await self.db.copy_records_to_table(
    #         "orders_by_federal_district",
    #         columns=['article_id', 'date', 'federal_district', 'orders_count', 'barcode', 'vendor_code'],
    #         records=records_data
    #     )

    async def add_orders_data(self, records_data):
        # Имена столбцов
        columns = ['article_id', 'date', 'federal_district', 'orders_count', 'barcode', 'vendor_code']

        # Создаем временную таблицу
        create_temp_table_query = '''
        CREATE TEMPORARY TABLE temp_orders AS
        SELECT article_id, date, federal_district, orders_count, barcode, vendor_code
        FROM orders_by_federal_district
        WHERE FALSE
        '''
        await self.db.execute(create_temp_table_query)

        # Загружаем данные во временную таблицу
        await self.db.copy_records_to_table(
            "temp_orders",
            columns=columns,
            records=records_data
        )

        # Вставляем записи, только если article_id существует в article
        insert_query = '''
            INSERT INTO orders_by_federal_district (article_id, date, federal_district, orders_count, barcode, vendor_code)
            SELECT article_id, date, federal_district, orders_count, barcode, vendor_code
            FROM temp_orders
            WHERE EXISTS (
                SELECT 1
                FROM article
                WHERE article.nm_id = temp_orders.article_id
            )
            ON CONFLICT (article_id, date, federal_district) DO UPDATE
            SET
                orders_count = EXCLUDED.orders_count;
        '''
        await self.db.execute(insert_query)

        # Удаляем временную таблицу
        drop_temp_table_query = '''
            DROP TABLE temp_orders
        '''
        await self.db.execute(drop_temp_table_query)

    async def get_awg_orders_per_days(self, start_day, end_day):
        query = """
        SELECT
            article_id,
            federal_district,
            ROUND(AVG(orders_count), 2) AS avg_opd
        FROM
            orders_by_federal_district obfd 
        WHERE
            date BETWEEN $1 AND $2
        GROUP BY
            article_id,
            federal_district;
                """

        result = await self.db.fetch(query, start_day, end_day)
        return result
