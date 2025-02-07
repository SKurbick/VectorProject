from datetime import datetime
from pprint import pprint


class OrdersRevenuesTable:
    def __init__(self, db):
        self.db = db

    async def get_data_by_date(self, date):
        query = """
        SELECT article_id, orders_count, orders_sum_rub from orders_revenues
        WHERE date = $1
        """
        query_result =await self.db.fetch(query, date)
        return query_result
    async def update_data(self, data):
        async with self.db.transaction():
            # Создаем подготовленный запрос для вставки данных с условием ON CONFLICT
            query = """
            INSERT INTO orders_revenues (
                buyouts_count, date, orders_count, open_card_count, orders_sum_rub,
                article_id, add_to_cart_count, cancel_sum_rub, cancel_count, buyouts_sum_rub
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (date, article_id) DO UPDATE SET
                buyouts_count = EXCLUDED.buyouts_count,
                orders_count = EXCLUDED.orders_count,
                open_card_count = EXCLUDED.open_card_count,
                orders_sum_rub = EXCLUDED.orders_sum_rub,
                add_to_cart_count = EXCLUDED.add_to_cart_count,
                cancel_sum_rub = EXCLUDED.cancel_sum_rub,
                cancel_count = EXCLUDED.cancel_count,
                buyouts_sum_rub = EXCLUDED.buyouts_sum_rub
            """
            dict_in_list_data = list(data.values())
            # Вставляем данные пакетами
            batch_size = 1000  # Размер пакета
            for i in range(0, len(dict_in_list_data), batch_size):
                batch = dict_in_list_data[i:i + batch_size]
                await self.db.executemany(query, [
                    (
                        item['buyouts_count'], datetime.strptime(item['date'], '%Y-%m-%d').date(), item['orders_count'],
                        item['open_card_count'],
                        item['orders_sum_rub'], item['article_id'], item['add_to_cart_count'],
                        item['cancel_sum_rub'], item['cancel_count'], item['buyouts_sum_rub']
                    )
                    for item in batch
                ])

    async def get_distinct_dates(self):
        query = """
        SELECT DISTINCT date FROM orders_revenues 
        ORDER BY date DESC; """

        result = await self.db.fetch(query=query)
        return result
