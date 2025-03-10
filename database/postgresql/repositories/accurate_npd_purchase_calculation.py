import datetime
from logger import app_logger as logger

class AccurateNetProfitPCTable:
    def __init__(self, db):
        self.db = db

    async def get_data_net_profit(self):
        net_profit_query = """
        SELECT DISTINCT article_id
        FROM accurate_npd_purchase_calculation;
        """

        result_data = await self.db.fetch(net_profit_query)
        article_id_list = list(result_data.values())

        return article_id_list

    async def add_new_article_net_profit_data(self, time, data, nm_ids_net_profit, new_nm_ids):
        """Добавления ЧП и заказов по новым артикулам которых нет в таблице"""
        async with self.db.transaction():

            net_profit_data = []
            for nm_id in new_nm_ids:
                try:
                    net_profit = nm_ids_net_profit[nm_id]
                    date = datetime.datetime.strptime(data[nm_id]['dt'], '%Y-%m-%d').date()
                    orders_count = data[nm_id]['ordersCount']
                    net_profit_data.append((nm_id, net_profit, date, time, orders_count))

                except KeyError as e:
                    logger.exception(f"KeyError (add_new_article_net_profit_data) {e}")
            net_profit_query = """
            INSERT INTO accurate_npd_purchase_calculation (article_id, net_profit, date, time, orders)
            VALUES ($1, $2, $3, $4, $5) ;
            """
            await self.db.executemany(net_profit_query, net_profit_data)

    async def update_net_profit_data(self, time, response_data, nm_ids_table_data, date):
        async with self.db.transaction():
            select_query = f"""
                SELECT article_id, SUM(orders) FROM accurate_npd_purchase_calculation 
                WHERE date = '{date}'
                GROUP BY article_id ;
                """

            result = await self.db.fetch(query=select_query)
            db_nm_ids_orders = dict(result)

            subtractions_orders_result = {
                nm_id: response_data[nm_id]["ordersCount"] - db_nm_ids_orders[nm_id] for nm_id in response_data
            }

            # Запрос для добавления или обновления данных
            query = """
            INSERT INTO accurate_npd_purchase_calculation (article_id, net_profit, orders, time, date)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (article_id, date, net_profit) DO UPDATE
            SET orders = accurate_npd_purchase_calculation.orders + EXCLUDED.orders,
                time = EXCLUDED.time;
            """

            # создаем результирующий список с данными для заполнения в бд
            data_for_add_db = [(nm_id, nm_ids_table_data[nm_id], subtractions_orders_result[nm_id], time,
                                datetime.datetime.strptime(date, '%Y-%m-%d').date()) for
                               nm_id, value in response_data.items()]

            # Разбиваем данные на пакеты
            batch_size = 1000
            for i in range(0, len(data_for_add_db), batch_size):
                batch = data_for_add_db[i:i + batch_size]
                await self.db.executemany(query, batch)

    async def check_nm_ids(self, account: [str, None], nm_ids: list, date):
        """Возвращает артикулы которых нет в таблице article"""
        nm_ids_str = ', '.join(f"({nm_id})" for nm_id in nm_ids)
        query = f"""
        SELECT article_id
        FROM (VALUES {nm_ids_str}) AS input(article_id)
        EXCEPT
        SELECT article_id
        FROM accurate_npd_purchase_calculation
        WHERE date = '{date}';
        """
        not_found_nm_ids = await self.db.fetch(query)

        return [result_nm_id["article_id"] for result_nm_id in not_found_nm_ids]

    async def get_net_profit_by_date(self, date):
        date_obj = datetime.datetime.strptime(date, "%Y-%m-%d")

        select_query = f"""
        SELECT article_id, sum(sum_net_profit) as sum_snp FROM accurate_npd_purchase_calculation
        WHERE date = $1
        GROUP BY article_id
        ORDER BY sum_snp desc;
        """

        result = await self.db.fetch(select_query, date_obj)
        return result

    async def get_net_profit_by_latest_dates(self, days: int=30):
        select_query = f"""
        WITH latest_dates AS (
            SELECT DISTINCT date
            FROM accurate_npd_purchase_calculation
            ORDER BY date DESC
            LIMIT $1
        )
        SELECT 
            article_id,
            to_char(date, 'DD.MM') AS date,
            SUM(sum_net_profit) AS snp
        FROM accurate_npd_purchase_calculation
        WHERE date IN (SELECT date FROM latest_dates)
        GROUP BY article_id, date;
        """
        result = await self.db.fetch(select_query, days)

        return result
