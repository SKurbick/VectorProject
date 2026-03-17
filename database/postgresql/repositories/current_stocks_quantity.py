import datetime
from collections import defaultdict
from typing import List

from database.postgresql.models.current_stocks_quantity import StocksQuantity


class CurrentStocksQuantity:
    def __init__(self, db):
        self.db = db

    async def get_all_data(self):
        async with self.db.acquire() as conn:
            query = "SELECT article_id, quantity_type, quantity FROM current_stocks_quantity"
            results = await conn.fetch(query)
            return results
