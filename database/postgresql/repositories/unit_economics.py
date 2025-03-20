

class UnitEconomicsTable:
    """Таблица unit_economics
     article_id
     price
     discount
     <...>
    """

    def __init__(self, db):
        self.db = db

    async def update_data(self, data):

        query = """
        INSERT INTO unit_economics (article_id, commission_wb, discount,
                                logistic_from_wb_wh_to_opp,  price, cost_price, percent_by_tax, last_update_time)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (article_id) DO UPDATE 
        SET commission_wb = EXCLUDED.commission_wb,
            discount = EXCLUDED.discount,
            logistic_from_wb_wh_to_opp = EXCLUDED.logistic_from_wb_wh_to_opp,
            price = EXCLUDED.price,
            percent_by_tax = EXCLUDED.percent_by_tax,
            last_update_time = EXCLUDED.last_update_time,
            cost_price = EXCLUDED.cost_price;                   
        """
        await self.db.executemany(query, data)
