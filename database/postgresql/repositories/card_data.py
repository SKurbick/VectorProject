from typing import Set


class CardData:
    """Таблица card_data
        article_id
        barcode
        commission_wb
        height
        length
        width
        local_vendor_code
        logistic_from_wb_wh_to_opp
        photo
        discount
        price
        subject_name
        rating
    """

    def __init__(self, db):
        self.db = db

    async def get_card_data(self):
        query = """
        SELECT * FROM card_data"""
        result = await self.db.fetch(query)
        return result

    async def update_card_data(self, data):
        query = """
        INSERT INTO card_data (article_id, barcode,commission_wb, discount, height, length, 
                                logistic_from_wb_wh_to_opp, photo_link, price, subject_name, width, last_update_time)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        ON CONFLICT (article_id) DO UPDATE 
        SET barcode = EXCLUDED.barcode,
            commission_wb = EXCLUDED.commission_wb,
            discount = EXCLUDED.discount,
            height = EXCLUDED.height,
            length = EXCLUDED.length,
            logistic_from_wb_wh_to_opp = EXCLUDED.logistic_from_wb_wh_to_opp,
            photo_link = EXCLUDED.photo_link,
            price = EXCLUDED.price,
            subject_name = EXCLUDED.subject_name,
            width = EXCLUDED.width,
            last_update_time = EXCLUDED.last_update_time;                   
        """
        await self.db.executemany(query, data)

    async def update_rating(self, data):
        query = """
        INSERT INTO card_data (article_id, rating)
        VALUES ($1, $2)
        ON CONFLICT (article_id) DO UPDATE 
        SET rating = EXCLUDED.rating;
        """
        await self.db.executemany(query, data)

    async def get_subject_name_to_article(self, article_ids: list):
        query = """
        SELECT subject_name, article_id FROM card_data
        WHERE article_id = ANY($1)
        """
        return await self.db.fetch(query, article_ids)

    async def get_actually_all_information(self, article_ids: Set[int]):
        query = """SELECT cd.*, a.local_vendor_code 
               FROM card_data cd 
               JOIN article a ON cd.article_id = a.nm_id 
               WHERE cd.article_id = ANY($1);"""
        return await self.db.fetch(query, article_ids)
