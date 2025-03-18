import asyncpg
from logger import app_logger as logger


class ArticleTable:
    """Таблица article"""

    def __init__(self, db):
        self.db = db

    async def check_nm_ids(self, account: str, nm_ids: list):
        """Возвращает артикулы которых нет в таблице article"""
        nm_ids_str = ', '.join(f"({nm_id})" for nm_id in nm_ids)
        logger.info(f"check_nm_ids  : {nm_ids_str}")
        query = f"""
        SELECT nm_id
        FROM (VALUES {nm_ids_str}) AS input(nm_id)
        EXCEPT
        SELECT nm_id
        FROM article;
        """
        not_found_nm_ids = await self.db.fetch(query)

        return [result_nm_id["nm_id"] for result_nm_id in not_found_nm_ids]

    async def update_articles(self, data, filter_nm_ids):
        """Добавляет артикулы c данными из списка data"""
        async with self.db.transaction():
            # Подготовка данных для пакетной вставки в vendor_mapping
            article_data = [(nm_id, data[nm_id]['account'], data[nm_id]['vendor_code'], data[nm_id]['wild'])
                            for nm_id in filter_nm_ids]

            logger.info(article_data)
            # Пакетная вставка в vendor_mapping
            vendor_mapping_query = """
            INSERT INTO article (nm_id, account, vendor_code, local_vendor_code)
            VALUES ($1, $2, $3, $4) 
            ON CONFLICT (account, vendor_code) DO NOTHING;
            """
            # await self.db.executemany(vendor_mapping_query, article_data)

            for i, (nm_id, account, vendor_code, wild) in enumerate(article_data):
                if vendor_code == "не найдено":
                    new_vendor_code = vendor_code
                    counter = 0
                    while True:
                        # Проверяем, существует ли уже такой vendor_code
                        check_query = """
                        SELECT 1 FROM article WHERE account = $1 AND vendor_code = $2;
                        """
                        result = await self.db.fetch(check_query, account, new_vendor_code)
                        if not result:
                            # Если такого vendor_code нет, используем его
                            break
                        counter += 1
                        new_vendor_code = f"{vendor_code}{counter}"

                    # Обновляем vendor_code в данных
                    article_data[i] = (nm_id, account, new_vendor_code, wild)

            await self.db.executemany(vendor_mapping_query, article_data)

    async def get_all_nm_ids(self):
        query = """
        SELECT *
        FROM article;
        """
        nm_ids = await self.db.fetch(query)
        return {str(data['nm_id']): dict(data) for data in nm_ids}
