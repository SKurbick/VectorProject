import asyncpg


class ArticleTable:
    """Таблица article"""
    def __init__(self, db):
        self.db = db

    async def check_nm_ids(self, account: str, nm_ids: list):
        """Возвращает артикулы которых нет в таблице article"""
        nm_ids_str = ', '.join(f"({nm_id})" for nm_id in nm_ids)
        query = f"""
        SELECT nm_id
        FROM (VALUES {nm_ids_str}) AS input(nm_id)
        EXCEPT
        SELECT nm_id
        FROM article;
        """
        not_found_nm_ids = await self.db.fetch(query)

        return [result_nm_id["nm_id"] for result_nm_id in not_found_nm_ids]

    async def update_articles(self, data):
        """Добавляет артикулы c данными из списка data"""
        async with self.db.transaction():
            # Подготовка данных для пакетной вставки в vendor_mapping
            article_data = [(nm_id, fields['account'], fields['vendor_code'], fields['wild'])
                            for nm_id, fields in data.items()]

            print(article_data)
            # Пакетная вставка в vendor_mapping
            vendor_mapping_query = """
            INSERT INTO article (nm_id, account, vendor_code, local_vendor_code)
            VALUES ($1, $2, $3, $4) ;
            """
            await self.db.executemany(vendor_mapping_query, article_data)
