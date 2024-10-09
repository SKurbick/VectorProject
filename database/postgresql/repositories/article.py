import asyncpg


class ArticleTable:
    def __init__(self, db):
        self.db = db

    async def check_nm_ids(self, account: str, nm_ids: list):
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


# nm_ids = [1,2,3,4,5,6]
# nm_ids_str = ', '.join(f"({nm_id})" for nm_id in nm_ids)
# print(nm_ids_str)