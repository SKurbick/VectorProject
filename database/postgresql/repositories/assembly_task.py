from typing import List, Dict, Any


class AssemblyTask:
    """Таблица article"""

    def __init__(self, db):
        self.db = db


    async def get_order_status_counts_by_vendor(self) -> List[Dict[str, Any]]:
        """
        Получает количество заказов по статусам для каждого вендора
        """
        query = """
        SELECT 
            a.local_vendor_code,
            COUNT(CASE WHEN osl.status IN ('IN_TECHNICAL_SUPPLY', 'NEW') THEN 1 END) as total_orders,
            COUNT(CASE WHEN osl.status = 'IN_TECHNICAL_SUPPLY' THEN 1 END) as in_technical_supply_count,
            COUNT(CASE WHEN osl.status = 'NEW' THEN 1 END) as new_count
        FROM public.order_status_log osl
        JOIN assembly_task as ast ON ast.task_id = osl.order_id
        JOIN article as a ON ast.article_id = a.nm_id
        INNER JOIN (
            SELECT 
                order_id, 
                MAX(created_at) as max_created_at
            FROM public.order_status_log
            GROUP BY order_id
        ) latest ON osl.order_id = latest.order_id AND osl.created_at = latest.max_created_at
        WHERE osl.status IN ('IN_TECHNICAL_SUPPLY', 'NEW')
        GROUP BY a.local_vendor_code
        ORDER BY total_orders DESC;
        """

        rows = await self.db.fetch(query)
        return [dict(row) for row in rows]
