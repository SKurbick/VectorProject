from datetime import datetime
from typing import Union, Dict

from pydantic import BaseModel


class CostPriceDB(BaseModel):
    local_vendor_code: str
    purchase_price: Union[int, None]
    status_by_lvc: Union[str, None]
    created_at: datetime
    last_check_datetime: datetime


# Контейнер для хранения данных
class CostPriceDBContainer:
    def __init__(self, records: list[dict]):
        # Преобразуем данные в модели Pydantic и сохраняем в словарь
        self.local_vendor_code: Dict[str, CostPriceDB] = {
            record["local_vendor_code"]: CostPriceDB(**record) for record in records
        }