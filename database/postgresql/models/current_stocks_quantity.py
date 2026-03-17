from typing import Dict, Union, Optional

from pydantic import BaseModel, RootModel, field_validator


class ArticleBase(BaseModel):
    article_id: int


class StocksQuantity(BaseModel):
    root: Dict[str, Union[None, int]]

    def __getitem__(self, item):
        return self.root[item]

    def __iter__(self):
        return iter(self.root)

    def __len__(self):
        return len(self.root)


class FederalDistrictData(BaseModel):
    daily_average: Union[float, None]
    balance_for_number_of_days: Union[float, None]

    @field_validator('daily_average', 'balance_for_number_of_days', mode='before')
    def round_float_values(cls, v: Optional[Union[float, str]]) -> Optional[float]:
        if v is None or v == '':
            return 0
        try:
            return round(float(v), 2)
        except (ValueError, TypeError):
            return None


# Модель для данных по каждому федеральному округу (словарь с ключами - названиями округов)
class TurnoverByFederalDistrict(RootModel):
    root: Dict[str, FederalDistrictData]

    def __getitem__(self, item):
        return self.root[item]

    def __iter__(self):
        return iter(self.root)

    def __len__(self):
        return len(self.root)


# Модель для всего набора данных (внешний словарь с ключами - числами)
class TurnoverByFederalDistrictData(RootModel):
    root: Dict[int, TurnoverByFederalDistrict]

    def __getitem__(self, item):
        return self.root[item]

    def __iter__(self):
        return iter(self.root)

    def __len__(self):
        return len(self.root)
