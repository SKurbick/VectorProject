from dataclasses import dataclass


@dataclass
class AccurateNetProfitData:
    id: int
    article_id: int
    net_profit: int
    orders: int
    time: str
    date: str
    sum_net_profit: int
