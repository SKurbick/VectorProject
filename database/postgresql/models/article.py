from dataclasses import dataclass
from datetime import datetime


@dataclass
class Article:
    id: int
    nm_id: int
    account: str
    created_at: datetime
    vendor_name: str
    local_vendor_name: str

    # @classmethod
    # def from_record(cls, record):
    #     return cls(
    #         id=record['id'],
    #         nm_id=record['nm_id'],
    #         account=record['account'],
    #         created_at=record['created_at'],
    #         vendor_name=record['vendor_name'],
    #         local_vendor_name=record['local_vendor_name']
    #
    #     )
    #
    # def __repr__(self):
    #     return f"<Article id={self.id} nm_id={self.nm_id} account={self.account}>"
