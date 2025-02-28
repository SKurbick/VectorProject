import time

import requests
from logger import app_logger as logger


class Wildberries:
    """Base class"""
    pass


class MarketplaceWB:
    """Base class"""
    pass


class AssemblyTasksMarketplaceWB:
    """API складов маркетплейс"""

    def __init__(self, token):
        self.url = "https://marketplace-api.wildberries.ru/api/v3/orders/"

    def get_list_new_assembly_tasks(self, ):
        pass


class SuppliesMarketplaceWB:
    """API складов маркетплейс"""

    pass


class BalancesMarketplaceWB:
    """API складов маркетплейс"""

    pass


class PassesMarketplaceWB:
    """API складов маркетплейс"""

    pass


class DeliveryByTheSellersMPWB:
    """API складов маркетплейс"""

    pass


class LeftoversMarketplace:
    def __init__(self, token):
        self.token = token
        self.url = "https://marketplace-api.wildberries.ru/api/v3/stocks/{}"
        self.headers = {
            "Authorization": self.token,
            'Content-Type': 'application/json'
        }

    def get_amount_from_warehouses(self, warehouse_id, barcodes, step=1000):
        url = self.url.format(f"{warehouse_id}")
        barcodes_quantity = []
        for start in range(0, len(barcodes), step):
            barcodes_part = barcodes[start: start + step]

            json_data = {
                "skus": barcodes_part
            }
            response = requests.post(url=url, headers=self.headers, json=json_data)

            stocks = response.json()["stocks"]
            if len(stocks) > 0:
                for stock in stocks:
                    barcodes_quantity.append(
                        {
                            "Баркод": stock["sku"],
                            "остаток": stock["amount"]
                        }
                    )
        return barcodes_quantity

    def edit_amount_from_warehouses(self, warehouse_id, edit_barcodes_list, step=1000):
        url = self.url.format(f"{warehouse_id}")
        for start in range(0, len(edit_barcodes_list), step):
            barcodes_part = edit_barcodes_list[start: start + step]
            logger.info(barcodes_part)
            json_data = {
                "stocks": barcodes_part
            }
            response = requests.put(url=url, headers=self.headers, json=json_data)
            if response.status_code > 399:
                logger.info(f"Запрос на изменение остатков: {response.json()}")
            else:
                logger.info(f"Запрос на изменение остатков. Код: {response.status_code}", )


class WarehouseMarketplaceWB:
    """API складов маркетплейс"""

    def __init__(self, token):
        self.token = token
        self.headers = {
            "Authorization": self.token,
            'Content-Type': 'application/json'
        }
        self.url = "https://marketplace-api.wildberries.ru/api/v3/warehouses"

    def get_account_warehouse(self, ):
        response = requests.get(url=self.url, headers=self.headers)
        if response.status_code > 400:
            try:
                for _ in range(10):
                    response = requests.get(url=self.url, headers=self.headers)
                    if response.status_code < 400:
                        time.sleep(60)
                        break

            except Exception as e:
                logger.exception(e)

        return response.json()
