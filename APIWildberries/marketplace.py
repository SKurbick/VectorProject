import requests


class Wildberries:
    """Base class"""
    pass


class MarketplaceWB:
    """Base class"""
    pass


class WarehouseMarketplaceWB:
    """API складов маркетплейс"""
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
        barcodes_quantity = {}
        for start in range(0, len(barcodes), step):
            barcodes_part = barcodes[start: start + step]

            json_data = {
                "skus": barcodes_part
            }
            response = requests.post(url=url, headers=self.headers, json=json_data)

            stocks = response.json()["stocks"]
            if len(stocks) > 0:
                for stock in stocks:
                    barcodes_quantity.update(
                        {
                            stock["sku"]: stock["amount"]
                        }
                    )
        return barcodes_quantity
