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
    REQUEST_TIMEOUT = (10, 60)
    REQUEST_RETRIES = 3

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

    @staticmethod
    def _get_not_found_chrt_ids(response_data):
        """Извлекает chrtId, которые WB явно отклонил как NotFound."""
        errors = response_data if isinstance(response_data, list) else [response_data]
        not_found_chrt_ids = set()

        for error in errors:
            if not isinstance(error, dict) or error.get("code") != "NotFound":
                continue

            error_data = error.get("data", [])
            if isinstance(error_data, dict):
                error_data = [error_data]

            for stock in error_data:
                if isinstance(stock, dict) and stock.get("chrtId") is not None:
                    not_found_chrt_ids.add(str(stock["chrtId"]))

        return not_found_chrt_ids

    def edit_amount_from_warehouses(self, warehouse_id, edit_barcodes_list, step=1000):
        """Возвращает позиции, успешно отправленные и отклонённые на этом складе."""
        url = self.url.format(f"{warehouse_id}")
        result = {"successful": [], "failed": []}
        batches_to_process = [
            edit_barcodes_list[start: start + step]
            for start in range(0, len(edit_barcodes_list), step)
        ]

        while batches_to_process:
            barcodes_part = batches_to_process.pop(0)
            logger.info(barcodes_part)
            json_data = {"stocks": barcodes_part}
            response = None

            for attempt in range(1, self.REQUEST_RETRIES + 1):
                try:
                    response = requests.put(
                        url=url,
                        headers=self.headers,
                        json=json_data,
                        timeout=self.REQUEST_TIMEOUT,
                    )
                    break
                except requests.exceptions.RequestException as e:
                    logger.error(
                        "Ошибка соединения при изменении остатков. Склад: {}. Попытка {}/{}. Ошибка: {}",
                        warehouse_id,
                        attempt,
                        self.REQUEST_RETRIES,
                        e,
                    )
                    if attempt < self.REQUEST_RETRIES:
                        time.sleep(10 * attempt)

            if response is None:
                logger.error(
                    "Ошибка запроса на изменение остатков. Пачка пропущена после {} попыток. Склад: {}",
                    self.REQUEST_RETRIES,
                    warehouse_id,
                )
                result["failed"].extend(barcodes_part)
                continue

            if response.status_code <= 399:
                logger.info(f"Запрос на изменение остатков. Код: {response.status_code}")
                result["successful"].extend(barcodes_part)
                continue

            try:
                response_data = response.json()
            except requests.exceptions.JSONDecodeError:
                response_data = response.text or "<пустой ответ>"

            not_found_chrt_ids = set()
            if response.status_code == 409:
                not_found_chrt_ids = self._get_not_found_chrt_ids(response_data)

            rejected_stocks = [
                stock for stock in barcodes_part
                if str(stock.get("chrtId")) in not_found_chrt_ids
            ]
            stocks_to_retry = [
                stock for stock in barcodes_part
                if str(stock.get("chrtId")) not in not_found_chrt_ids
            ]

            if rejected_stocks and len(stocks_to_retry) < len(barcodes_part):
                result["failed"].extend(rejected_stocks)
                logger.error(
                    "WB отклонил chrtId как NotFound. Склад: {}. Исключены: {}",
                    warehouse_id,
                    sorted(not_found_chrt_ids),
                )
                if stocks_to_retry:
                    batches_to_process.insert(0, stocks_to_retry)
                continue

            logger.error(
                "Ошибка запроса на изменение остатков. Пачка пропущена. Код: {}. Content-Type: {}. Ответ: {}",
                response.status_code,
                response.headers.get("Content-Type", "<не указан>"),
                response_data,
            )
            result["failed"].extend(barcodes_part)

        return result


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
