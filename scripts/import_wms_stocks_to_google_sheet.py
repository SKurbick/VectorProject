#!/usr/bin/env python3
"""Выгрузка остатков WMS в Google Sheets.

Строки — product_id под заголовком wild, столбцы — зоны WMS,
последний столбец — активные резервы по сборочным заданиям.
Заголовки и пользовательские столбцы справа от «Резервы СЗ» не изменяются.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from service import wms_stocks_google_sheet as wms_stock_service

DEFAULT_SPREADSHEET = "Расчет закупки Россия"
DEFAULT_WORKSHEET = "Остатки из WMS"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--wms-url",
        default=os.getenv("WMS_BASE_URL"),
        help="Базовый URL WMS (или переменная WMS_BASE_URL)",
    )
    parser.add_argument(
        "--spreadsheet",
        default=os.getenv("WMS_STOCKS_SPREADSHEET", DEFAULT_SPREADSHEET),
    )
    parser.add_argument(
        "--worksheet",
        default=os.getenv("WMS_STOCKS_WORKSHEET", DEFAULT_WORKSHEET),
    )
    parser.add_argument(
        "--credentials",
        default=os.getenv("CREEDS_FILE_NAME", "creds.json"),
        help="Путь к JSON сервисного аккаунта Google",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Получить данные и показать размер матрицы, не изменяя Google Sheet",
    )
    return parser.parse_args()


async def _get_json(session: Any, path: str) -> list[dict[str, Any]]:
    async with session.get(path) as response:
        response.raise_for_status()
        payload = await response.json()

    if not isinstance(payload, list):
        raise ValueError(f"WMS вернул не список для {path}: {type(payload).__name__}")
    return payload


async def get_stock_by_zones(wms_url: str) -> list[dict[str, Any]]:
    import aiohttp

    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(
        base_url=wms_url.rstrip("/"), timeout=timeout
    ) as session:
        zones = await _get_json(session, "/api/locations/zones")
        summaries = await asyncio.gather(
            *(
                _get_json(
                    session,
                    f"/api/inventory/location/{zone['location_id']}/recursive-summary",
                )
                for zone in zones
            )
        )

    return [
        {
            "zone_id": zone["location_id"],
            "zone_code": zone["location_code"],
            "zone_name": zone.get("name", ""),
            "warehouse_code": zone.get("warehouse_code", ""),
            "stocks": summary,
        }
        for zone, summary in zip(zones, summaries)
    ]


async def get_reserved_quantities(
    wms_url: str, page_size: int = 5000
) -> dict[str, int | float]:
    """Получить reserved_qty по всем товарам с учетом пагинации WMS."""
    import aiohttp

    timeout = aiohttp.ClientTimeout(total=30)
    reserved_by_product: dict[str, int | float] = {}
    offset = 0

    async with aiohttp.ClientSession(
        base_url=wms_url.rstrip("/"), timeout=timeout
    ) as session:
        while True:
            page = await _get_json(
                session,
                "/api/inventory/availability"
                f"?only_reserved=true&limit={page_size}&offset={offset}",
            )
            for item in page:
                product_id = str(item["product_id"])
                reserved_by_product[product_id] = item.get("reserved_qty", 0) or 0

            if len(page) < page_size:
                break
            offset += page_size

    return reserved_by_product


def build_stock_matrix(
    zones: list[dict[str, Any]],
    reserved_by_product: dict[str, int | float] | None = None,
) -> list[list[Any]]:
    """Собрать таблицу: product_id × zone_code плюс резервы СЗ."""
    reserved_by_product = reserved_by_product or {}
    headers = [str(zone["zone_code"]) for zone in zones]
    if len(headers) != len(set(headers)):
        headers = [
            f"{zone['zone_code']} [{zone['zone_id']}]"
            for zone in zones
        ]

    quantities_by_product: dict[str, dict[int, int | float]] = defaultdict(dict)
    for zone_index, zone in enumerate(zones):
        for stock in zone["stocks"]:
            product_id = str(stock["product_id"])
            quantity = stock.get("total_quantity", 0) or 0
            quantities_by_product[product_id][zone_index] = (
                quantities_by_product[product_id].get(zone_index, 0) + quantity
            )

    product_ids = set(quantities_by_product) | set(reserved_by_product)
    matrix: list[list[Any]] = [["wild", *headers, "Резервы СЗ"]]
    for product_id in sorted(product_ids, key=str.casefold):
        zone_quantities = quantities_by_product[product_id]
        matrix.append(
            [
                product_id,
                *(zone_quantities.get(index, 0) for index in range(len(zones))),
                reserved_by_product.get(product_id, 0),
            ]
        )
    return matrix


def merge_stock_rows(
    existing_rows: list[list[Any]], matrix: list[list[Any]]
) -> tuple[list[list[Any]], int]:
    """Сохранить порядок товаров и добавить новые товары в конец списка."""
    column_count = len(matrix[0])
    quantities_by_product = {
        str(row[0]): list(row[1:])
        for row in matrix[1:]
    }

    existing_product_ids: list[str] = []
    seen: set[str] = set()
    for row_number, row in enumerate(existing_rows, start=2):
        product_id = str(row[0]).strip() if row else ""
        if not product_id:
            raise ValueError(
                f"Пустой wild в строке {row_number}. "
                "Уберите разрывы внутри списка товаров перед запуском."
            )
        if product_id in seen:
            raise ValueError(
                f"Товар {product_id!r} несколько раз указан в столбце wild"
            )
        seen.add(product_id)
        existing_product_ids.append(product_id)

    new_product_ids = [
        str(row[0]) for row in matrix[1:] if str(row[0]) not in seen
    ]
    zero_values = [0] * (column_count - 1)
    merged_rows = [
        [product_id, *quantities_by_product.get(product_id, zero_values)]
        for product_id in [*existing_product_ids, *new_product_ids]
    ]
    return merged_rows, len(new_product_ids)


def _column_label(column_number: int) -> str:
    label = ""
    while column_number:
        column_number, remainder = divmod(column_number - 1, 26)
        label = chr(65 + remainder) + label
    return label


def update_worksheet(
    matrix: list[list[Any]], spreadsheet: str, worksheet: str, credentials: str
) -> tuple[int, int]:
    """Обновить только служебный блок, не затрагивая клиентские столбцы."""
    import gspread

    credentials_path = Path(credentials)
    if not credentials_path.is_absolute():
        credentials_path = PROJECT_ROOT / credentials_path
    if not credentials_path.is_file():
        raise FileNotFoundError(f"Не найден файл credentials: {credentials_path}")

    client = gspread.service_account(filename=str(credentials_path))
    sheet = client.open(spreadsheet).worksheet(worksheet)
    expected_headers = matrix[0]
    managed_column_count = len(expected_headers)
    last_managed_column = _column_label(managed_column_count)
    managed_values = sheet.get(f"A1:{last_managed_column}")
    actual_headers = managed_values[0] if managed_values else []
    actual_headers += [""] * (managed_column_count - len(actual_headers))

    if actual_headers != expected_headers:
        raise ValueError(
            "Заголовки Google Sheet не совпадают с зонами WMS. "
            f"Ожидаются: {expected_headers}; в листе: {actual_headers}. "
            "Обновите структуру листа и скрипт вручную."
        )

    existing_rows = managed_values[1:]
    while existing_rows and not any(existing_rows[-1]):
        existing_rows.pop()
    merged_rows, added_count = merge_stock_rows(existing_rows, matrix)
    required_rows = len(merged_rows) + 1

    if sheet.row_count < required_rows:
        sheet.resize(rows=required_rows, cols=sheet.col_count)

    if merged_rows:
        sheet.update(
            values=merged_rows,
            range_name=f"A2:{last_managed_column}{required_rows}",
            value_input_option="RAW",
        )
    return len(merged_rows), added_count


async def async_main(args: argparse.Namespace) -> None:
    if not args.wms_url:
        raise SystemExit("Укажите --wms-url или переменную окружения WMS_BASE_URL")

    zones, reserved_by_product = await asyncio.gather(
        wms_stock_service.get_stock_by_zones(args.wms_url),
        wms_stock_service.get_reserved_quantities(args.wms_url),
    )
    matrix = wms_stock_service.build_stock_matrix(zones, reserved_by_product)
    print(
        f"Получено зон: {len(zones)}; товаров с резервами: "
        f"{len(reserved_by_product)}; товаров в матрице: {len(matrix) - 1}; "
        f"размер матрицы: {len(matrix)}x{len(matrix[0])}"
    )

    if args.dry_run:
        print("Dry-run: Google Sheet не изменен")
        return

    updated_count, added_count = await asyncio.to_thread(
        wms_stock_service.update_worksheet,
        matrix=matrix,
        spreadsheet=args.spreadsheet,
        worksheet=args.worksheet,
        credentials=args.credentials,
    )
    print(
        f"Обновлен лист: {args.spreadsheet} / {args.worksheet}; "
        f"товаров обновлено: {updated_count}; новых добавлено: {added_count}"
    )


def main() -> None:
    from dotenv import load_dotenv

    load_dotenv(PROJECT_ROOT / ".env")
    args = parse_args()
    asyncio.run(async_main(args))


if __name__ == "__main__":
    main()
