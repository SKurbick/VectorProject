import asyncio

from service import wms_stocks_google_sheet
from service.wms_stocks_google_sheet import (
    WMSStocksGoogleSheetService,
    build_stock_matrix,
    merge_stock_rows,
)


def test_build_stock_matrix_fills_missing_stocks_with_zero():
    zones = [
        {
            "zone_id": 12,
            "zone_code": "PUSHKINO-STORAGE",
            "stocks": [
                {"product_id": "wild1825", "total_quantity": 150},
                {"product_id": "wild100", "total_quantity": 2},
            ],
        },
        {
            "zone_id": 13,
            "zone_code": "PUSHKINO-PICKING",
            "stocks": [
                {"product_id": "wild1825", "total_quantity": 7},
            ],
        },
    ]

    reservations = {"wild1825": 4, "wild-only-reserved": 2}

    assert build_stock_matrix(zones, reservations) == [
        ["wild", "PUSHKINO-STORAGE", "PUSHKINO-PICKING", "Резервы СЗ"],
        ["wild-only-reserved", 0, 0, 2],
        ["wild100", 2, 0, 0],
        ["wild1825", 150, 7, 4],
    ]


def test_build_stock_matrix_disambiguates_duplicate_zone_codes():
    zones = [
        {"zone_id": 12, "zone_code": "STORAGE", "stocks": []},
        {"zone_id": 21, "zone_code": "STORAGE", "stocks": []},
    ]

    assert build_stock_matrix(zones) == [
        ["wild", "STORAGE [12]", "STORAGE [21]", "Резервы СЗ"],
    ]


def test_merge_stock_rows_preserves_order_appends_new_and_zeros_missing():
    existing_rows = [
        ["wild1825", 1, 2, 3],
        ["old-product", 10, 20, 30],
    ]
    matrix = [
        ["wild", "STORAGE", "PICKING", "Резервы СЗ"],
        ["new-product", 5, 6, 7],
        ["wild1825", 150, 7, 4],
    ]

    rows, added_count = merge_stock_rows(existing_rows, matrix)

    assert rows == [
        ["wild1825", 150, 7, 4],
        ["old-product", 0, 0, 0],
        ["new-product", 5, 6, 7],
    ]
    assert added_count == 1


def test_sync_many_collects_once_and_updates_each_destination(monkeypatch):
    service = WMSStocksGoogleSheetService(
        wms_url="http://wms",
        spreadsheet="primary spreadsheet",
        worksheet="primary worksheet",
        credentials="creds.json",
    )
    matrix = [["wild", "STORAGE", "Резервы СЗ"], ["wild1", 10, 2]]
    collect_calls = 0
    update_calls = []

    async def fake_collect():
        nonlocal collect_calls
        collect_calls += 1
        return ([{"zone_code": "STORAGE"}], {"wild1": 2}, matrix)

    def fake_update_worksheet(matrix_arg, spreadsheet, worksheet, credentials):
        update_calls.append((matrix_arg, spreadsheet, worksheet, credentials))
        return 1, 0

    monkeypatch.setattr(service, "collect", fake_collect)
    monkeypatch.setattr(
        wms_stocks_google_sheet, "update_worksheet", fake_update_worksheet
    )

    results = asyncio.run(
        service.sync_many(
            [
                ("primary spreadsheet", "primary worksheet"),
                ("UNIT 2.0 (tested)", "WMS остатки"),
            ]
        )
    )

    assert collect_calls == 1
    assert update_calls == [
        (matrix, "primary spreadsheet", "primary worksheet", "creds.json"),
        (matrix, "UNIT 2.0 (tested)", "WMS остатки", "creds.json"),
    ]
    assert [result.updated_products for result in results] == [1, 1]
