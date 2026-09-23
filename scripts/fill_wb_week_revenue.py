#!/usr/bin/env python3
"""Заполнить существующие недельные колонки выручки данными WB API.

Скрипт не создаёт и не сдвигает колонки. По умолчанию работает в dry-run:
получает данные и выводит сводку. Для записи требуется явный флаг --apply.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
WB_URL = (
    "https://seller-analytics-api.wildberries.ru"
    "/api/analytics/v3/sales-funnel/products"
)


@dataclass(frozen=True)
class Period:
    header: str
    start: str
    end: str


PERIODS = (
    Period(header="09.07-09.13", start="2026-09-07", end="2026-09-13"),
    Period(header="09.14-09.20", start="2026-09-14", end="2026-09-20"),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Записать полученные данные. Без флага Google Sheet не изменяется.",
    )
    parser.add_argument(
        "--spreadsheet",
        default=os.getenv("SPREADSHEET"),
        help="Название Google-таблицы (по умолчанию SPREADSHEET из .env)",
    )
    parser.add_argument(
        "--worksheet",
        default=os.getenv("SHEET"),
        help="Название листа (по умолчанию SHEET из .env)",
    )
    parser.add_argument(
        "--credentials",
        default=os.getenv("CREEDS_FILE_NAME", "creds.json"),
        help="JSON сервисного аккаунта Google",
    )
    parser.add_argument(
        "--tokens",
        default=os.getenv("TOKENS_FILE_NAME", "tokens.json"),
        help="JSON с токенами кабинетов WB",
    )
    return parser.parse_args()


def resolve_project_path(value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else PROJECT_ROOT / path


def normalize_account(value: Any) -> str:
    return str(value).strip().capitalize()


def parse_article(value: Any) -> int | None:
    text = str(value).strip()
    if not text:
        return None
    try:
        number = float(text.replace(",", "."))
    except ValueError:
        return None
    if not number.is_integer():
        return None
    return int(number)


def column_label(column_number: int) -> str:
    label = ""
    while column_number:
        column_number, remainder = divmod(column_number - 1, 26)
        label = chr(65 + remainder) + label
    return label


def require_unique_header(headers: list[str], header: str) -> int:
    matches = [index for index, value in enumerate(headers) if value == header]
    if not matches:
        raise ValueError(
            f"Колонка {header!r} отсутствует. Скрипт не создаёт новые колонки."
        )
    if len(matches) > 1:
        raise ValueError(f"Колонка {header!r} встречается несколько раз.")
    return matches[0]


def load_tokens(path: Path) -> dict[str, str]:
    with path.open(encoding="utf-8") as stream:
        raw_tokens = json.load(stream)

    result: dict[str, str] = {}
    for source_name, token in raw_tokens.items():
        account = normalize_account(source_name)
        if account in result and result[account] != token:
            raise ValueError(f"Конфликт токенов после нормализации: {account!r}")
        result[account] = token
    return result


def load_sheet_context(
    spreadsheet: str,
    worksheet: str,
    credentials: Path,
) -> tuple[Any, list[str], list[list[Any]], dict[str, list[int]]]:
    import gspread

    client = gspread.service_account(filename=str(credentials))
    sheet = client.open(spreadsheet).worksheet(worksheet)
    all_values = sheet.get_all_values()
    if not all_values:
        raise ValueError("Лист Google Sheets пуст.")

    headers = all_values[0]
    article_index = require_unique_header(headers, "Артикул")
    account_index = require_unique_header(headers, "ЛК")
    profit_index = require_unique_header(headers, "Чистая прибыль 1ед.")
    for period in PERIODS:
        require_unique_header(headers, period.header)

    articles_by_account: dict[str, list[int]] = {}
    skipped_invalid_profit = 0
    for row in all_values[1:]:
        account = (
            normalize_account(row[account_index])
            if account_index < len(row)
            else ""
        )
        article = (
            parse_article(row[article_index])
            if article_index < len(row)
            else None
        )
        profit = (
            str(row[profit_index]).replace("\xa0", "").strip()
            if profit_index < len(row)
            else ""
        )
        if not account or article is None:
            continue
        if not profit.lstrip("-").isdigit():
            skipped_invalid_profit += 1
            continue
        articles_by_account.setdefault(account, []).append(article)

    for account, articles in articles_by_account.items():
        unique_articles = sorted(set(articles))
        if len(unique_articles) > 1000:
            raise ValueError(
                f"У кабинета {account!r} {len(unique_articles)} артикулов; "
                "WB API принимает максимум 1000 nmIds за запрос."
            )
        articles_by_account[account] = unique_articles

    print(
        f"Строк в листе: {len(all_values) - 1}; "
        f"кабинетов: {len(articles_by_account)}; "
        f"строк пропущено из-за значения 'Чистая прибыль 1ед.': "
        f"{skipped_invalid_profit}"
    )
    return sheet, headers, all_values, articles_by_account


async def fetch_period(
    session: Any,
    semaphore: asyncio.Semaphore,
    account: str,
    token: str,
    article_ids: list[int],
    period: Period,
) -> dict[int, int | float]:
    result: dict[int, int | float] = {}
    offset = 0
    limit = 1000

    while True:
        payload = {
            "selectedPeriod": {"start": period.start, "end": period.end},
            "nmIds": article_ids,
            "timezone": "Europe/Moscow",
            "orderBy": {"field": "orderSum", "mode": "asc"},
            "limit": limit,
            "offset": offset,
        }

        response_data: dict[str, Any] | None = None
        for attempt in range(1, 6):
            async with semaphore:
                async with session.post(
                    WB_URL,
                    headers={
                        "Authorization": token,
                        "Content-Type": "application/json",
                    },
                    json=payload,
                ) as response:
                    if response.status == 429:
                        wait_seconds = int(response.headers.get("Retry-After", "60"))
                        print(
                            f"{account} / {period.header}: HTTP 429, "
                            f"повтор через {wait_seconds} сек. ({attempt}/5)"
                        )
                    elif response.status == 200:
                        response_data = await response.json()
                        break
                    else:
                        body = (await response.text())[:1000]
                        raise RuntimeError(
                            f"{account} / {period.header}: WB API вернул "
                            f"HTTP {response.status}: {body}"
                        )
            if response_data is not None:
                break
            await asyncio.sleep(wait_seconds)

        if response_data is None:
            raise RuntimeError(
                f"{account} / {period.header}: исчерпаны повторы WB API"
            )

        products = response_data.get("data", {}).get("products", [])
        for product in products:
            nm_id = int(product["product"]["nmId"])
            result[nm_id] = product["statistic"]["selected"]["orderSum"]

        if len(products) < limit:
            return result
        offset += limit


async def fetch_account(
    session: Any,
    semaphore: asyncio.Semaphore,
    account: str,
    token: str,
    article_ids: list[int],
) -> tuple[str, dict[str, dict[int, int | float]]]:
    period_results = await asyncio.gather(
        *(
            fetch_period(
                session=session,
                semaphore=semaphore,
                account=account,
                token=token,
                article_ids=article_ids,
                period=period,
            )
            for period in PERIODS
        )
    )
    return account, {
        period.header: result
        for period, result in zip(PERIODS, period_results)
    }


async def fetch_all_accounts(
    articles_by_account: dict[str, list[int]],
    tokens: dict[str, str],
) -> dict[str, dict[str, dict[int, int | float]]]:
    import aiohttp

    missing_tokens = sorted(set(articles_by_account) - set(tokens))
    if missing_tokens:
        raise ValueError(
            "Нет токенов для кабинетов: " + ", ".join(missing_tokens)
        )

    timeout = aiohttp.ClientTimeout(total=90)
    semaphore = asyncio.Semaphore(10)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        account_results = await asyncio.gather(
            *(
                fetch_account(
                    session=session,
                    semaphore=semaphore,
                    account=account,
                    token=tokens[account],
                    article_ids=article_ids,
                )
                for account, article_ids in sorted(articles_by_account.items())
            )
        )
    return dict(account_results)


def print_summary(
    results: dict[str, dict[str, dict[int, int | float]]]
) -> None:
    print("\nПолучены данные WB:")
    for period in PERIODS:
        values = [
            value
            for account_data in results.values()
            for value in account_data[period.header].values()
        ]
        print(
            f"  {period.header}: товаров {len(values)}, "
            f"ненулевых {sum(value != 0 for value in values)}, "
            f"orderSum {sum(values):,.2f}"
        )


def update_existing_columns(
    sheet: Any,
    expected_headers: list[str],
    results: dict[str, dict[str, dict[int, int | float]]],
) -> dict[str, int]:
    current_values = sheet.get_all_values()
    if not current_values:
        raise ValueError("Лист Google Sheets стал пустым перед записью.")
    headers = current_values[0]
    if headers != expected_headers:
        raise RuntimeError(
            "Заголовки листа изменились после чтения данных. "
            "Запись отменена; запустите скрипт заново."
        )

    article_index = require_unique_header(headers, "Артикул")
    account_index = require_unique_header(headers, "ЛК")
    updated_counts: dict[str, int] = {}
    updates: list[dict[str, Any]] = []

    for period in PERIODS:
        column_index = require_unique_header(headers, period.header)
        column_values: list[list[Any]] = []
        updated_count = 0

        for row in current_values[1:]:
            existing_value = row[column_index] if column_index < len(row) else ""
            account = (
                normalize_account(row[account_index])
                if account_index < len(row)
                else ""
            )
            article = (
                parse_article(row[article_index])
                if article_index < len(row)
                else None
            )
            new_value = (
                results.get(account, {})
                .get(period.header, {})
                .get(article)
            )
            if new_value is not None:
                column_values.append([new_value])
                updated_count += 1
            else:
                column_values.append([existing_value])

        column = column_label(column_index + 1)
        updates.append({
            "range": f"{column}2:{column}{len(current_values)}",
            "values": column_values,
        })
        updated_counts[period.header] = updated_count

    sheet.batch_update(updates, value_input_option="RAW")
    return updated_counts


async def async_main(args: argparse.Namespace) -> None:
    if not args.spreadsheet or not args.worksheet:
        raise SystemExit(
            "Укажите --spreadsheet и --worksheet или настройте SPREADSHEET/SHEET."
        )

    credentials = resolve_project_path(args.credentials)
    tokens_path = resolve_project_path(args.tokens)
    if not credentials.is_file():
        raise FileNotFoundError(f"Не найден файл credentials: {credentials}")
    if not tokens_path.is_file():
        raise FileNotFoundError(f"Не найден файл токенов: {tokens_path}")

    sheet, headers, _, articles_by_account = load_sheet_context(
        spreadsheet=args.spreadsheet,
        worksheet=args.worksheet,
        credentials=credentials,
    )
    results = await fetch_all_accounts(
        articles_by_account=articles_by_account,
        tokens=load_tokens(tokens_path),
    )
    print_summary(results)

    if not args.apply:
        print("\nDry-run завершён: Google Sheet не изменён.")
        print("Для записи повторите команду с флагом --apply.")
        return

    updated_counts = update_existing_columns(
        sheet=sheet,
        expected_headers=headers,
        results=results,
    )
    print("\nЗапись завершена:")
    for header, count in updated_counts.items():
        print(f"  {header}: обновлено строк {count}")


def main() -> None:
    from dotenv import load_dotenv

    load_dotenv(PROJECT_ROOT / ".env")
    args = parse_args()
    asyncio.run(async_main(args))


if __name__ == "__main__":
    main()
