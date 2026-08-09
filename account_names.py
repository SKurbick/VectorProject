"""Canonical keys used to match Wildberries account names across sources."""

import asyncio


def normalize_account_name(account: str) -> str:
    """Return the canonical dictionary key for a WB account name."""
    return str(account).strip().capitalize()


def create_wb_account_tasks(account_items, tokens, coroutine_factory, logger):
    """Validate tokens before creating independent per-account asyncio tasks."""
    tasks = []
    task_accounts = []
    for account, payload in account_items:
        normalized_account = normalize_account_name(account)
        token = tokens.get(normalized_account)
        if not token:
            logger.warning(
                f"Кабинет {account!r} пропущен: для ключа "
                f"{normalized_account!r} отсутствует токен WB"
            )
            continue
        tasks.append(asyncio.create_task(coroutine_factory(account, payload, token)))
        task_accounts.append(account)
    return task_accounts, tasks

