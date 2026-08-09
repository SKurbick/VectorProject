import asyncio
import json
import logging
from unittest.mock import mock_open, patch

import pytest

import settings
from account_names import create_wb_account_tasks, normalize_account_name


@pytest.mark.parametrize(
    "value",
    [" СТАРТ0854 ", "старт0854", "СтАрТ0854", "\u00a0СТАРТ0854\u00a0"],
)
def test_normalize_account_name_handles_spaces_nbsp_and_case(value):
    assert normalize_account_name(value) == "Старт0854"


def load_tokens(data):
    with patch("builtins.open", mock_open(read_data=json.dumps(data))):
        return settings.get_wb_tokens()


def test_token_key_matches_account_from_another_source():
    tokens = load_tokens({" \u00a0СТАРТ0854 ": "token"})

    database_account = "старт0854"
    google_sheets_account = " СтАрТ0854 "

    assert tokens.get(normalize_account_name(database_account)) == "token"
    assert tokens.get(normalize_account_name(google_sheets_account)) == "token"


def test_missing_token_returns_none_in_batch_lookup():
    tokens = load_tokens({"Другой": "token"})

    assert tokens.get(normalize_account_name("Старт0854")) is None


def test_equal_duplicate_tokens_are_kept_once(caplog):
    tokens = load_tokens({"Старт0854": "token", " СТАРТ0854 ": "token"})

    assert tokens == {"Старт0854": "token"}
    assert "Дублирующийся ключ токена WB" in caplog.text


def test_conflicting_duplicate_tokens_raise_configuration_error():
    with pytest.raises(ValueError, match="Конфликт токенов WB"):
        load_tokens({"Старт0854": "first", "\u00a0СТАРТ0854\u00a0": "second"})


def test_batch_skips_missing_token_and_leaves_no_pending_tasks():
    created_for = []

    async def process(account, payload, token):
        created_for.append(account)
        if account == "Ошибка":
            raise RuntimeError("account failure")
        return payload, token

    async def run_batch():
        accounts, tasks = create_wb_account_tasks(
            [(" без токена ", 0), ("СТАРТ0854", 1), ("Ошибка", 2)],
            {"Старт0854": "first", "Ошибка": "second"},
            process,
            logging.getLogger(__name__),
        )
        assert accounts == ["СТАРТ0854", "Ошибка"]
        assert len(tasks) == 2
        results = await asyncio.gather(*tasks, return_exceptions=True)
        assert results[0] == (1, "first")
        assert isinstance(results[1], RuntimeError)
        assert all(task.done() for task in tasks)

    asyncio.run(run_batch())
    assert created_for == ["СТАРТ0854", "Ошибка"]

