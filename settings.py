import json
from dataclasses import dataclass
from dotenv import load_dotenv
import os

from account_names import normalize_account_name
from logger import app_logger as logger

load_dotenv()


def get_wb_tokens() -> dict:
    with open(settings.TOKENS_FILE_NAME, "r", encoding='utf-8') as file:
        raw_tokens = json.load(file)
    tokens = {}
    original_names = {}
    for original_name, token in raw_tokens.items():
        normalized_name = normalize_account_name(original_name)
        if normalized_name in tokens:
            previous_name = original_names[normalized_name]
            if tokens[normalized_name] != token:
                raise ValueError(
                    f"Конфликт токенов WB: ключи {previous_name!r} и {original_name!r} "
                    f"нормализуются в {normalized_name!r}, но содержат разные токены"
                )
            logger.warning(
                "Дублирующийся ключ токена WB %r (исходные ключи: %r и %r)",
                normalized_name, previous_name, original_name,
            )
            continue
        tokens[normalized_name] = token
        original_names[normalized_name] = original_name
    return tokens


@dataclass
class Setting:
    SHEET: str = os.getenv("SHEET")
    SPREADSHEET: str = os.getenv("SPREADSHEET")
    CREEDS_FILE_NAME: str = os.getenv("CREEDS_FILE_NAME")
    TOKENS_FILE_NAME: str = os.getenv("TOKENS_FILE_NAME")
    PC_SHEET: str = os.getenv("PC_SHEET")
    PC_SPREADSHEET: str = os.getenv("PC_SPREADSHEET")

    TELEGRAM_TOKEN: str = os.getenv("TELEGRAM_TOKEN")
    TELEGRAM_CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID")

    ONE_C_ROUTING_API:str = os.getenv("ONE_C_ROUTING_API")

class DBConfig:
    DB_USER: str = os.getenv('DB_USER')
    DB_PASSWORD: str = os.getenv('DB_PASSWORD')
    DB_NAME: str = os.getenv('DB_NAME')
    DB_HOST: str = os.getenv('DB_HOST')
    DB_PORT: int = os.getenv('DB_PORT')


DATABASE = DBConfig()
settings = Setting()
