import json
from dataclasses import dataclass
from dotenv import load_dotenv
import os

load_dotenv()


def get_wb_tokens() -> dict:
    with open(settings.TOKENS_FILE_NAME, "r", encoding='utf-8') as file:
        tokens = json.load(file)
    return tokens


@dataclass
class Setting:
    SHEET: str = os.getenv("SHEET")
    SPREADSHEET: str = os.getenv("SPREADSHEET")
    CREEDS_FILE_NAME: str = os.getenv("CREEDS_FILE_NAME")
    TOKENS_FILE_NAME: str = os.getenv("TOKENS_FILE_NAME")


settings = Setting()
