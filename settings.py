import json


def get_wb_tokens() -> dict:
    with open("tokens.json", "r", encoding='utf-8') as file:
        tokens = json.load(file)
    return tokens


def get_creds_googlesheets() -> dict:
    with open("creeds.json", "r", encoding='utf-8') as file:
        creds = json.load(file)
    return creds


def get_nm_ids_in_db(account):
    with open("database.json", "r", encoding='utf-8') as file:
        nm_ids = json.load(file)
        if account not in nm_ids["account_nm_ids"]:
            return []
    return nm_ids["account_nm_ids"][account]


def add_nm_ids_in_db(new_nm_ids):
    """Добавление артикулов в БД"""
    with open('database.json', 'r+') as file:
        # Загрузите данные из файла
        data = json.load(file)
        data['nm_ids'].extend(new_nm_ids)
        file.seek(0)
        json.dump(data, file, indent=4,ensure_ascii=False)
        file.truncate()


def get_revenue_for_nm_ids():
    with open("database.json", "r", encoding='utf-8') as file:
        nm_ids = json.load(file)
    return nm_ids["revenue_result"]
