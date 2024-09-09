import json


class DatabaseJson:
    def __init__(self, token):
        self.database_file = "database.json"
        self.gs_token = "creds.json"
        self.wb_api_token = token

    def get_wb_tokens(self, ) -> dict:
        with open(self.wb_api_token, "r", encoding='utf-8') as file:
            tokens = json.load(file)
        return tokens

    def get_creds_googlesheets(self, ) -> dict:
        with open(self.gs_token, "r", encoding='utf-8') as file:
            creds = json.load(file)
        return creds

    def get_nm_ids_in_db(self, ):
        with open(self.database_file, "r", encoding='utf-8') as file:
            nm_ids = json.load(file)
        return nm_ids["nm_ids"]

    def add_new_nm_ids_in_db(self, new_nm_ids):
        with open(self.database_file, 'r+') as file:
            # Загрузите данные из файла
            data = json.load(file)
            data['nm_ids'].extend(new_nm_ids)
            file.seek(0)
            json.dump(data, file, indent=4)
            file.truncate()

    def get_revenue_for_nm_ids(self, ):
        with open(self.database_file, "r", encoding='utf-8') as file:
            nm_ids = json.load(file)
        return nm_ids["revenue_result"]

    def add_orders_data(self, revenue_data: dict):  # добавление или обновление выручки по артикулам и дням в бд
        with open(self.database_file, 'r+') as file:
            # Загрузите данные из файла
            database = json.load(file)
            for nm_id in revenue_data:
                if nm_id in database["revenue_result"]:  # если артикул есть, то данные будут обновленны или дополненны
                    database["revenue_result"][nm_id].update(revenue_data[nm_id])
                else:  # если артикула нет, то будет добавлен с актуальными данными
                    database["revenue_result"].update({nm_id: revenue_data[nm_id]})
            file.seek(0)
            json.dump(database, file, indent=4)
            file.truncate()

    def get_data_for_nm_ids(self):
        with open(self.database_file, "r", encoding='utf-8') as file:
            nm_ids = json.load(file)
        return nm_ids["nm_ids_data"]

    def add_data_for_nm_ids(self, nm_ids_data: dict):
        with open(self.database_file, 'r+') as file:
            # Загрузите данные из файла
            database = json.load(file)
            # добавляет данные по артикулу в БД
            for nm_id in nm_ids_data:
                database["nm_ids_data"].update({nm_id: nm_ids_data[nm_id]})
            file.seek(0)
            json.dump(database, file, indent=4)
            file.truncate()
