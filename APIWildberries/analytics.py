import datetime
import time
from pprint import pprint

import requests


class Analytics:
    pass


class AnalyticsNMReport:
    def __init__(self, token):
        self.token = token
        self.url = "https://seller-analytics-api.wildberries.ru/api/v2/nm-report/{}"
        self.headers = {
            "Authorization": self.token,
            'Content-Type': 'application/json'
        }

    def get_last_days_revenue(self, nm_ids: list,
                              begin_date: datetime,
                              end_date: datetime,
                              step: int = 20):
        """По методу есть ограничения на 3 запроса в минуту и в 20 nmID за запрос.
            По умолчанию передаются даты последнего (вчерашнего) дня
        """
        url = self.url.format("detail/history")
        result_data = {}
        for start in range(0, len(nm_ids), step):
            nm_ids_part = nm_ids[start: start + step]

            json_data = {
                "nmIDs": nm_ids_part,
                "period": {
                    "begin": str(begin_date),
                    "end": str(end_date)
                },
                "timezone": "Europe/Moscow",
                "aggregationLevel": "day"
            }
            response = requests.post(url=url, headers=self.headers, json=json_data)

            # обработка ограничения API WB на количество запросов
            if response.status_code > 400:
                for _ in range(10):
                    print("[INFO] просмотр выручки. Попал в исключение. Ожидание 75 с.")
                    time.sleep(75)
                    response = requests.post(url=url, headers=self.headers, json=json_data)
                    if response.status_code < 400:
                        break
            for data in response.json()["data"]:

                nm_id_from_data = str(data["nmID"])
                revenue_by_dates = {}
                for nm_id_history in data["history"]:
                    date_object = datetime.datetime.strptime(nm_id_history["dt"], "%Y-%m-%d")
                    output_date = date_object.strftime("%d-%m-%Y")

                    revenue_by_dates[output_date] = nm_id_history["ordersSumRub"]

                result_data[nm_id_from_data] = revenue_by_dates

        return result_data
