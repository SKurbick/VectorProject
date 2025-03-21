import datetime
import asyncpg
from logger import app_logger as logger


class CostPriceTable:
    def __init__(self, db):
        self.db = db

    async def get_current_data(self):
        query = '''
        SELECT DISTINCT ON (local_vendor_code) 
        *
        FROM cost_price
        ORDER BY local_vendor_code, created_at DESC;'''

        result = await self.db.fetch(query=query)
        return result

    async def insert_new_data(self, data):
        # текущий день
        date_value = datetime.datetime.today().date()
        # Разделение данных на записи с числовым значением и строковым значением
        numeric_records = []
        text_records = []
        for local_vendor_code, value in data.items():
            if str(value).isdigit() is False and value != '':
                status_by_lvc = value
                purchase_price = None
                text_records.append((local_vendor_code, date_value, status_by_lvc))
            else:
                try:
                    purchase_price = int(value)
                    status_by_lvc = None
                    numeric_records.append((local_vendor_code, date_value, purchase_price))
                except ValueError:
                    logger.error(f"значение из таблицы Сопост {value}")
                    raise ValueError("Значение должно быть строкой или числом")

        # Создание временных таблиц для числовых и текстовых записей
        await self.db.execute('''
            CREATE TEMPORARY TABLE temp_numeric (
                local_vendor_code TEXT NOT NULL,
                date DATE NOT NULL,
                purchase_price INTEGER NOT NULL
            )
        ''')

        await self.db.execute('''
            CREATE TEMPORARY TABLE temp_text (
                local_vendor_code TEXT NOT NULL,
                date DATE NOT NULL,
                status_by_lvc TEXT NOT NULL
            )
        ''')

        # Вставка данных во временные таблицы
        if numeric_records:
            await self.db.copy_records_to_table('temp_numeric', records=numeric_records)

        if text_records:
            await self.db.copy_records_to_table('temp_text', records=text_records)

        # Вставка или обновление числовых записей
        if numeric_records:
            await self.db.execute('''
                INSERT INTO cost_price (local_vendor_code, date, purchase_price, created_at, last_check_datetime)
                SELECT local_vendor_code, date, purchase_price, NOW(), NOW()
                FROM temp_numeric
                ON CONFLICT (local_vendor_code, date, purchase_price) DO UPDATE SET
                    last_check_datetime = EXCLUDED.last_check_datetime
            ''')

        # Вставка или обновление текстовых записей
        if text_records:
            await self.db.execute('''
                INSERT INTO cost_price (local_vendor_code, date, status_by_lvc, created_at, last_check_datetime)
                SELECT local_vendor_code, date, status_by_lvc, NOW(), NOW()
                FROM temp_text
                ON CONFLICT (local_vendor_code, date, status_by_lvc) DO UPDATE SET
                    last_check_datetime = EXCLUDED.last_check_datetime
            ''')

        # Удаление временных таблиц
        await self.db.execute('DROP TABLE temp_numeric')
        await self.db.execute('DROP TABLE temp_text')

