import datetime


class InventoryTurnoverByRegTable:
    def __init__(self, db):
        self.db = db

    async def update_stock_balances(self, date_str, only_stocks_data, supply_data):
        date = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
        records_base = []
        records_with_supply = []

        for article, data in only_stocks_data.items():
            for key in data:
                if key == 'barcode':
                    continue
                records_base.append(
                    (article,int(data['barcode']), date, data[key], key)
                )
        if supply_data:
            for article, district_data in supply_data.items():
                for district, data in district_data.items():
                    records_with_supply.append(
                        (article, int(data['barcode']), date, data['quantity'], district, data['supply_qty'], data['supply_count'])
                    )
        await self.db.copy_records_to_table(
            "inventory_turnover_by_reg",
            columns=["article_id", "barcode", "date", "quantity", "federal_district"],
            records=records_base
        )

        if records_with_supply:
            await self.db.copy_records_to_table(
                "inventory_turnover_by_reg",
                columns=["article_id", "barcode", "date", "quantity", "federal_district", "supply_qty", "supply_count"],
                records=records_with_supply
            )