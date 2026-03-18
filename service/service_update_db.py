import datetime
import asyncio
from typing import List, Dict

from database.postgresql.database import Database, Database1
from database.postgresql.repositories.card_data import CardData
from database.postgresql.repositories.article import ArticleTable
from database.postgresql.repositories.cost_price import CostPriceTable
from database.postgresql.models.cost_price import CostPriceDBContainer
from database.postgresql.repositories.unit_economics import UnitEconomicsTable
from utils import merge_dicts, calculate_sum_for_logistic

from APIWildberries.tariffs import CommissionTariffs
from APIWildberries.content import ListOfCardsContent
from APIWildberries.prices_and_discounts import ListOfGoodsPricesAndDiscounts  #

from service.gs_service import PCGoogleSheet

from settings import get_wb_tokens, Setting
from logger import app_logger as logger


class Service:

    async def actualize_card_data_in_db(self, account_articles: Dict[str, int]):
        """Обновление состояния данных карточек по всем кабинетам"""
        logger.info("Обновление состояния данных карточек по всем кабинетам в бд")
        time_start = datetime.datetime.now()
        tasks = []
        for account, nm_ids in account_articles.items():
            tokens = get_wb_tokens()
            token = tokens[account.capitalize()]
            task = asyncio.create_task(self.get_actually_data_by_account(
                token=token,
                account=account,
                articles=nm_ids
            ))
            tasks.append(task)

        together_results = await asyncio.gather(*tasks)

        to_update_card_data = []
        to_update_article = []
        to_update_unit_economics = []
        current_time = datetime.datetime.now()
        async with Database1() as db:
            async with db.acquire() as conn:
                cost_price = await CostPriceTable(db=conn).get_current_data()
                cost_price_model = CostPriceDBContainer(cost_price)  # получение закупочной стоимости по wild
            for results in together_results:
                for article, card_data in results.items():
                    try:
                        to_update_article.append(
                            (article, card_data['account'], card_data['local_vendor_code'], card_data['vendor_code']))
                        to_update_card_data.append(
                            (article, card_data.get('barcode', None), card_data.get('commission_wb', None),
                             card_data.get('Скидка %', None),
                             card_data.get('height', None), card_data.get('length', None),
                             card_data.get('logistic_from_wb_wh_to_opp', None), card_data.get('photo_link', None),
                             card_data.get('Цена на WB без скидки', None),
                             card_data.get('subject_name', None), card_data.get('width', None),
                             current_time)
                        )
                        cost_price = cost_price_model.local_vendor_code.get(card_data['local_vendor_code'],
                                                                            None)  # получение закупочной стоимости
                        percent_by_tax = 8  # 💩плохая реализация #todo перенести данные в бд, вставка от запроса api, вставка значения по умолчанию
                        if cost_price is not None:
                            cost_price = cost_price.purchase_price
                        to_update_unit_economics.append(
                            (article, card_data.get('commission_wb', None),
                             card_data.get('Скидка %', None),
                             card_data.get('logistic_from_wb_wh_to_opp', None),
                             card_data.get('Цена на WB без скидки', None),
                             cost_price,
                             percent_by_tax,
                             current_time)
                        )
                    except KeyError as e:
                        logger.error(f"Error in -func actualize_card_data_in_db {article} : {e}")
        async with Database1() as db:
            async with db.acquire() as connection:
                async with connection.transaction():
                    card_data_db = CardData(db=connection)
                    article = ArticleTable(db=connection)
                    unit_economics = UnitEconomicsTable(db=connection)
                    await article.update_article_data(data=to_update_article)
                    await card_data_db.update_card_data(data=to_update_card_data)
                    await unit_economics.update_data(data=to_update_unit_economics)
            logger.info(
                f"Обновление состояния данных карточек по всем кабинетам в бд завершено. Время выполнения: {datetime.datetime.now() - time_start}")

    async def get_actually_data_by_account(self, account, token, articles):
        """Получение данных по кабинету:
              article_id, account, length, width, height, barcode, local_vendor_code, vendor_code,
              skus, photo_link, logistic_from_wb_wh_to_opp, commission_wb, price, discount, subject_name,
        """
        wb_api_content = ListOfCardsContent(token=token)
        wb_api_price_and_discount = ListOfGoodsPricesAndDiscounts(token=token)
        commission_traffics = CommissionTariffs(token=token)

        task1 = wb_api_content.get_list_of_cards_async(nm_ids_list=articles, limit=100, account=account)
        # task1 = wb_api_content.get_all_list_of_cards_async(account=account) # получение всех карточек
        task2 = wb_api_price_and_discount.get_log_for_nm_ids_async(filter_nm_ids=articles, account=account)
        task3 = commission_traffics.get_tariffs_box_from_marketplace_async()

        card_from_nm_ids_filter, goods_nm_ids, current_tariffs_data = await asyncio.gather(task1, task2, task3)

        merge_json_data = merge_dicts(goods_nm_ids, card_from_nm_ids_filter)

        subject_names = set()  # итог всех полученных с карточек предметов
        for article, data in merge_json_data.items():
            if "local_vendor_code" in data and data["local_vendor_code"] != "не найдено":
                subject_names.add(data["subject_name"])  # собираем множество с предметами
                try:
                    result_log_value = calculate_sum_for_logistic(
                        # на лету считаем "Логистика от склада WB до ПВЗ"
                        for_one_liter=float(current_tariffs_data["boxDeliveryBase"].replace(',', '.')),
                        next_liters=float(current_tariffs_data["boxDeliveryLiter"].replace(',', '.')),
                        height=int(data['height']),
                        length=int(data['length']),
                        width=int(data['width']), )
                    # добавляем результат вычислений в итоговые данные
                    data["logistic_from_wb_wh_to_opp"] = result_log_value
                except Exception as e:
                    logger.info(f"ERROR by calculate_sum_for_logistic : {str(e)}")
            else:
                logger.info(f"article : {article}, data : {data}")
        # получение комиссии WB
        subject_commissions = await commission_traffics.get_commission_on_subject_async(subject_names=subject_names)
        for card in merge_json_data.values():
            if subject_commissions is not None:
                for sc in subject_commissions.items():
                    if "subject_name" in card and sc[0] == card["subject_name"]:
                        card["commission_wb"] = sc[1]
        return merge_json_data
