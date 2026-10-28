import asyncio
import unittest
from unittest.mock import AsyncMock, Mock, patch

from APIGoogleSheet.googlesheet import GoogleSheet
from APIWildberries.content import ListOfCardsContent


class WithoutJSONStorageTests(unittest.TestCase):
    def test_cards_are_returned_without_file_access(self):
        response = Mock(status_code=200)
        response.json.return_value = {
            'cards': [{
                'nmID': 123,
                'dimensions': {'length': 10, 'width': 20, 'height': 30},
                'subjectName': 'Товар',
                'sizes': [{'skus': ['barcode']}],
                'vendorCode': 'wild1960',
            }],
            'cursor': {'total': 1},
        }
        client = ListOfCardsContent('test-token')
        with (
            patch('APIWildberries.content.requests.post', return_value=response),
            patch('builtins.open', side_effect=AssertionError('Unexpected file access')),
        ):
            cards = client.get_list_of_cards([123], limit=100, account='Вектор')
        self.assertEqual(cards[123]['Баркод'], 'barcode')
        self.assertEqual(cards[123]['vendor_code'], 'wild1960')
        self.assertEqual(cards[123]['account'], 'Вектор')

    def test_missing_cards_keep_placeholder_without_file_access(self):
        response = Mock(status_code=200)
        response.json.return_value = {'cards': [], 'cursor': {'total': 0}}
        client = ListOfCardsContent('test-token')
        with (
            patch('APIWildberries.content.requests.post', return_value=response),
            patch('builtins.open', side_effect=AssertionError('Unexpected file access')),
        ):
            cards = client.get_list_of_cards([123], limit=100, account='Вектор')
        self.assertEqual(cards[123]['vendor_code'], 'не найдено')

    def test_new_articles_use_postgresql_and_normalize_accounts_and_ids(self):
        db_context = AsyncMock()
        repository = Mock()
        repository.get_all_nm_ids = AsyncMock(return_value={
            '123': {'account': '\u00a0ВЕКТОР '},
            '456': {'account': 'Другой'},
        })
        with (
            patch('APIGoogleSheet.googlesheet.Database1', return_value=db_context),
            patch('APIGoogleSheet.googlesheet.ArticleTable', return_value=repository),
            patch('builtins.open', side_effect=AssertionError('Unexpected file access')),
        ):
            result = asyncio.run(GoogleSheet.check_new_nm_ids(' вектор ', ['123', 456, '789', 789]))
        self.assertEqual(result, [456, 789])
        repository.get_all_nm_ids.assert_awaited_once()

    def test_empty_article_list_does_not_open_database(self):
        with patch('APIGoogleSheet.googlesheet.Database1') as database:
            self.assertEqual(asyncio.run(GoogleSheet.check_new_nm_ids('Вектор', [])), [])
        database.assert_not_called()


if __name__ == '__main__':
    unittest.main()
