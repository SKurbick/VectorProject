import asyncio
import json
import time

import aiohttp
import requests
from requests import Response, Session
from typing import Any, Dict, Optional
from logger import app_logger as logger


class HttpClient:

    def __init__(self, timeout: int = 10, retries: int = 1, delay: int = 0):
        """Инициализирует HttpClient.
        Args:
            timeout: Таймаут для каждого запроса в секундах.
            retries: Количество попыток перед тем, как считать запрос неудачным.
        """
        self.timeout: int = timeout
        self.retries: int = retries
        self.session: Session = Session()
        self.delay: int = delay

    def _make_request(self, method: str, url: str, **kwargs: object) -> str | None:
        """Выполняет HTTP-запрос с повторными попытками.
        Args:
            method: HTTP-метод (например, "GET", "POST").
            url: URL-адрес для запроса.
            **kwargs: Дополнительные аргументы для передачи в `requests.Session.request`.
        Returns:
            Объект ответа, если запрос успешен, иначе None.
        """
        for attempt in range(self.retries):
            try:
                response: Response = self.session.request(method, url, timeout=self.timeout, **kwargs)
                response.raise_for_status()
                return response.text
            except requests.RequestException as e:
                logger.warning(f"Попытка {attempt + 1}: Ошибка во время {method} {url} - {e}")
                time.sleep(self.delay)
        return None

    def request(self, method: str, url: str, params: Optional[Dict[str, Any]] = None,
                json: Optional[Dict[str, Any]] = None, data: Optional[Dict[str, Any]] = None,
                headers: Optional[Dict[str, str]] = None) -> Optional[str]:
        """Выполняет HTTP-запрос с указанным методом, URL и параметрами.
        Args:
            method: HTTP-метод (например, "GET", "POST").
            url: URL-адрес для запроса.
            params: Параметры запроса.
            json: JSON-данные для отправки в теле запроса.
            data: Данные для отправки в теле запроса.
            headers: HTTP-заголовки для включения в запрос.
        Returns:
            Содержимое ответа, если запрос успешен, иначе None.
        """
        return self._make_request(method, url, params=params, json=json, data=data, headers=headers)

    def get(self, url: str, params: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, str]] = None) -> \
            Optional[str]:
        """Выполняет GET-запрос по указанному URL.
        Args:
            url: URL-адрес для запроса.
            params: Параметры запроса.
            headers: HTTP-заголовки для включения в запрос.
        Returns:
            Содержимое ответа, если запрос успешен, иначе None.
        """
        return self.request("GET", url, params=params, headers=headers)

    def post(self, url: str, json: Optional[Dict[str, Any]] = None, data: Optional[Dict[str, Any]] = None,
             headers: Optional[Dict[str, str]] = None) -> Optional[str]:
        """Выполняет POST-запрос по-указанному URL.
        Args:
            url: URL-адрес для запроса.
            json: JSON-данные для отправки в теле запроса.
            data: Данные для отправки в теле запроса.
            headers: HTTP-заголовки для включения в запрос.
        Returns:
            Содержимое ответа, если запрос успешен, иначе None.
        """
        return self.request("POST", url, json=json, data=data, headers=headers)

    def put(self, url: str, json: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, str]] = None):
        """Выполняет PUT-запрос по указанному URL.
        Args:
            url: URL-адрес для запроса.
            json: JSON-данные для отправки в теле запроса.
            headers: HTTP-заголовки для включения в запрос.
        Returns:
            Содержимое ответа, если запрос успешен, иначе None.
        """
        return self.request("PUT", url, json=json, headers=headers)

    def delete(self, url: str, headers: Optional[Dict[str, str]] = None):
        """Выполняет DELETE-запрос по указанному URL.
        Args:
            url: URL-адрес для запроса.
            headers: HTTP-заголовки для включения в запрос.
        Returns:
            Содержимое ответа, если запрос успешен, иначе None.
        """
        return self.request("DELETE", url, headers=headers)


class AsyncHttpClient:

    def __init__(self, timeout: int = 10, retries: int = 1, delay: int = 0):
        """Инициализирует AsyncHttpClient.
        Args:
            timeout: Таймаут для каждого запроса в секундах.
            retries: Количество попыток перед тем, как считать запрос неудачным.
        """
        self.timeout: int = timeout
        self.retries: int = retries
        self.delay: int = delay

    async def _make_request(self, method: str, url: str, **kwargs: object) -> Optional[str]:
        """Выполняет асинхронный HTTP-запрос с повторными попытками.
        Args:
            method: HTTP-метод (например, "GET", "POST").
            url: URL-адрес для запроса.
            **kwargs: Дополнительные аргументы для передачи в `aiohttp.ClientSession.request`.
        Returns:
            Текст ответа, если запрос успешен, иначе None.
        """
        for attempt in range(self.retries):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.request(method, url, timeout=self.timeout,ssl=False, **kwargs) as response:
                        response.raise_for_status()
                        return await response.text()
            except (aiohttp.ClientError, aiohttp.ClientConnectionError) as e:
                logger.warning(f"Попытка {attempt + 1}: Ошибка во время {method} {url} - {e}")
                await asyncio.sleep(self.delay)
        return None

    async def request(self, method: str, url: str, params: Optional[Dict[str, Any]] = None,
                      json: Optional[Dict[str, Any]] = None, data: Optional[Dict[str, Any]] = None,
                      headers: Optional[Dict[str, str]] = None) -> Optional[str]:
        """Выполняет асинхронный HTTP-запрос с указанным методом, URL и параметрами.
        Args:
            method: HTTP-метод (например, "GET", "POST").
            url: URL-адрес для запроса.
            params: Параметры запроса.
            json: JSON-данные для отправки в теле запроса.
            data: Данные для отправки в теле запроса.
            headers: HTTP-заголовки для включения в запрос.
        Returns:
            Текст ответа, если запрос успешен, иначе None.
        """
        return await self._make_request(method, url, params=params, json=json, data=data, headers=headers)

    async def get(self, url: str, params: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, str]] = None) \
            -> Optional[str]:
        """Выполняет асинхронный GET-запрос по указанному URL.
        Args:
            url: URL-адрес для запроса.
            params: Параметры запроса.
            headers: HTTP-заголовки для включения в запрос.
        Returns:
            Текст ответа, если запрос успешен, иначе None.
        """
        return await self.request("GET", url, params=params, headers=headers)

    async def post(self, url: str, json: Optional[Dict[str, Any]] = None, data: Optional[Dict[str, Any]] = None,
                   headers: Optional[Dict[str, str]] = None) -> Optional[str]:
        """Выполняет асинхронный POST-запрос по-указанному URL.
        Args:
            url: URL-адрес для запроса.
            json: JSON-данные для отправки в теле запроса.
            data: Данные для отправки в теле запроса.
            headers: HTTP-заголовки для включения в запрос.
        Returns:
            Текст ответа, если запрос успешен, иначе None.
        """
        return await self.request("POST", url, json=json, data=data, headers=headers)

    async def put(self, url: str, json: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, str]] = None) -> \
            Optional[str]:
        """Выполняет асинхронный PUT-запрос по указанному URL.
        Args:
            url: URL-адрес для запроса.
            json: JSON-данные для отправки в теле запроса.
            headers: HTTP-заголовки для включения в запрос.
        Returns:
            Текст ответа, если запрос успешен, иначе None.
        """
        return await self.request("PUT", url, json=json, headers=headers)

    async def delete(self, url: str, headers: Optional[Dict[str, str]] = None) -> Optional[str]:
        """Выполняет асинхронный DELETE-запрос по указанному URL.
        Args:
            url: URL-адрес для запроса.
            headers: HTTP-заголовки для включения в запрос.
        Returns:
            Текст ответа, если запрос успешен, иначе None.
        """
        return await self.request("DELETE", url, headers=headers)


def parse_json(response_text: str) -> dict:
    """Преобразует строку ответа в JSON или выбрасывает исключение.
    Args:
        response_text: Строка ответа от сервера.
    Returns:
        Распарсенный JSON в виде словаря.
    Raises:
        ValueError: Если строка не является корректным JSON.
    """
    try:
        return json.loads(response_text)
    except json.JSONDecodeError as e:
        raise ValueError(f"Ошибка парсинга JSON: {e}") from e
