import asyncpg
from contextlib import asynccontextmanager
from settings import DATABASE


class Database:
    def __init__(self, user=DATABASE.DB_USER, password=DATABASE.DB_PASSWORD, database=DATABASE.DB_NAME,
                 host=DATABASE.DB_HOST, port=DATABASE.DB_PORT):
        self._user = user
        self._password = password
        self._database = database
        self._host = host
        self._port = port
        self._connection = None

    async def connect(self):
        self._connection = await asyncpg.connect(
            user=self._user,
            password=self._password,
            database=self._database,
            host=self._host,
            port=self._port
        )

    async def close(self):
        if self._connection:
            await self._connection.close()

    async def fetch(self, query, *args):
        return await self._connection.fetch(query, *args)

    async def fetchrow(self, query, *args):
        return await self._connection.fetchrow(query, *args)

    async def execute(self, query, *args):
        return await self._connection.execute(query, *args)

    async def executemany(self, query, args):
        return await self._connection.executemany(query, args)

    @asynccontextmanager
    async def transaction(self):
        if not self._connection:
            raise RuntimeError(f"Не удалось создать транзакцию {self._database}")
        async with self._connection.transaction():
            yield
