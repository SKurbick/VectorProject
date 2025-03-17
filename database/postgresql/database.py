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

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

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


class Database1:
    def __init__(self, user=DATABASE.DB_USER, password=DATABASE.DB_PASSWORD, database=DATABASE.DB_NAME,
                 host=DATABASE.DB_HOST, port=DATABASE.DB_PORT):
        self._user = user
        self._password = password
        self._database = database
        self._host = host
        self._port = port
        self._pool = None
        self._max_size = 90
        self._min_size = 30
        self._timeout = 300
        self._command_timeout = 250

    async def connect(self):
        self._pool = await asyncpg.create_pool(
            user=self._user,
            password=self._password,
            database=self._database,
            host=self._host,
            port=self._port,
            max_size=self._max_size,
            min_size=self._min_size,
            timeout=self._timeout,
            command_timeout=self._command_timeout
        )

    async def close(self):
        if self._pool:
            await self._pool.close()

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    @asynccontextmanager
    async def acquire(self):
        if not self._pool:
            raise RuntimeError("Database pool is not initialized")
        async with self._pool.acquire() as connection:
            yield connection

    async def fetch(self, query, *args):
        async with self.acquire() as connection:
            return await connection.fetch(query, *args)

    async def fetchrow(self, query, *args):
        async with self.acquire() as connection:
            return await connection.fetchrow(query, *args)

    async def execute(self, query, *args):
        async with self.acquire() as connection:
            return await connection.execute(query, *args)

    async def executemany(self, query, args):
        async with self.acquire() as connection:
            return await connection.executemany(query, args)

    @asynccontextmanager
    async def transaction(self):
        async with self.acquire() as connection:
            async with connection.transaction():
                yield

    async def copy_records_to_table(self, table_name, columns, records):
        async with self.acquire() as connection:
            await connection.copy_records_to_table(table_name, columns=columns, records=records)
