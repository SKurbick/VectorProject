import asyncio
from contextlib import asynccontextmanager
from contextvars import ContextVar

import asyncpg
from settings import DATABASE
from logger import app_logger as logger


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
    """Shared pools per event loop; connections are pinned within acquire/transaction."""

    _pools = {}
    _pool_locks = {}

    def __init__(self, user=DATABASE.DB_USER, password=DATABASE.DB_PASSWORD, database=DATABASE.DB_NAME,
                 host=DATABASE.DB_HOST, port=DATABASE.DB_PORT):
        self._user = user
        self._password = password
        self._database = database
        self._host = host
        self._port = port
        self._pool = None
        self._connection_context = ContextVar('database_connection', default=None)

    async def connect(self):
        key = (
            asyncio.get_running_loop(), self._user, self._password,
            self._database, self._host, self._port,
        )
        lock = self._pool_locks.setdefault(key, asyncio.Lock())
        async with lock:
            if key not in self._pools:
                logger.info("Creating shared PostgreSQL pool (min={}, max={})",
                            DATABASE.DB_POOL_MIN_SIZE, DATABASE.DB_POOL_MAX_SIZE)
                self._pools[key] = await asyncpg.create_pool(
                    user=self._user,
                    password=self._password,
                    database=self._database,
                    host=self._host,
                    port=self._port,
                    min_size=DATABASE.DB_POOL_MIN_SIZE,
                    max_size=DATABASE.DB_POOL_MAX_SIZE,
                    timeout=DATABASE.DB_CONNECT_TIMEOUT,
                    command_timeout=DATABASE.DB_COMMAND_TIMEOUT,
                    server_settings={'application_name': DATABASE.DB_APPLICATION_NAME},
                )
            self._pool = self._pools[key]

    async def close(self):
        """Detach this wrapper; the application owns the shared pool."""
        self._pool = None

    @classmethod
    async def close_pools(cls):
        """Close pools owned by the current loop when the application stops."""
        loop = asyncio.get_running_loop()
        for key in list(cls._pool_locks):
            if key[0] is not loop:
                continue
            async with cls._pool_locks[key]:
                pool = cls._pools.pop(key, None)
                try:
                    if pool is not None:
                        await asyncio.wait_for(pool.close(), timeout=DATABASE.DB_CLOSE_TIMEOUT)
                except (TimeoutError, asyncio.CancelledError):
                    if pool is not None:
                        pool.terminate()
                    raise
                finally:
                    cls._pool_locks.pop(key, None)

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    @asynccontextmanager
    async def acquire(self):
        bound = self._connection_context.get()
        if bound is not None and bound[0] is asyncio.current_task():
            yield bound[1]
            return
        if not self._pool:
            raise RuntimeError("Database pool is not initialized")
        async with self._pool.acquire(timeout=DATABASE.DB_ACQUIRE_TIMEOUT) as connection:
            token = self._connection_context.set((asyncio.current_task(), connection))
            try:
                yield connection
            finally:
                self._connection_context.reset(token)

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
                yield connection

    async def copy_records_to_table(self, table_name, columns, records):
        async with self.acquire() as connection:
            await connection.copy_records_to_table(table_name, columns=columns, records=records)
