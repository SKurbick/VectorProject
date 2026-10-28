import asyncio
import unittest
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, patch

from database.postgresql.database import Database1
from database.postgresql.repositories.orders_by_federal_district import OrdersByFederalDistrict
from settings import DATABASE


class FakeConnection:
    def __init__(self, pool, number):
        self.pool = pool
        self.number = number

    @asynccontextmanager
    async def transaction(self):
        self.pool.events.append(('begin', self.number))
        try:
            yield
        except BaseException:
            self.pool.events.append(('rollback', self.number))
            raise
        else:
            self.pool.events.append(('commit', self.number))

    async def fetch(self, *args):
        return await self.execute(*args)

    async def fetchrow(self, *args):
        return await self.execute(*args)

    async def execute(self, query, *args):
        self.pool.events.append(('query', self.number, query))
        return self.number

    async def executemany(self, *args):
        return await self.execute(*args)

    async def copy_records_to_table(self, table_name, **kwargs):
        self.pool.events.append(('copy', self.number, table_name))


class FakePool:
    def __init__(self, max_size):
        self.semaphore = asyncio.Semaphore(max_size)
        self.events = []
        self.acquisitions = 0
        self.in_use = 0
        self.peak = 0
        self.closed = False
        self.terminated = False

    @asynccontextmanager
    async def acquire(self, timeout):
        await asyncio.wait_for(self.semaphore.acquire(), timeout)
        self.acquisitions += 1
        self.in_use += 1
        self.peak = max(self.peak, self.in_use)
        try:
            yield FakeConnection(self, self.acquisitions)
        finally:
            self.in_use -= 1
            self.semaphore.release()

    async def close(self):
        self.closed = True

    def terminate(self):
        self.terminated = True


class DatabasePoolTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.pools = []

        async def create_pool(**kwargs):
            await asyncio.sleep(0)
            pool = FakePool(kwargs['max_size'])
            self.pools.append(pool)
            return pool

        config = patch.multiple(
            DATABASE, DB_POOL_MIN_SIZE=1, DB_POOL_MAX_SIZE=2,
            DB_ACQUIRE_TIMEOUT=0.5, DB_CLOSE_TIMEOUT=0.05,
            DB_APPLICATION_NAME='VectorProject-test',
        )
        config.start()
        self.addCleanup(config.stop)
        factory = patch('database.postgresql.database.asyncpg.create_pool',
                        new_callable=AsyncMock, side_effect=create_pool)
        self.create_pool = factory.start()
        self.addCleanup(factory.stop)

    async def asyncTearDown(self):
        await Database1.close_pools()

    async def test_concurrent_wrappers_create_one_pool(self):
        wrappers = [Database1() for _ in range(10)]
        await asyncio.gather(*(db.connect() for db in wrappers))
        self.create_pool.assert_awaited_once()
        self.assertTrue(all(db._pool is self.pools[0] for db in wrappers))
        self.assertEqual(self.create_pool.call_args.kwargs['min_size'], 1)
        self.assertEqual(self.create_pool.call_args.kwargs['max_size'], 2)
        self.assertEqual(self.create_pool.call_args.kwargs['server_settings'],
                         {'application_name': 'VectorProject-test'})

    async def test_context_exit_keeps_shared_pool_until_shutdown(self):
        async with Database1() as db:
            await db.execute('first')
        self.assertFalse(self.pools[0].closed)
        async with Database1() as db:
            await db.execute('second')
        self.create_pool.assert_awaited_once()
        await Database1.close_pools()
        self.assertTrue(self.pools[0].closed)
        self.assertFalse(Database1._pools)

    async def test_failed_initialization_can_retry(self):
        normal_factory = self.create_pool.side_effect
        self.create_pool.side_effect = OSError('offline')
        with self.assertRaises(OSError):
            await Database1().connect()
        self.assertFalse(Database1._pools)
        self.create_pool.side_effect = normal_factory
        await Database1().connect()
        self.assertEqual(len(self.pools), 1)

    async def test_different_credentials_do_not_share_pool(self):
        await Database1(database='first').connect()
        await Database1(database='second').connect()
        self.assertEqual(len(self.pools), 2)

    async def test_transaction_pins_all_query_helpers_to_one_connection(self):
        async with Database1() as db:
            async with db.transaction() as connection:
                expected = connection.number
                self.assertEqual(await db.fetch('fetch'), expected)
                self.assertEqual(await db.fetchrow('fetchrow'), expected)
                self.assertEqual(await db.execute('execute'), expected)
                self.assertEqual(await db.executemany('batch', []), expected)
                await db.copy_records_to_table('temp', columns=[], records=[])
        self.assertEqual(self.pools[0].acquisitions, 1)
        self.assertEqual(self.pools[0].events[-1], ('commit', expected))

    async def test_exception_rolls_back_and_unpins_connection(self):
        async with Database1() as db:
            with self.assertRaisesRegex(ValueError, 'failed'):
                async with db.transaction():
                    await db.execute('write')
                    raise ValueError('failed')
            self.assertEqual(self.pools[0].events[-1], ('rollback', 1))
            self.assertEqual(await db.execute('after'), 2)
        self.assertEqual(self.pools[0].in_use, 0)

    async def test_nested_transaction_uses_same_connection(self):
        async with Database1() as db:
            async with db.transaction():
                with self.assertRaises(ValueError):
                    async with db.transaction():
                        await db.execute('nested')
                        raise ValueError('rollback savepoint')
                await db.execute('outer')
        self.assertEqual(self.pools[0].acquisitions, 1)
        self.assertEqual([event[:2] for event in self.pools[0].events],
                         [('begin', 1), ('begin', 1), ('query', 1),
                          ('rollback', 1), ('query', 1), ('commit', 1)])

    async def test_explicit_acquire_also_pins_helpers(self):
        async with Database1() as db:
            async with db.acquire() as connection:
                self.assertEqual(await db.execute('same'), connection.number)
        self.assertEqual(self.pools[0].acquisitions, 1)

    async def test_child_task_does_not_inherit_parent_connection(self):
        async with Database1() as db:
            async with db.transaction():
                parent = await db.execute('parent')
                child = await asyncio.create_task(db.execute('child'))
                self.assertNotEqual(parent, child)
                self.assertEqual(await db.execute('parent again'), parent)
        self.assertEqual(self.pools[0].acquisitions, 2)

    async def test_parallel_tasks_use_separate_connections_with_pool_limit(self):
        async with Database1() as db:
            async def work():
                async with db.transaction():
                    first = await db.execute('first')
                    await asyncio.sleep(0.001)
                    self.assertEqual(await db.execute('second'), first)
                    return first
            ids = await asyncio.gather(*(work() for _ in range(8)))
        self.assertEqual(len(set(ids)), 8)
        self.assertEqual(self.pools[0].peak, 2)
        self.assertEqual(self.pools[0].in_use, 0)

    async def test_cancelled_transaction_rolls_back_and_releases(self):
        async with Database1() as db:
            ready = asyncio.Event()
            async def work():
                async with db.transaction():
                    await db.execute('write')
                    ready.set()
                    await asyncio.Event().wait()
            task = asyncio.create_task(work())
            await ready.wait()
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            self.assertEqual(self.pools[0].events[-1], ('rollback', 1))
            self.assertEqual(self.pools[0].in_use, 0)

    async def test_temporary_table_operation_is_one_transaction(self):
        async with Database1() as db:
            await OrdersByFederalDistrict(db).add_orders_data([])
        self.assertEqual(self.pools[0].acquisitions, 1)
        self.assertEqual([event[0] for event in self.pools[0].events],
                         ['begin', 'query', 'copy', 'query', 'query', 'commit'])

    async def test_shutdown_timeout_terminates_pool(self):
        await Database1().connect()
        async def stuck_close():
            await asyncio.Event().wait()
        self.pools[0].close = stuck_close
        with self.assertRaises(TimeoutError):
            await Database1.close_pools()
        self.assertTrue(self.pools[0].terminated)
        self.assertFalse(Database1._pools)


if __name__ == '__main__':
    unittest.main()
