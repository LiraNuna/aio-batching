import asyncio


def sync_wait_for(future_or_coroutine):
    current_loop = asyncio.get_event_loop()
    if not current_loop.is_running():
        return current_loop.run_until_complete(future_or_coroutine)

    asyncio.events._set_running_loop(None)

    new_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(new_loop)
    result = new_loop.run_until_complete(future_or_coroutine)

    asyncio.set_event_loop(current_loop)
    asyncio.events._set_running_loop(current_loop)

    return result


class Batch:
    batches = {}
    schedules = set()

    @classmethod
    def schedule(cls, key):
        loop = asyncio.get_event_loop()
        if loop not in cls.schedules:
            loop.call_later(0, cls.schedule_batches)
            cls.schedules.add(loop)

        future = loop.create_future()
        cls.batches.setdefault(cls, []).append((key, future))

        return future

    @classmethod
    async def resolve_batch(cls, batch, futures):
        batch_keys = [key for key, future in futures]
        print(f'>>> flush {batch.__name__}({len(batch_keys)}) {batch_keys}')

        future_results = batch.resolve_futures(batch_keys)
        if asyncio.iscoroutine(future_results):
            future_results = await future_results

        for key_future_pair, result in zip(futures, future_results):
            key, future = key_future_pair
            future.set_result(result)

    @classmethod
    def schedule_batches(cls):
        loop = asyncio.get_event_loop()
        for batch in list(cls.batches.keys()):
            loop.create_task(cls.resolve_batch(batch, cls.batches.pop(batch)))

        cls.schedules.remove(loop)

    @staticmethod
    def resolve_futures(batch):
        raise NotImplemented()

    @classmethod
    async def gen(cls, key):
        return await cls.schedule(key)

    @classmethod
    async def genv(cls, keys):
        return await asyncio.gather(*[cls.gen(key) for key in keys])


class DoubleBatch(Batch):
    @staticmethod
    async def resolve_futures(batch):
        await asyncio.sleep(1)
        return [x+x for x in batch]


class SquareBatch(Batch):
    @staticmethod
    async def resolve_futures(batch):
        await asyncio.sleep(1)
        return [x*x for x in batch]


async def double_square(x):
    double = await DoubleBatch.gen(x)
    square = await SquareBatch.gen(double)
    return square


async def square_double(x):
    square = await SquareBatch.gen(x)
    print('sq', sync_wait_for(SquareBatch.gen(x)))
    double = await DoubleBatch.gen(square)
    return double


async def triple_double(x):
    d1 = await DoubleBatch.gen(x)
    d2 = await DoubleBatch.gen(d1)
    d3 = await DoubleBatch.gen(d2)
    return d3


async def double_square_square_double(x):
    ds = await double_square(x)
    print('dsds', sync_wait_for(square_double(x)))
    sd = await square_double(ds)
    return sd


async def root():
    x = await asyncio.gather(
        square_double(10),
        square_double(20),
        square_double(30),
        double_square(8),
        double_square(9),
        double_square(10),
        DoubleBatch.genv([1, 2, 3, 4, 5, 6]),
        SquareBatch.genv([-1, -2, -3, -4, -5, -6]),
        triple_double(100),
        triple_double(200),
        triple_double(300),
        double_square_square_double(123),
        double_square_square_double(456),
        double_square_square_double(789),
    )

    assert x == [200, 800, 1800, 256, 324, 400, [2, 4, 6, 8, 10, 12], [1, 4, 9, 16, 25, 36], 800, 1600, 2400, 7324372512, 1383596163072, 12401036654112]
    print(x)


loop = asyncio.get_event_loop()
loop.run_until_complete(root())

