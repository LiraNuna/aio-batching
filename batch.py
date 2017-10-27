import asyncio


class Batch:
    batches = {}
    loop = asyncio.get_event_loop()

    @staticmethod
    async def resolve_batch(batch, futures):
        batch_keys = [key for key, future in futures]
        print(f'>>> flush {batch.__name__}({len(batch_keys)}) {batch_keys}')

        future_results = batch.resolve_futures(batch_keys)
        if asyncio.iscoroutine(future_results):
            future_results = await future_results

        for key_future_pair, result in zip(futures, future_results):
            key, future = key_future_pair
            future.set_result(result)

    @staticmethod
    def schedule_batches():
        for batch in list(Batch.batches.keys()):
            Batch.loop.create_task(Batch.resolve_batch(batch, Batch.batches.pop(batch)))

    # Internal interface

    @staticmethod
    def resolve_futures(batch):
        raise NotImplemented()

    @classmethod
    def schedule(cls, key):
        if not Batch.batches:
            Batch.loop.call_later(0, Batch.schedule_batches)

        future = Batch.loop.create_future()
        Batch.batches.setdefault(cls, []).append((key, future))
        return future

    # External interface

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
    double = await DoubleBatch.gen(square)
    return double


async def triple_double(x):
    d1 = await DoubleBatch.gen(x)
    d2 = await DoubleBatch.gen(d1)
    d3 = await DoubleBatch.gen(d2)
    return d3


async def double_square_square_double(x):
    ds = await double_square(x)
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

