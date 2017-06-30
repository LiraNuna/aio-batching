import asyncio


class Batch:
    # Global for all batches
    batches = []
    handles = []
    loop = asyncio.get_event_loop()

    # Per-batch instances
    futures = []

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        cls.futures = []
        cls.batches.append(cls)

    @classmethod
    def schedule(cls):
        cls.handles.append(cls.loop.call_later(0, cls.scheduler))

    @classmethod
    def scheduler(cls):
        cls.handles.pop()
        if cls.handles:
            return

        cls.batches = sorted(cls.batches, key=lambda batch: len(batch.futures))

        current_batch = cls.batches.pop()
        if current_batch.futures:
            batch_keys = [key for key, future in current_batch.futures]
            future_results = current_batch.resolve_futures(batch_keys)
            print(f'>>> flush {current_batch.__name__}({len(batch_keys)}) {batch_keys}')
            for key_future_pair, result in zip(current_batch.futures, future_results):
                key, future = key_future_pair
                future.set_result(result)

            cls.schedule()

        current_batch.futures = []
        cls.batches.insert(0, current_batch)

    @staticmethod
    def resolve_futures(batch):
        raise NotImplemented()

    @classmethod
    async def gen(cls, key):
        future = cls.loop.create_future()
        cls.futures.append((key, future))

        cls.schedule()
        return await future

    @classmethod
    async def genv(cls, keys):
        return await asyncio.gather(*[cls.gen(key) for key in keys])


class DoubleBatch(Batch):
    @staticmethod
    def resolve_futures(batch):
        return [x+x for x in batch]


class SquareBatch(Batch):
    @staticmethod
    def resolve_futures(batch):
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

