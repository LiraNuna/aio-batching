import asyncio


class Batch:
    # Global for all batches
    batches = []
    loop = asyncio.get_event_loop()

    # Per-batch instances
    futures = []
    event = asyncio.Event()

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        cls.futures = []
        cls.event = asyncio.Event()
        cls.batches.append(cls)

    @classmethod
    def scheduler(cls):
        cls.batches = sorted(cls.batches, key=lambda batch: len(batch.futures))
        current_batch = cls.batches.pop()

        if current_batch.futures:
            batch_keys = [key for key, future in current_batch.futures]
            future_results = current_batch.resolve_futures(batch_keys)
            for key_future_pair, result in zip(current_batch.futures, future_results):
                key, future = key_future_pair
                future.set_result(result)

        current_batch.event.set()
        current_batch.futures = []
        cls.batches.insert(0, current_batch)

    @staticmethod
    def resolve_futures(batch):
        raise NotImplemented()

    @classmethod
    async def gen(cls, key):
        future = cls.loop.create_future()
        cls.futures.append((key, future))

        cls.event.clear()
        cls.loop.call_soon(cls.scheduler)
        await cls.event.wait()

        return future.result()

    @classmethod
    async def genv(cls, keys):
        return [await cls.gen(key) for key in keys]


class DoubleBatch(Batch):
    @staticmethod
    def resolve_futures(batch):
        return [x+x for x in batch]


class SquareBatch(Batch):
    @staticmethod
    def resolve_futures(batch):
        return [x*x for x in batch]


async def square_double(x):
    double = await DoubleBatch.gen(x)
    square = await SquareBatch.gen(double)
    return square


async def root():
    x = await asyncio.gather(
        square_double(10),
        square_double(20),
        square_double(30),
    )

    print(x)


loop = asyncio.get_event_loop()
loop.run_until_complete(root())

