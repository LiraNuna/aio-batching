import asyncio
from typing import Any, Awaitable, List, Tuple

queue: List[Tuple[int, asyncio.Future]] = []
event = asyncio.Event()
loop = asyncio.get_event_loop()


def scheduler():
    global queue
    if not queue:
        return

    print('>>>>> flush', [k for k, _ in queue])
    for key, future in queue:
        future.set_result(key ** key)

    queue = []
    event.set()


async def single(x: int):
    future = loop.create_future()
    queue.append((x, future))

    event.clear()
    loop.call_soon(scheduler)
    await event.wait()

    return future.result()


async def deep(y) -> Awaitable[int]:
    x = await single(-1)
    x = [await single(t) for t in (9, 8, 7, 6)]

    return x


async def root():
    x = await single(1)
    y = await asyncio.gather(
        single(2), 
        single(3), 
        single(4),
    )
    z = await asyncio.gather(
        single(2), 
        single(3), 
        single(4),
    )

    w = await asyncio.gather(
        single(1),
        asyncio.gather(
            single(2), 
            single(3), 
            single(4),
        ),
        asyncio.gather(
            deep(2), 
            deep(3), 
            deep(4),
        ),
    )

    print(x, y, z, w, sep='\n')


loop.run_until_complete(root())

