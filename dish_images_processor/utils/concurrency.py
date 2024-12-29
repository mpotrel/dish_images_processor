import asyncio
from contextlib import asynccontextmanager

class ConcurrencyLimiter:
    def __init__(self, max_concurrent: int):
        self._semaphore = asyncio.Semaphore(max_concurrent)

    @asynccontextmanager
    async def limit(self):
        async with self._semaphore:
            yield
