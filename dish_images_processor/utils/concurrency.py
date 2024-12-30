import asyncio
from dish_images_processor.config.logging import get_logger
logger = get_logger(__name__)

class ConcurrencyLimiter:
    def __init__(self, max_concurrent: int):
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._max_concurrent = max_concurrent
        self._current_count = 0  # Track actual concurrent requests

    async def limit(self):
        """
        Acquire semaphore for concurrent operation
        """
        self._current_count += 1
        logger.info(f"Attempting to acquire semaphore. Current count: {self._current_count}")

        if self._semaphore._value == 0:
            logger.warning(f"ALL {self._max_concurrent} SLOTS OCCUPIED. WAITING!")

        await self._semaphore.acquire()
        logger.info(f"Semaphore acquired. Remaining slots: {self._semaphore._value}")
        return self

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self._semaphore.release()
        self._current_count -= 1
        logger.info(f"Semaphore released. Current count: {self._current_count}")
        return False
