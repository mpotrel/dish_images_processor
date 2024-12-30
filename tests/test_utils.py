import asyncio

import pytest

@pytest.mark.asyncio
async def test_concurrency_limiter_basic(mock_limiter):
    async def test_operation():
        async with await mock_limiter.limit():
            return True

    result = await test_operation()
    assert result is True

@pytest.mark.asyncio
async def test_concurrency_limiter_concurrent_access(mock_limiter):
    async def test_operation(delay: float):
        async with await mock_limiter.limit():
            await asyncio.sleep(delay)
            return True

    results = await asyncio.gather(
        test_operation(0.1),
        test_operation(0.1)
    )

    assert all(results)

@pytest.mark.asyncio
async def test_concurrency_limiter_max_concurrent(mock_limiter):
    completion_order = []

    async def test_operation(id: int):
        async with await mock_limiter.limit():
            await asyncio.sleep(0.1)
            completion_order.append(id)
            return True

    # Start 3 operations (limiter max is 2)
    results = await asyncio.gather(
        test_operation(1),
        test_operation(2),
        test_operation(3)
    )

    assert all(results)
    assert len(completion_order) == 3
    # First two operations should complete before the third
    assert 3 not in completion_order[:2]

@pytest.mark.asyncio
async def test_concurrency_limiter_error_handling(mock_limiter):
    async def test_operation():
        async with await mock_limiter.limit():
            raise ValueError("Test error")

    with pytest.raises(ValueError):
        await test_operation()
