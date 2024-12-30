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
    active_operations = 0
    max_active = 0

    async def test_operation(id: int):
        nonlocal active_operations, max_active
        async with await mock_limiter.limit():
            active_operations += 1
            max_active = max(max_active, active_operations)
            await asyncio.sleep(0.1)
            active_operations -= 1
            return True

    results = await asyncio.gather(
        test_operation(1),
        test_operation(2),
        test_operation(3)
    )

    assert max_active <= 2  # Verify concurrency limit
    assert all(results)

@pytest.mark.asyncio
async def test_concurrency_limiter_error_handling(mock_limiter):
    async def test_operation():
        async with await mock_limiter.limit():
            raise ValueError("Test error")

    with pytest.raises(ValueError):
        await test_operation()
