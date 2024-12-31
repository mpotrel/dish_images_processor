import aiohttp

from dish_images_processor.config.logging import get_logger

logger = get_logger(__name__)

async def is_valid_image(url: str) -> bool:
    """Check if URL points to a valid image by checking headers"""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.head(url, allow_redirects=True) as response:
                content_type = response.headers.get('content-type', '')
                return content_type.startswith('image/')
    except Exception as e:
        logger.error(f"Error validating image URL {url}: {e}")
        return False
