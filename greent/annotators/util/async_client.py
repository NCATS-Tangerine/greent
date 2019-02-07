import aiohttp
import logging

logger = logging.getLogger(name = __name__)

async def async_get_json(url, headers = {}):
    """
        Gets json response from url asyncronously.
    """
    async with aiohttp.ClientSession() as session :
        async with session.get(url, headers= headers) as response:
            if response.status != 200:
                logger.error(f"Failed to get response from {url}. Status code {response.status}")
                return {}
            return await response.json()

async def async_get_text(url,headers = {}):
    """
        Gets text response from url asyncronously
    """
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers= headers) as response:
            if response.status != 200:
                logger.error(f'Failed to get response from {url}, returned status : {response.status}')
                return ''
            return await response.text()
