import asyncio
import concurrent.futures
import requests
from greent.util import LoggingUtil

logger = LoggingUtil.init_logging (__name__)

class AsyncUtil:
    
    @staticmethod
    async def parallel_requests (urls, process_response, degree=20):
        with concurrent.futures.ThreadPoolExecutor(max_workers=degree) as executor:
            loop = asyncio.get_event_loop()
            futures = [
                loop.run_in_executor(
                    executor, 
                    requests.get, 
                    url
                )
                for url in urls
            ]
            for response in await asyncio.gather(*futures):
                process_response (response)

    @staticmethod
    def execute_parallel_requests (urls, response_processor, chunk_size=20):
        chunk_size = 20
        chunks = [urls[i:i + chunk_size] for i in range(0, len(urls), chunk_size)]
        for chunk in chunks:
            logger.debug (" requests: {0}".format (len(chunk)))
            loop = asyncio.get_event_loop()
            loop.run_until_complete(AsyncUtil.parallel_requests(chunk, process_response=response_processor))
