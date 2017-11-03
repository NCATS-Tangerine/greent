import asyncio
import logging
import concurrent.futures
import requests
from collections import namedtuple
from greent.util import LoggingUtil
import multiprocessing

logger = LoggingUtil.init_logging (__name__, level=logging.DEBUG)

Operation = namedtuple ('Operation', [ 'operation', 'arguments' ])
default_chunk_size = multiprocessing.cpu_count()

class AsyncUtil:
    
    @staticmethod
    async def parallel_requests (urls, process_response, degree=default_chunk_size):
        with concurrent.futures.ThreadPoolExecutor(max_workers=degree) as executor:
            loop = asyncio.get_event_loop ()
            futures = [ loop.run_in_executor (executor, requests.get, url) for url in urls ]
            for response in await asyncio.gather(*futures):
                process_response (response)

    @staticmethod
    def execute_parallel_requests (urls, response_processor, chunk_size=default_chunk_size):
        chunks = [urls[i:i + chunk_size] for i in range(0, len(urls), chunk_size)]
        logger.debug ("urls: {}".format (urls))
        for chunk in chunks:
            #logger.debug (" async requests: {0}".format (len(chunk)))
            loop = asyncio.get_event_loop()
            loop.run_until_complete(AsyncUtil.parallel_requests(chunk, process_response=response_processor))

    @staticmethod
    async def parallel_operation (operations, process_response, degree=default_chunk_size):
        with concurrent.futures.ThreadPoolExecutor(max_workers=degree) as executor:
            loop = asyncio.get_event_loop ()
            futures = [ loop.run_in_executor (executor, op.operation, op.arguments) for op in operations ]
            for response in await asyncio.gather (*futures):
                process_response (response)
    @staticmethod
    def execute_parallel_operations (operations, response_processor, chunk_size=default_chunk_size):
        chunks = [operations[i:i + chunk_size] for i in range(0, len(operations), chunk_size)]
        for chunk in chunks:
            #logger.debug (" async-op: {0}".format (len(chunk)))
            loop = asyncio.get_event_loop()
            loop.run_until_complete(AsyncUtil.parallel_operation(chunk, process_response=response_processor))
