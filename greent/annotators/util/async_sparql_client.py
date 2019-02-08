from aiosparql.client import SPARQLClient
from greent.triplestore import TripleStore 
import logging
from string import Template
logger = logging.getLogger(__name__)


class TripleStoreAsync(TripleStore):
    def __init__(self, host_name):
        super().__init__(host_name)
        self.host_name = host_name

    async def async_execute_query(self, query, post = False):
        """
        Always returns JSON response.
        """
        async with SPARQLClient(self.host_name) as client:
            result = await client.query(query)
        return result

    async def async_query(self, query_text, outputs, flat= False, post = False):
        """
        Some modifications from the TripleStore class to accomodate the return format
        of our async client.
        """
        response = await self.async_execute_query (query_text, post)
        bindings = response['results']['bindings']
        result = None
        if flat:
            result = list(map(lambda b : [ b[val]['value'] for val in outputs ], bindings))
        else:
            result = list(map(lambda b : { val : b[val]['value'] for val in outputs },bindings ))
        logger.debug ("query result: %s", result)
        return result
    
    async def async_query_template(self, template_text, outputs, inputs=[]):
        """
        Substitutes Template parameters with actual 
        """        
        return await self.async_query(Template (template_text).safe_substitute (**inputs), outputs)
    
    
