from greent.annotators.annotator import Annotator
import logging

logger = logging.getLogger(name = __name__)

class DiseaseAnnotator(Annotator):

    def __init__(self, rosetta):
        super().__init__(rosetta)
        self.prefix_source_mapping = {
            'MONDO': self.get_mondo_properties
        }

    async def get_mondo_properties(self, mondo_curie):
        """
        Gets the ascestors from onto and maps them to the ones we are intereseted in.
        """
        conf = self.get_prefix_config('MONDO')
        ancestors_url = conf['url'] + mondo_curie
        response = await self.async_get_json(ancestors_url)
        if 'ancestors' not in response:
            return {}
        ancestors = response['ancestors']
        properties = { conf['keys'][x] : True for x in ancestors if x in conf['keys']}
        return properties

