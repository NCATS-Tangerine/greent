from greent import node_types
import requests
import time
import logging
from greent.annotators.annotator import Annotator
import re
from greent.annotators.util.ftp_helper import pull_hgnc_json

logger = logging.getLogger(__name__)
class GeneAnnotator(Annotator):
    """
    Singleton service that will download the whole hgnc_json reformat it and grab from there. 
    """
    instance = None

    def __init__(self, rosetta):
        if not self.instance:
            self.instance = GeneAnnotator.__GeneAnnotator(rosetta)
        else:
            logger.debug('found annotator already created... going to  use that one.')
            self.instance.rosetta = rosetta
    
    def __getattr__(self, name):
        #redirect calls to the instance.
        return getattr(self.instance, name)
    
    class __GeneAnnotator(Annotator):
        def __init__(self, rosetta):
            super().__init__(rosetta)
            self.prefix_source_mapping = {
                'HGNC': self.get_hgnc_annotations
            }
            self.hgnc_data = None
        
        def get_hgnc_full(self,hgnc_id = None):
            """
            Downloads and reformats hgnc so it can be access with hgnc_id.
            """
            if not self.hgnc_data:
                logger.debug('Fetching hgnc whole data... ')
                self.hgnc_data = {}
                hgnc_json = pull_hgnc_json()
                for hgnc_item in hgnc_json['response']['docs']:
                    if hgnc_item['hgnc_id'] not in self.hgnc_data:
                        self.hgnc_data[hgnc_item['hgnc_id']] = []
                    self.hgnc_data[hgnc_item['hgnc_id']].append(hgnc_item)
            if hgnc_id:
                return self.hgnc_data.get(hgnc_id,[])
            return self.hgnc_data


        def get_hgnc_annotations(self, node_curie):
            """
            Returns a dictionary of annotations
            """
            docs = self.get_hgnc_full(node_curie)
            conf = self.get_prefix_config('HGNC')
            annotations = {}
            for doc in docs:
                extract = self.extract_annotation_from_hgnc(doc, conf['keys'] )
                annotations.update(extract)
            return annotations

        def extract_annotation_from_hgnc(self, raw, keys_of_interest= []):
            """
            Exracts certain parts of  the HGNC gene data.
            """
            new  = { keys_of_interest[key] : raw[key] for key in keys_of_interest if key in raw }
            #sanity check
            if len(new.keys()) != len(keys_of_interest):
                logger.warning(f"found data less than expected for {raw['hgnc_id']} ")
            if 'location' in new and new['location'] != None:
                # some don't have location
                # Cytogenetic location, I think first digit denotes Chromosome number. 
                regex = re.compile(r'\d+|\D+')
                match = regex.search(new['location'])[0]
                new['chromosome'] = match
            new['taxon'] = '9606'
            return new