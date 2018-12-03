import requests
from greent import node_types
from greent.graph_components import KNode, LabeledID
from greent.service import Service
from greent.util import Text, LoggingUtil
import logging,json

logger = logging.getLogger(__name__)

class MyVariant(Service):
    def __init__(self, context):
        super(MyVariant, self).__init__("myvariant", context)
        self.effects_ignore_list = ['intergenic_region', 'sequence_feature']

    def sequence_variant_to_gene(self, variant_node):
        return_results = []
        myvariant_ids = variant_node.get_synonyms_by_prefix('MYVARIANT_HG19')
        myvariant_assembly = "hg19"
        if not myvariant_ids:
            myvariant_ids = variant_node.get_synonyms_by_prefix('MYVARIANT_HG38')
            myvariant_assembly = "hg38"
        if not myvariant_ids:
            logger.warning(f'No MyVariant ID found for {variant_node.id}, sequence_variant_to_gene failed.')
        else: 
            for curie_myvariant_id in myvariant_ids:
                variant_id = Text.un_curie(curie_myvariant_id)
                query_url = f'{self.url}variant/{variant_id}?assembly={myvariant_assembly}'
                query_response = requests.get(query_url)
                if query_response.status_code == 200:
                    query_json = query_response.json()
                    if 'snpeff' in query_json and 'ann' in query_json['snpeff']:
                        annotation_info = query_json['snpeff']['ann']
                        # sometimes this is a list and sometimes a single instance
                        if not isinstance(annotation_info, list):
                            annotation_info = [annotation_info]
                        for annotation in annotation_info:
                            new_result = self.process_snpeff_annotation(variant_node, annotation, curie_myvariant_id, query_url)
                            return_results.extend(new_result)
                else:
                    logger.error(f'MyVariant returned a non-200 response: {query_response.status_code})')

        return return_results

    def process_snpeff_annotation(self, variant_node, annotation, curie_id, query_url):
        results = []
        try:
            #gene_identifier = f'HGNC:{annotation["gene_id"]}'
            # TODO: this assumes the strange behavior that gene_id is a symbol not an ID
            # for now we overwrite it with a real ID if we can find it
            # when myvariant fixes that, we won't necessarily need this
            gene_symbol = annotation['genename']
            synonyms = self.context.core.hgnc.get_synonyms(f'HGNC.SYMBOL:{gene_symbol}')
            for identifier in [s.identifier for s in synonyms]:
                if Text.get_curie(identifier) == 'HGNC':
                    
                    if 'putative_impact' in annotation:
                        props={'putative_impact': annotation['putative_impact']}
                    else:
                        props = {}

                    effects = annotation['effect'] # could be multiple, with a & delimeter
                    effects_list = effects.split('&')
                    for effect in effects_list:
                        if effect in self.effects_ignore_list:
                            continue

                        gene_node = KNode(identifier, type=node_types.GENE, name=gene_symbol)
                        predicate = LabeledID(identifier=f'SNPEFF:{effect}', label=f'{effect}')
                        edge = self.create_edge(variant_node, gene_node, 'myvariant.sequence_variant_to_gene', curie_id, predicate, url=query_url, properties=props)
                        results.append((edge, gene_node))

                    break
        except KeyError as e:
            logger.error(f'myvariant annotation error:{e}')
                
        return results
