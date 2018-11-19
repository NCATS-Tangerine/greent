import requests
from greent import node_types
from greent.graph_components import KNode, LabeledID
from greent.service import Service
from greent.util import Text, LoggingUtil
import logging,json

logger = LoggingUtil.init_logging(__name__, logging.DEBUG)

class MyVariant(Service):
    def __init__(self, context):
        super(MyVariant, self).__init__("myvariant", context)
        self.effects_ignore_list = ['intergenic_region']

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
                            if new_result:
                                return_results.extend(new_result)
                else:
                    logger.error(f'MyVariant returned a non-200 response: {query_response.status_code})')

        return return_results

    def process_snpeff_annotation(self, variant_node, annotation, curie_id, query_url):
        results = []
        if 'gene_id' in annotation:
            gene_identifier = f'HGNC:{annotation["gene_id"]}'
            
            # TODO: this assumes the strange behavior that gene_id is a symbol not an ID
            # for now we overwrite it with a real ID if we can find it
            # when myvariant fixes that, we won't necessarily need this
            if 'genename' in annotation:
                gene_symbol = annotation['genename']
                synonyms = self.context.core.hgnc.get_synonyms(f'HGNC.SYMBOL:{gene_symbol}')
                for identifier in [s.identifier for s in synonyms]:
                    if Text.get_curie(identifier).upper() == 'HGNC':
                        gene_identifier = identifier
                        break

            if 'effect' in annotation:
                effects = annotation['effect'] # could be multiple, with a & delimeter
            else:
                effects = 'missing_effect'
            if 'putative_impact' in annotation:
                props={'putative_impact': annotation['putative_impact']}
            else:
                props = {}

            gene_node = KNode(gene_identifier, type=node_types.GENE, name=gene_symbol)

            for effect in effects.split('&'):
                # This should be switched so that the hgnc id is the node id
                # For now they are returning both fields with the symbol so I took the symbol as node id because it's actually correct
                # gene_node = KNode(f'HGNC.SYMBOL:{gene_symbol}', type=node_types.GENE)
                #gene_node.add_synonyms((LabeledID(identifier=f'HGNC:{gene_id}', label=f'{gene_id}')))
                
                if effect in self.effects_ignore_list:
                    continue

                predicate = LabeledID(identifier=f'SNPEFF:{effect}', label=f'{effect}')
                edge = self.create_edge(variant_node, gene_node, 'myvariant.sequence_variant_to_gene', curie_id, predicate, url=query_url, properties=props)
                results.append((edge, gene_node))
                
        return results
