import requests
from greent import node_types
from greent.graph_components import KNode, LabeledID
from greent.service import Service
from greent.util import Text, LoggingUtil
import logging,json

logger = logging.getLogger(__name__)

class MyVariant(Service):
    
    def __init__(self, context, rosetta):
        super(MyVariant, self).__init__("myvariant", context)
        self.synonymizer = rosetta.synonymizer
        self.effects_ignore_list = ['intergenic_region', 'sequence_feature']

    def batch_sequence_variant_to_gene(self, variant_nodes):
        if len(variant_nodes) <= 1000:
            annotation_dictionary = {}
            post_params = {'fields' : 'snpeff', 'ids' : '', 'assembly': 'hg38'}
            node_lookup = {}
            for node in variant_nodes:
                # we could support hg19 as well, but calls need to be all one or the other
                # for now we only do hg38
                myvariant_ids = node.get_synonyms_by_prefix('MYVARIANT_HG38')
                for myvar_id in myvariant_ids:
                    post_params['ids'] += f'{Text.un_curie(myvar_id)},'
                    node_lookup[myvar_id] = node

            if not post_params['ids']:
                logger.warning('batch_sequence_variant_to_gene called but nodes provided had no MyVariant IDs')
                return annotation_dictionary
            # remove that extra comma
            post_params['ids'] = post_params['ids'][:-1]
            query_url = f'{self.url}variant'
            query_response = requests.post(query_url, data=post_params)
            if query_response.status_code == 200:
                query_json = query_response.json()
                for annotation_json in query_json:
                    try:
                        annotation_id = annotation_json['_id']
                        myvar_id = f'MYVARIANT_HG38:{annotation_id}'
                        variant_node = node_lookup[myvar_id]
                        annotation_dictionary[variant_node.id] = self.process_annotation(variant_node, annotation_json, myvar_id, query_url)
                    except KeyError as e:
                        logger.warning(f'MyVariant batch call failed on an annotation.')
                        pass 
            else:
                logger.error(f'MyVariant non-200 response on batch: {query_response.status_code})')
            return annotation_dictionary
        else:
            return self.batch_sequence_variant_to_gene(variant_nodes[0:1000]).update(self.batch_sequence_variant_to_gene(hgvs_list[1000:]))

    def sequence_variant_to_gene(self, variant_node):
        return_results = []
        myvariant_ids = variant_node.get_synonyms_by_prefix('MYVARIANT_HG38')
        myvariant_assembly = 'hg38'
        if not myvariant_ids:
            myvariant_ids = variant_node.get_synonyms_by_prefix('MYVARIANT_HG19')
            myvariant_assembly = 'hg19'
        if not myvariant_ids:
            logger.warning(f'No MyVariant ID found for {variant_node.id}, sequence_variant_to_gene failed.')
        else: 
            curie_myvariant_id = myvariant_ids.pop()
            myvariant_id = Text.un_curie(curie_myvariant_id)
            query_url = f'{self.url}variant/{myvariant_id}?assembly={myvariant_assembly}&fields=snpeff'
            query_response = requests.get(query_url)
            if query_response.status_code == 200:
                query_json = query_response.json()
                return_results = self.process_annotation(variant_node, query_json, curie_myvariant_id, query_url)
            else:
                logger.error(f'MyVariant returned a non-200 response: {query_response.status_code})')

        return return_results

    def process_annotation(self, variant_node, annotation_json, curie_id, query_url):
        results = []
        already_synonymized_gene_nodes = {}
        try:
            if 'snpeff' in annotation_json:
                annotations = annotation_json['snpeff']['ann']
                # sometimes this is a list and sometimes a single instance
                if not isinstance(annotations, list):
                    annotations = [annotations]
                for annotation in annotations:
                    # for now we only take transcript feature type annotations
                    if annotation['feature_type'] != 'transcript':
                        continue

                    # TODO: this assumes the strange behavior that gene_id is a symbol not an ID
                    # for now we overwrite it with a real ID if we can find it
                    # when myvariant fixes that, we'll just do this:
                    # gene_identifier = f'HGNC:{annotation["gene_id"]}'
                    gene_symbol = annotation['genename']

                    #synonyms = self.context.core.hgnc.get_synonyms(f'HGNC.SYMBOL:{gene_symbol}')
                    #for identifier in [s.identifier for s in synonyms]:
                    #    if Text.get_curie(identifier) == 'HGNC':

                    if gene_symbol in already_synonymized_gene_nodes:
                        temp_node = already_synonymized_gene_nodes[gene_symbol]
                    else:
                        temp_node = KNode(f'HGNC.SYMBOL:{gene_symbol}', name=f'{gene_symbol}', type=node_types.GENE)
                        self.synonymizer.synonymize(temp_node)
                        already_synonymized_gene_nodes[gene_symbol] = temp_node

                    gene_ids = temp_node.get_synonyms_by_prefix('HGNC')
                    if gene_ids:
                        identifier = gene_ids.pop()
                        props = {}
                        #do we want this?
                        #if 'putative_impact' in annotation:
                        #    props['putative_impact'] = annotation['putative_impact']

                        effects_list = annotation['effect'].split('&')
                        for effect in effects_list:
                            if effect in self.effects_ignore_list:
                                continue

                            predicate = LabeledID(identifier=f'SNPEFF:{effect}', label=f'{effect}')
                            edge = self.create_edge(variant_node, temp_node, 'myvariant.sequence_variant_to_gene', curie_id, predicate, url=query_url, properties=props)
                            results.append((edge, temp_node))
                    else:
                        logger.debug(f'MyVariant provided gene symbol: ({gene_symbol}), synonymization could not find a real ID.')

        except KeyError as e:
            logger.error(f'myvariant annotation error:{e}')
                
        return results
