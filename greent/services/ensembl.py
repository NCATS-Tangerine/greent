import requests
from greent import node_types
from greent.graph_components import KNode, LabeledID
from greent.service import Service
from greent.util import Text, LoggingUtil
import logging,json

logger = LoggingUtil.init_logging(__name__, level=logging.DEBUG)#

class Ensembl(Service):
    
    def __init__(self, context):
        super(Ensembl, self).__init__("ensembl", context)
        self.clingen = context.core.clingen
        self.cache = context.cache

    def sequence_variant_to_gene(self, variant_node):
        flanking_region_size = 500000
        predicate = LabeledID(identifier=f'GAMMA:0000102', label=f'nearby_variant_of')
        results = []

        #logger.debug(f'ensembl: props given to ensembl: {variant_node.properties} ')
        #logger.debug(f'ensembl: variant : {variant_node.dump()}')

        #if ('sequence_location' not in variant_node.properties.keys()) or (not variant_node.properties['sequence_location']):
        #    logger.debug(f'ensembl: variant location properties not set properly for variant: {variant_node.id}')
        #    return results
        #seqvar_location = variant_node.properties['sequence_location']

        robokop_ids = variant_node.get_synonyms_by_prefix('ROBO_VARIANT')
        if not robokop_ids:
            logger.debug(f'ensembl: robokop variant key not found for variant: {variant_node.id}')
            return results
        else:
            try:
                robokop_key = robokop_ids.pop()
                robokop_data = Text.un_curie(robokop_key).split('|')
                reference_genome = robokop_data[0]
                chromosome = robokop_data[1]
                start_position = int(robokop_data[2])
                end_position = int(robokop_data[3])
            except IndexError as e:
                logger.debug(f'ensembl: robokop variant key not set properly for variant: {variant_node.id} - {robokop_ids[0]}')
                return results

        #reference_genome = seqvar_location[0]
        #chromosome = seqvar_location[1]
        #position = int(seqvar_location[2])

        flanking_min = start_position - flanking_region_size
        if flanking_min < 0:
            flanking_min = 0
        flanking_max = end_position + flanking_region_size
        
        if reference_genome == 'HG19':
            service_url = 'https://grch37.rest.ensembl.org'
        elif reference_genome == 'HG38':
            service_url = self.url
        else:
            logger.debug(f'ensembl: robokop_id reference genome not recognized by ensembl : {reference_genome}')


        overlap_url = '/overlap/region/human/'
        options_url = '?feature=gene'
        query_url = f'{service_url}{overlap_url}{chromosome}:{flanking_min}-{flanking_max}{options_url}'

        query_response = requests.get(query_url, headers={"Content-Type" : "application/json"})
        if query_response.status_code == 200:
            query_json = query_response.json()
            gene_ids = self.parse_genes_from_ensembl(query_json)
            for gene_id, gene_start, gene_end in gene_ids:
                gene_node = KNode(f'ENSEMBL:{gene_id}', name=f'{gene_id}', type=node_types.GENE)
                if start_position < gene_start:
                    distance = gene_start - start_position
                elif end_position > gene_end:
                    distance = end_position - gene_end
                else:
                    distance = 0
                props = {'distance' : distance}
                edge = self.create_edge(variant_node, gene_node, 'ensembl.sequence_variant_to_gene', variant_node.id, predicate, url=query_url, properties=props)
                results.append((edge, gene_node))
        else:
            logger.error(f'Ensembl returned a non-200 response: {query_response.status_code})')

        logger.info(f'ensembl sequence_variant_to_gene found {len(results)} results for {variant_node.id}')

        return results

    def parse_genes_from_ensembl(self, json_genes):
        genes = []
        for gene in json_genes:
            try:
                gene_id = gene['gene_id']
                start = gene['start']
                end = gene['end']
                genes.append((gene_id, start, end))
            except KeyError as e:
                logger.debug(f'gene properties not found in ensembl result: {gene} : {e}')
        return genes

    def sequence_variant_to_sequence_variant(self, variant_node):
        ld_url = '/ld/human/'
        options_url = '?r2=0.7'
        population = '1000GENOMES:phase_3:MXL'
        predicate = LabeledID(identifier=f'NCIT:C16798', label=f'linked_to')

        return_results = []
        dbsnp_curie_ids = variant_node.get_synonyms_by_prefix('DBSNP')
        for dbsnp_curie in dbsnp_curie_ids:
            variant_id = Text.un_curie(dbsnp_curie)
            query_url = f'{self.url}{ld_url}{variant_id}/{population}{options_url}'
            query_response = requests.get(query_url, headers={"Content-Type" : "application/json"})
            if query_response.status_code == 200:
                query_json = query_response.json()
                variant_results = self.parse_ld_variants_from_ensembl(query_json)
                for variant_info in variant_results:
                    new_variant_id = variant_info[0]
                    r_squared = variant_info[1]
                    props = {'r2' : r_squared}
                    new_variant_curie = f'DBSNP:{new_variant_id}'
                    new_variant_node = KNode(new_variant_curie, name=f'{new_variant_id}', type=node_types.SEQUENCE_VARIANT)
                    synonyms = self.cache.get(f'synonymize({new_variant_curie})') 
                    if synonyms is None:
                        synonyms = self.clingen.get_synonyms_by_other_ids(new_variant_node)
                        self.cache.set(f'synonymize({new_variant_curie})', synonyms)
                    for synonym in synonyms:
                        if Text.get_curie(synonym.identifier) == 'CAID':
                            caid_node = KNode(synonym.identifier, name=f'{new_variant_id}', type=node_types.SEQUENCE_VARIANT)
                            edge = self.create_edge(variant_node, caid_node, 'ensembl.sequence_variant_to_sequence_variant', dbsnp_curie, predicate, url=query_url, properties=props)
                            return_results.append((edge, caid_node))
            else:
                logger.error(f'Ensembl returned a non-200 response for {variant_node.identifier}: {query_response.status_code})')

        return return_results

    def parse_ld_variants_from_ensembl(self, json_variants):
        variants = []
        for variant in json_variants:
            try:
                variant_id = variant['variation2']
                r_squared = variant['r2']
                variants.append((variant_id, r_squared))
            except KeyError:
                logger.debug(f'variation2 or r2 not found in ensembl result: {variant}')
        return variants
