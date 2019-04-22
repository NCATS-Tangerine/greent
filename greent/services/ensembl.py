import requests
from greent import node_types
from greent.graph_components import KNode, LabeledID
from greent.service import Service
from greent.util import Text, LoggingUtil
import logging,json

logger = LoggingUtil.init_logging(__name__, level=logging.DEBUG)#

class Ensembl(Service):
    
    def __init__(self, context, rosetta):
        super(Ensembl, self).__init__("ensembl", context)
        self.synonymizer = rosetta.synonymizer

    def sequence_variant_to_gene(self, variant_node):
        flanking_region_size = 500000
        predicate = LabeledID(identifier=f'GAMMA:0000102', label=f'nearby_variant_of')
        results = []

        #logger.debug(f'ensembl: props given to ensembl: {variant_node.properties} ')
        #logger.debug(f'ensembl: variant : {variant_node.dump()}')

        if ('sequence_location' not in variant_node.properties.keys()) or (not variant_node.properties['sequence_location']):
            logger.debug(f'ensembl: variant location properties not set properly for variant: {variant_node.id}')
            return results

        seqvar_location = variant_node.properties['sequence_location']
        
        reference_genome = seqvar_location[0]
        chromosome = seqvar_location[1]
        position = int(seqvar_location[2])

        start_position = position - flanking_region_size
        if start_position < 0:
            start_position = 0
        end_position = position + flanking_region_size
        
        if reference_genome == 'HG19':
            service_url = 'https://grch37.rest.ensembl.org'
        else:
            service_url = self.url

        overlap_url = '/overlap/region/human/'
        options_url = '?feature=gene'
        query_url = f'{service_url}{overlap_url}{chromosome}:{start_position}-{end_position}{options_url}'

        query_response = requests.get(query_url, headers={ "Content-Type" : "application/json"})
        if query_response.status_code == 200:
            query_json = query_response.json()
            gene_ids = self.parse_genes_from_ensembl(query_json)
            for gene_id in gene_ids:
                gene_node = KNode(f'ENSEMBL:{gene_id}', name=f'{gene_id}', type=node_types.GENE)
                self.synonymizer.synonymize(gene_node)
                edge = self.create_edge(variant_node, gene_node, 'ensembl.sequence_variant_to_gene', variant_node.id, predicate, url=query_url)
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
                genes.append(gene_id)
            except KeyError:
                logger.debug(f'gene_id not found in ensembl result: {gene}')
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
                    new_variant_node = KNode(f'DBSNP:{new_variant_id}', name=f'{new_variant_id}', type=node_types.SEQUENCE_VARIANT)
                    self.synonymizer.synonymize(new_variant_node)
                    edge = self.create_edge(variant_node, new_variant_node, 'ensembl.sequence_variant_to_sequence_variant', dbsnp_curie, predicate, url=query_url, properties=props)
                    return_results.append((edge, new_variant_node))
            else:
                logger.error(f'Ensembl returned a non-200 response: {query_response.status_code})')

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
