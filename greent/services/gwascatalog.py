import requests
from greent import node_types
from greent.graph_components import KNode, LabeledID
from greent.service import Service
from greent.util import Text, LoggingUtil
import logging,json

logger = LoggingUtil.init_logging(__name__, logging.DEBUG)

class GWASCatalog(Service):
    def __init__(self, context):
        super(GWASCatalog, self).__init__("gwascatalog", context)

    def sequence_variant_to_phenotype(self, variant_node):
        return_results = []
        dbsnp_curie_ids = variant_node.get_synonyms_by_prefix('DBSNP')
        for dbsnp_curie_id in dbsnp_curie_ids:
            query_url = f'{self.url}singleNucleotidePolymorphisms/{Text.un_curie(dbsnp_curie_id)}/associations?projection=associationBySnp'
            query_json = self.query_service(query_url)
            if query_json:
                try:
                    for association in query_json['_embedded']['associations']:
                        phenotype_nodes = []
                        for trait in association['efoTraits']:
                            trait_id = trait['shortForm']
                            trait_name = trait['trait']
                            # For now only take EFO terms, these could also be Orphanet IDs here
                            if trait_id.startswith('EFO_'):
                                efo_id = trait_id[4:]
                                phenotype_nodes.append(KNode(f'EFO:{efo_id}', name=f'{trait_name}', type=node_types.DISEASE_OR_PHENOTYPIC_FEATURE))
                            elif trait_id.startswith('Orp'):
                                orphanet_id = trait_id[9:]
                                phenotype_nodes.append(KNode(f'ORPHANET:{orphanet_id}', name=f'{trait_name}', type=node_types.DISEASE_OR_PHENOTYPIC_FEATURE))
                            else:
                                logger.info(f'gwascatalog returned an unknown id type: {trait_id}')

                        if phenotype_nodes:
                            props = {}
                            try:
                                props['pvalue'] = float(association['pvalue'])
                            except ValueError:
                                pass

                            pubs = []
                            association_id = association['_links']['self']['href'].rsplit('/', 1)[1]
                            pubmed_id = self.get_pubmed_id_by_association(association_id)
                            if pubmed_id:
                                pubs.append(f'PMID:{pubmed_id}')

                            predicate = LabeledID(identifier=f'gwascatalog:has_phenotype',label=f'has_phenotype')
                            for new_node in phenotype_nodes:
                                edge = self.create_edge(
                                    variant_node, 
                                    new_node, 
                                    'gwascatalog.sequence_variant_to_disease_or_phenotypic_feature', 
                                    variant_node.id, 
                                    predicate, 
                                    url=query_url, 
                                    properties=props, 
                                    publications=pubs)
                                return_results.append((edge, new_node))

                except (KeyError, IndexError) as e:
                    logger.warning(f'problem parsing results from GWASCatalog: {e}')

        return return_results

    def get_pubmed_id_by_association(self, association_id):
        pubmed_id = None
        query_url = f'{self.url}associations/{association_id}/study'
        query_json = self.query_service(query_url)
        if query_json:
            try:
                pubmed_id = query_json['publicationInfo']['pubmedId']
            except KeyError as e:
                logger.warning(f'problem parsing pubmed id results from GWASCatalog: {e}')
        return pubmed_id

    def disease_or_phenotypic_feature_to_sequence_variant(self, phenotype_node):
        return_results = []
        efo_trait_ids = phenotype_node.get_synonyms_by_prefix('EFO')
        for efo_id in efo_trait_ids:
            query_url = f'{self.url}efoTraits/EFO_{Text.un_curie(efo_id)}/associations?projection=associationByEfoTrait'
            query_json = self.query_service(query_url)
            if query_json:
                try:
                    for association in query_json['_embedded']['associations']:
                        variant_nodes = []
                        for snp in association['snps']:
                            variant_rsid = snp['rsId']
                            variant_nodes.append(KNode(
                                            f'DBSNP:{variant_rsid}', 
                                            name=f'Variant(dbSNP): {variant_rsid}', 
                                            type=node_types.SEQUENCE_VARIANT))

                        if variant_nodes:
                            props = {}
                            try:
                                props['pvalue'] = float(association['pvalue'])
                            except ValueError:
                                pass

                            pubs = []
                            association_id = association['_links']['self']['href'].rsplit('/', 1)[1]
                            pubmed_id = self.get_pubmed_id_by_association(association_id)
                            if pubmed_id:
                                pubs.append(f'PMID:{pubmed_id}')

                            predicate = LabeledID(identifier=f'gwascatalog:correlated_with', label=f'correlated_with')
                            for new_node in variant_nodes:
                                edge = self.create_edge(
                                    phenotype_node, 
                                    new_node, 
                                    'gwascatalog.disease_or_phenotypic_feature_to_sequence_variant', 
                                    phenotype_node.id, 
                                    predicate, 
                                    url=query_url, 
                                    properties=props, 
                                    publications=pubs)
                                return_results.append((edge, new_node))

                except (KeyError, IndexError) as e:
                    logger.warning(f'problem parsing results from GWASCatalog: {e}')

        return return_results

    def query_service(self, query_url):
        query_response = requests.get(query_url)
        if query_response.status_code != 200:
            logger.warning(f'GWAS Catalog returned a non-200 response({query_response.status_code}) calling ({query_url})')
            return {}
        else:
            query_json = query_response.json()
            return query_json

