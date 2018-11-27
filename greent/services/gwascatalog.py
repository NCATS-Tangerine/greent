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
        if dbsnp_curie_ids:
            for dbsnp_curie_id in dbsnp_curie_ids:
                query_url = f'{self.url}singleNucleotidePolymorphisms/{Text.un_curie(dbsnp_curie_id)}/associations'
                query_json = self.query_service(query_url)
                if ('_embedded' in query_json) and ('associations' in query_json['_embedded']):
                    for association in query_json['_embedded']['associations']:
                        if ('_links' in association) and ('self' in association['_links']) and ('href' in association['_links']['self']):
                                association_id = association['_links']['self']['href'].rsplit('/', 1)[1]
                                if association_id:
                                    props = {}
                                    if 'pvalue' in association:
                                        props['pvalue'] = association['pvalue']

                                    pubmed_id = self.get_pubmed_id_by_association(association_id)
                                    if pubmed_id:
                                        props['pubmedId'] = pubmed_id

                                    efo_traits = self.get_efo_traits_by_association(association_id)
                                    for efo_trait in efo_traits:
                                        efo_node = KNode(f'EFO:{efo_trait["id"]}', name=f'{efo_trait["trait"]}', type=node_types.DISEASE_OR_PHENOTYPIC_FEATURE)
                                        predicate = LabeledID(identifier=f'gwascatalog:has_phenotype',label=f'has_phenotype')
                                        edge = self.create_edge(variant_node, efo_node, 'gwascatalog.sequence_variant_to_disease_or_phenotypic_feature', variant_node.id, predicate, url=query_url, properties=props)
                                        return_results.append((edge, efo_node))
        return return_results

    def get_pubmed_id_by_association(self, association_id):
        query_url = f'{self.url}associations/{association_id}/study'
        query_json = self.query_service(query_url)
        if ('publicationInfo' in query_json) and ('pubmedId' in query_json['publicationInfo']):
            return query_json['publicationInfo']['pubmedId']
        else:
            return None

    def get_efo_traits_by_association(self, association_id):
        query_url = f'{self.url}associations/{association_id}/efoTraits'
        query_json = self.query_service(query_url)
        efo_traits = []
        if ('_embedded' in query_json) and ('efoTraits' in query_json['_embedded']):
            for efo_trait in query_json['_embedded']['efoTraits']:
                if 'shortForm' in efo_trait:
                    efo_traits.append({'id': efo_trait['shortForm'], 'trait': efo_trait['trait']})
        return efo_traits

    def query_service(self, query_url):
        headers = {'Accept':'application/json'}
        query_response = requests.get(query_url)
        if query_response.status_code != 200:
            logger.warning(f'GWAS Catalog returned a non-200 response({query_response.status_code}) calling ({query_url})')
            return {}
        else:
            query_json = query_response.json()
            return query_json

