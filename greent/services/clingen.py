import requests
from greent import node_types
from greent.graph_components import KNode, LabeledID
from greent.service import Service
from greent.util import Text, LoggingUtil
import logging,json

logger = LoggingUtil.init_logging(__name__, logging.DEBUG)

class ClinGen(Service):
    def __init__(self, context):
        super(ClinGen, self).__init__("clingen", context)

    def get_synonyms_by_caid(self, caid):
        synonyms = set()
        query_url = f'{self.url}allele/{caid}'
        allele_json = self.query_service(query_url)
        if allele_json:
            synonyms.update(self.parse_allele_json_for_synonyms(allele_json))
        return synonyms

    def get_synonyms_by_hgvs(self, hgvs_id):
        synonyms = set()
        query_url = f'{self.url}allele?hgvs={hgvs_id}'
        allele_json = self.query_service(query_url)
        if allele_json:
            synonyms.update(self.parse_allele_json_for_synonyms(allele_json))
        return synonyms

    def get_synonyms_by_other_ids(self, variant_node):
        # Just looking for 1 hit because any hit should return all of the available synonyms and caid if they exist
        synonyms = set()
        hgvs_ids = variant_node.get_synonyms_by_prefix('HGVS')
        if hgvs_ids:
            for hgvs_id in hgvs_ids:
                synonyms.update(self.get_synonyms_by_hgvs(Text.un_curie(hgvs_id)))
                if len(synonyms) > 0: return synonyms
        
        dbsnp_ids = variant_node.get_synonyms_by_prefix('DBSNP')
        if dbsnp_ids:
            for dbsnp_id in dbsnp_ids:
                rsid = Text.un_curie(dbsnp_id)
                if rsid.startswith('rs'):
                    rsid = rsid[2:]
                synonyms.update(self.get_synonyms_by_parameter_matching(variant_node, 'dbSNP.rs', rsid))
                if len(synonyms) > 0: return synonyms

        clinvar_ids = variant_node.get_synonyms_by_prefix('CLINVARVARIANT')
        if clinvar_ids:
            for clinvar_id in clinvar_ids:
                synonyms.update(self.get_synonyms_by_parameter_matching(variant_node, 'ClinVar.variationId', Text.un_curie(clinvar_id)))
                if len(synonyms) > 0: return synonyms
        
        myvariant_hg19_ids = variant_node.get_synonyms_by_prefix('MYVARIANT_HG19')
        if myvariant_hg19_ids:
            for myvariant_id in myvariant_hg19_ids:
                synonyms.update(self.get_synonyms_by_parameter_matching(variant_node, 'MyVariantInfo_hg19.id', Text.un_curie(myvariant_id)))
                if len(synonyms) > 0: return synonyms

        myvariant_hg38_ids = variant_node.get_synonyms_by_prefix('MYVARIANT_HG38')
        if myvariant_hg38_ids:
            for myvariant_id in myvariant_hg38_ids:
                synonyms.update(self.get_synonyms_by_parameter_matching(variant_node, 'MyVariantInfo_hg38.id', Text.un_curie(myvariant_id)))
                if len(synonyms) > 0: return synonyms

        return synonyms

    def get_synonyms_by_parameter_matching(self, variant_node, url_param, url_param_value):
        synonyms = set()
        query_url = f'{self.url}alleles?{url_param}={url_param_value}'
        query_json = self.query_service(query_url)
        for allele_json in query_json:
            synonyms.update(self.parse_allele_json_for_synonyms(allele_json))
        return synonyms

    def parse_allele_json_for_synonyms(self, allele_json):
        synonyms = set()
        if '@id' in allele_json:
            variant_caid = allele_json['@id'].rsplit('/', 1)[1]
            synonyms.add(LabeledID(identifier=f'CAID:{variant_caid}', label=f'Variant(caid): {variant_caid}'))
                            
        if ('externalRecords' in allele_json):
            if ('MyVariantInfo_hg19' in allele_json['externalRecords']):
                for myvar_json in allele_json['externalRecords']['MyVariantInfo_hg19']:
                    myvariant_id = myvar_json['id']
                    synonyms.add(LabeledID(identifier=f'MYVARIANT_HG19:{myvariant_id}', label=f'Variant(myvar19): {myvariant_id}'))

            if ('MyVariantInfo_hg38' in allele_json['externalRecords']):
                for myvar_json in allele_json['externalRecords']['MyVariantInfo_hg38']:
                    myvariant_id = myvar_json['id']
                    synonyms.add(LabeledID(identifier=f'MYVARIANT_HG38:{myvariant_id}', label=f'Variant(myvar38): {myvariant_id}'))

            if ('ClinVarVariations' in allele_json['externalRecords']):
                for clinvar_json in allele_json['externalRecords']['ClinVarVariations']:
                    clinvar_id = clinvar_json['variationId']
                    synonyms.add(LabeledID(identifier=f'CLINVARVARIANT:{clinvar_id}', label=f'Variant(ClinVar): {clinvar_id}'))

            if ('dbSNP' in allele_json['externalRecords']):
                for dbsnp_json in allele_json['externalRecords']['dbSNP']:
                    if 'rs' in dbsnp_json:
                        variant_rsid = dbsnp_json['rs']
                        synonyms.add(LabeledID(identifier=f'DBSNP:rs{variant_rsid}', label=f'Variant(dbSNP): {variant_rsid}'))

        if ('genomicAlleles' in allele_json):
            for genomic_allele in allele_json['genomicAlleles']:
                if 'hgvs' in genomic_allele:
                    for hgvs_id in genomic_allele['hgvs']:
                        synonyms.add(LabeledID(identifier=f'HGVS:{hgvs_id}', label=f'Variant(hgvs): {hgvs_id}'))

        return synonyms

    def gene_to_sequence_variant(self, gene_node):
        curie_gene_symbols = gene_node.get_synonyms_by_prefix('HGNC.SYMBOL')
        if not curie_gene_symbols:
            logger.warn('HGNC.SYMBOL synonyms not found!!!')

        return_results = []
        for currie_gene_symbol in curie_gene_symbols:
            counter = 0
            gene_symbol = Text.un_curie(currie_gene_symbol)
            while True:
                query_url = f'{self.url}alleles?gene={gene_symbol}&limit=100&skip={counter}'
                query_results = self.query_service(query_url)
                if query_results:
                    for allele_json in query_results:
                        if '@id' in allele_json:
                            variant_caid = allele_json['@id'].rsplit('/', 1)[1]
                            variant_node = KNode(f'CAID:{variant_caid}', type=node_types.SEQUENCE_VARIANT)
                            predicate = LabeledID(identifier=f'clingen.gene_to_sequence_variant',label=f'gene_to_sequence_variant')
                            edge = self.create_edge(gene_node, variant_node, 'clingen.gene_to_sequence_variant', currie_gene_symbol, predicate, url=query_url)
                            return_results.append((edge, variant_node))
                    counter += 100
                else:
                    break
        return return_results

    def query_service(self, query_url):
        #headers = {'Accept':'application/json'}
        query_response = requests.get(query_url)
        if query_response.status_code != 200:
            logger.error(f'ClinGen returned a non-200 response({query_response.status_code}) calling ({query_url})')
            return {}
        else:
            query_json = query_response.json()
            return query_json
        

