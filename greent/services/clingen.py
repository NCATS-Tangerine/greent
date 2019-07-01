import requests
from greent import node_types
from greent.graph_components import KNode, LabeledID
from greent.service import Service
from greent.util import Text, LoggingUtil
from math import ceil
import logging,json

logger = LoggingUtil.init_logging(__name__, logging.DEBUG)

class ClinGen(Service):
    def __init__(self, context):
        super(ClinGen, self).__init__("clingen", context)
        self.synon_fields_param = 'fields=none+@id+externalRecords.dbSNP+externalRecords.ClinVarVariations+externalRecords.MyVariantInfo_hg38+genomicAlleles-genomicAlleles.referenceSequence'
        self.synonym_buffer = {}

    def get_batch_of_synonyms(self, variant_list, variant_format='hgvs'):
        # possible variant_format values not implemented yet
        # would only need to switch prefix for building synonym dictionary 
        # hgvs
        # id (CA ID or PA ID)
        # MyVariantInfo_hg19.id
        # MyVariantInfo_hg38.id 
        # ExAC.id
        # gnomAD.id
        if not variant_list: 
            return {}

        separator = '\n'
        synonym_dictionary = {}
        batches = ceil(len(variant_list) / 2000)
        for i in range(batches):
            variant_subset = variant_list[i*2000:i*2000+2000]
            hgvs_pseudo_file = separator.join(variant_subset)
            query_url = f'{self.url}alleles?file={variant_format}&{self.synon_fields_param}'
            all_alleles_json = self.query_service(query_url, data=hgvs_pseudo_file)
            for index, allele_json in enumerate(all_alleles_json):
                hgvs_id = f'HGVS:{variant_subset[index]}'
                synonym_dictionary[hgvs_id] = self.parse_allele_json_for_synonyms(allele_json)
        
        return synonym_dictionary
        
    def get_synonyms_by_caid(self, caid):
        synonyms = set()
        query_url = f'{self.url}allele/{caid}?{self.synon_fields_param}'
        allele_json = self.query_service(query_url)
        if allele_json:
            synonyms.update(self.parse_allele_json_for_synonyms(allele_json))
        return synonyms

    def get_synonyms_by_hgvs(self, hgvs_id):
        synonyms = set()
        query_url = f'{self.url}allele?hgvs={hgvs_id}&{self.synon_fields_param}'
        allele_json = self.query_service(query_url)
        if allele_json:
            synonyms.update(self.parse_allele_json_for_synonyms(allele_json))
        return synonyms

    def get_synonyms_by_rsid_with_sequence(self, rsid, actual_sequence):
        if rsid.startswith('rs'):
            rsid = rsid[2:]

        return self.get_synonyms_by_parameter_matching('dbSNP.rs', rsid, match_sequence=actual_sequence)

    def get_synonyms_by_other_ids(self, variant_node):
        # Just looking for 1 hit because any hit should return all of the available synonyms and caid if they exist
        hgvs_ids = variant_node.get_synonyms_by_prefix('HGVS')
        if hgvs_ids:
            for hgvs_id in hgvs_ids:
                synonyms = self.get_synonyms_by_hgvs(Text.un_curie(hgvs_id))
                if synonyms: return synonyms
        
        dbsnp_ids = variant_node.get_synonyms_by_prefix('DBSNP')
        if dbsnp_ids:
            for dbsnp_id in dbsnp_ids:
                rsid = Text.un_curie(dbsnp_id)
                if rsid.startswith('rs'):
                    synonyms = self.get_synonyms_by_parameter_matching('dbSNP.rs', rsid[2:])
                    if synonyms: return synonyms

        clinvar_ids = variant_node.get_synonyms_by_prefix('CLINVARVARIANT')
        if clinvar_ids:
            for clinvar_id in clinvar_ids:
                synonyms = self.get_synonyms_by_parameter_matching('ClinVar.variationId', Text.un_curie(clinvar_id))
                if synonyms: return synonyms
        
        myvariant_hg38_ids = variant_node.get_synonyms_by_prefix('MYVARIANT_HG38')
        if myvariant_hg38_ids:
            for myvariant_id in myvariant_hg38_ids:
                synonyms = self.get_synonyms_by_parameter_matching( 'MyVariantInfo_hg38.id', Text.un_curie(myvariant_id))
                if synonyms: return synonyms

        myvariant_hg19_ids = variant_node.get_synonyms_by_prefix('MYVARIANT_HG19')
        if myvariant_hg19_ids:
            for myvariant_id in myvariant_hg19_ids:
                synonyms = self.get_synonyms_by_parameter_matching('MyVariantInfo_hg19.id', Text.un_curie(myvariant_id))
                if synonyms: return synonyms

        return set()

    def get_synonyms_by_parameter_matching(self, url_param, url_param_value, match_sequence=None):
        synonyms = set()
        query_url = f'{self.url}alleles?{url_param}={url_param_value}&{self.synon_fields_param}'
        query_json = self.query_service(query_url)
        for allele_json in query_json:
            synonyms.update(self.parse_allele_json_for_synonyms(allele_json, match_sequence))
        return synonyms

    def parse_allele_json_for_synonyms(self, allele_json, match_sequence=None):
        synonyms = set()
        try:
            variant_caid = allele_json['@id'].rsplit('/', 1)[1]
        except (KeyError, IndexError):
            return synonyms

        synonyms.add(LabeledID(identifier=f'CAID:{variant_caid}', label=f'{variant_caid}'))
        
        if 'genomicAlleles' in allele_json:
            try:
                for genomic_allele in allele_json['genomicAlleles']:
                    for hgvs_id in genomic_allele['hgvs']:
                        synonyms.add(LabeledID(identifier=f'HGVS:{hgvs_id}', label=f'{hgvs_id}'))
                    if 'referenceGenome' in genomic_allele and genomic_allele['referenceGenome'] == 'GRCh38':
                        # this is put on hold 
                        # this doesn't match the sequence, bail
                        #if match_sequence and match_sequence != sequence:
                        #    return set()
                        
                        if 'chromosome' in genomic_allele:
                            sequence = genomic_allele['coordinates'][0]['allele']
                            chromosome = genomic_allele['chromosome']
                            start_position = genomic_allele['coordinates'][0]['start']
                            end_position = genomic_allele['coordinates'][0]['end']
                            robokop_variant_id = f'HG38|{chromosome}|{start_position}|{end_position}|{sequence}'
                            synonyms.add(LabeledID(identifier=f'ROBO_VARIANT:{robokop_variant_id}', label=robokop_variant_id))

            except KeyError as e:
                logger.info(f'parsing sequence variant synonym - genomicAlleles KeyError for {variant_caid}: {e}')
                            
        if 'externalRecords' in allele_json:
            if 'MyVariantInfo_hg19' in allele_json['externalRecords']:
                for myvar_json in allele_json['externalRecords']['MyVariantInfo_hg19']:
                    myvariant_id = myvar_json['id']
                    synonyms.add(LabeledID(identifier=f'MYVARIANT_HG19:{myvariant_id}', label=f'{myvariant_id}'))

            if 'MyVariantInfo_hg38' in allele_json['externalRecords']:
                for myvar_json in allele_json['externalRecords']['MyVariantInfo_hg38']:
                    myvariant_id = myvar_json['id']
                    synonyms.add(LabeledID(identifier=f'MYVARIANT_HG38:{myvariant_id}', label=f'{myvariant_id}'))
    
            if 'dbSNP' in allele_json['externalRecords']:
                for dbsnp_json in allele_json['externalRecords']['dbSNP']:
                    variant_rsid = dbsnp_json['rs']
                    synonyms.add(LabeledID(identifier=f'DBSNP:rs{variant_rsid}', label=f'rs{variant_rsid}'))

            if 'ClinVarVariations' in allele_json['externalRecords']:
                for clinvar_json in allele_json['externalRecords']['ClinVarVariations']:
                    clinvar_id = clinvar_json['variationId']
                    synonyms.add(LabeledID(identifier=f'CLINVARVARIANT:{clinvar_id}', label=f'{clinvar_id}'))

        return synonyms

    def gene_to_sequence_variant(self, gene_node):
        curie_gene_symbols = gene_node.get_synonyms_by_prefix('HGNC.SYMBOL')
        if not curie_gene_symbols:
            logger.warn('HGNC.SYMBOL synonyms not found!!!')

        return_results = []
        for currie_gene_symbol in curie_gene_symbols:
            counter = 0
            gene_symbol = Text.un_curie(currie_gene_symbol)
            query_url_main = f'{self.url}alleles?gene={gene_symbol}&fields=none+@id&limit=2000&skip='
            while True:
                query_url = f'{query_url_main}{counter}'
                query_results = self.query_service(query_url)
                if query_results:
                    for allele_json in query_results:
                        if '@id' in allele_json:
                            id_split = allele_json['@id'].rsplit('/', 1)
                            if (len(id_split) > 1) and ('CA' in id_split[1]):
                                variant_caid = id_split[1]
                                variant_node = KNode(f'CAID:{variant_caid}', type=node_types.SEQUENCE_VARIANT)
                                predicate = LabeledID(identifier=f'clingen.gene_to_sequence_variant',label=f'gene_to_sequence_variant')
                                edge = self.create_edge(gene_node, variant_node, 'clingen.gene_to_sequence_variant', currie_gene_symbol, predicate, url=query_url)
                                return_results.append((edge, variant_node))
                    counter += 2000
                else:
                    break
        return return_results

    def get_variants_by_region(self, reference_sequence_label, center_position, region_size):
        flanking_size = int(region_size / 2)
        begin = center_position - flanking_size
        if begin < 0: begin = 0
        end = center_position + flanking_size
        query_url_main = f'{self.url}alleles?refseq={reference_sequence_label}&begin={begin}&end={end}&fields=none+@id&limit=2000&skip='
        counter = 0
        return_results = []
        while True:
            query_url = f'{query_url_main}{counter}'
            query_results = self.query_service(query_url)
            if query_results:
                for allele_json in query_results:
                    if '@id' in allele_json:
                        id_split = allele_json['@id'].rsplit('/', 1)
                        if (len(id_split) > 1) and ('CA' in id_split[1]):
                            variant_caid = id_split[1]
                            variant_node = KNode(f'CAID:{variant_caid}', type=node_types.SEQUENCE_VARIANT)
                            return_results.append(variant_node)
                counter += 2000
            else:
                break
        return return_results

    def query_service(self, query_url, data=None):
        if data:
            query_response = requests.post(query_url, data=data)
        else:
            query_response = requests.get(query_url)
        if query_response.status_code != 200:
            logger.warning(f'ClinGen returned a non-200 response({query_response.status_code}) calling ({query_url})')
            return {}
        else:
            query_json = query_response.json()
            return query_json

