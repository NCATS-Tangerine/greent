from greent.rosetta import Rosetta
from greent import node_types
from greent.util import LoggingUtil
from greent.graph_components import KNode, KEdge
from builder.buildmain import run
from builder.question import LabeledID
from multiprocessing import Pool
from greent.export import BufferedWriter
from functools import partial
from statistics import median
import logging, time, csv

logger = LoggingUtil.init_logging(__name__, level=logging.DEBUG)

class ObesityHubBuilder(object):

    reference_chrom_versions = {
        'GRCh37': {
            'p1': {
                '1': '10',
                '2': '11',
                '3': '11',
                '4': '11',
                '5': '9',
                '6': '11',
                '7': '13',
                '8': '10',
                '9': '11',
                '10': '10',
                '11': '9',
                '12': '11',
                '13': '10',
                '14': '8',
                '15': '9',
                '16': '9',
                '17': '10',
                '18': '9',
                '19': '9',
                '20': '10',
                '21': '8',
                '22': '10',
                '23': '10',
                '24': '9',
                'X': '10',
                'Y': '9'
            }
        },
        'GRCh38': {
            'p1': {
                '1': '11',
                '2': '12',
                '3': '12',
                '4': '12',
                '5': '10',
                '6': '12',
                '7': '14',
                '8': '11',
                '9': '12',
                '10': '11',
                '11': '10',
                '12': '12',
                '13': '11',
                '14': '9',
                '15': '10',
                '16': '10',
                '17': '11',
                '18': '10',
                '19': '10',
                '20': '11',
                '21': '9',
                '22': '11',
                '23': '11',
                '24': '10',
                'X': '11',
                'Y': '10'
            }
        }
    }

    reference_prefixes = {
        'GRCh37': 'NC_0000',
        'GRCh38': 'NC_0000'
    }

    def __init__(self, rosetta, debug=False):
        self.rosetta = rosetta
        self.cache = rosetta.cache
        self.clingen = rosetta.core.clingen
        self.gwascatalog = rosetta.core.gwascatalog
        self.myvariant = rosetta.core.myvariant
        self.concept_model = rosetta.type_graph.concept_model

        # for files that come in without real ids
        # populate this with the labels they do have and their real IDs if we can find them
        self.metabolite_labled_id_lookup = {}

    def create_gwas_graph(self, source_nodes, gwas_file_names, gwas_file_directory, p_value_cutoff, p_value_median_threshold=0.525, max_hits=100000, reference_genome='GRCh37', reference_patch='p1', analysis_id=None):
        self.prepopulate_gwascatalog_cache()
        variants_processed = 0
        predicate = LabeledID(identifier=f'RO:0002609', label=f'related_to')
        pool = Pool(processes=10)
        for source_node in source_nodes:
            try:
                filepath = f'{gwas_file_directory}/{gwas_file_names[source_node.id]}'
            except KeyError:
                logger.warning('create_gwas_graph bad filename look-up')
            #if not self.quality_control_check(filepath, p_value_cutoff, p_value_median_threshold, max_hits, delimiter='\t'):
            #    logger.debug(f'GWAS File: {gwas_file_names[source_node.id]} did not pass QC.')
            #    continue

            variant_info = self.get_variants_from_gwas(filepath, p_value_cutoff, reference_genome, reference_patch)
            if len(variant_info) > 0:
                self.rosetta.synonymizer.synonymize(source_node)
                with BufferedWriter(self.rosetta) as writer:
                    writer.write_node(source_node)

                labled_variant_ids = []
                uncached_variant_annotation_nodes = []
                for hgvs_id, p_value in variant_info:
                    curie_hgvs = f'HGVS:{hgvs_id}'
                    p_value = p_values.get(hgvs_id)
                    variant_node = KNode(curie_hgvs, name=hgvs_id, type=node_types.SEQUENCE_VARIANT)
                    self.rosetta.synonymizer.synonymize(variant_node)
                    labled_variant_ids.append(LabeledID(identifier=variant_node.id, label=variant_node.name))
                    new_edge = self.write_new_association(source_node, variant_node, predicate, p_value, analysis_id=analysis_id)

                    if self.cache.get(f'myvariant.sequence_variant_to_gene({variant_node.id})') is None:
                        uncached_variant_annotation_nodes.append(variant_node)
                        # this is a pretty arbitrary chunk size, should this be parallelized?
                        if len(uncached_variant_annotation_nodes) == 1000:
                            self.prepopulate_variant_annotation_cache(uncached_variant_annotation_nodes)
                            uncached_variant_annotation_nodes = []

                if uncached_variant_annotation_nodes:
                    self.prepopulate_variant_annotation_cache(uncached_variant_annotation_nodes)

                if labled_variant_ids:
                    partial_run_one = partial(find_connections, node_types.SEQUENCE_VARIANT, node_types.GENE)
                    pool.map(partial_run_one, labled_variant_ids)
                
                    partial_run_one = partial(find_connections, node_types.SEQUENCE_VARIANT, node_types.DISEASE_OR_PHENOTYPIC_FEATURE)
                    pool.map(partial_run_one, labled_variant_ids)
                    
                    variants_processed += len(labled_variant_ids)

        pool.close()
        pool.join()

        logger.info(f'create_gwas_graph complete - {variants_processed} significant variants found and processed.')

        return variants_processed

    def get_variants_from_gwas(self, gwas_filepath, p_value_cutoff, reference_genome, reference_patch, impute2_cutoff=0.5, alt_af_min=0.01, alt_af_max=0.99):
        results = []
        try:
            with open(gwas_filepath) as f:
                headers = next(f).split()
                try:
                    pval_index = headers.index('PVALUE')
                    chrom_index = headers.index('CHROM')
                    pos_index = headers.index('POS')
                    ref_index = headers.index('REF')
                    alt_index = headers.index('ALT')
                except ValueError:
                    logger.warning(f'Error reading file headers for {gwas_filepath}')
                    return variants

                if 'ALT_AF' in headers:
                    alt_af_index = headers.index('ALT_AF')
                else:
                    alt_af_index = None

                if 'IMPUTE2_INFO' in headers:
                    impute2_index = headers.index('IMPUTE2_INFO')
                else:
                    impute2_index = None

                line_counter = 1
                for line in f:
                    try:
                        line_counter += 1
                        data = line.split()
                        # if no p value throw it out
                        p_value_string = data[pval_index]
                        if (p_value_string != 'NA'):
                            p_value = float(p_value_string)
                            if p_value <= p_value_cutoff:
                                if impute2_index is not None:
                                    impute2_score = float(data[impute2_index])
                                if ((impute2_index is None) or (impute2_score >= impute2_cutoff)):
                                    if alt_af_index is not None:
                                        alt_af_freq = float(data[alt_af_index])
                                    if ((alt_af_index is None) or (alt_af_min <= alt_af_freq <= alt_af_max)):
                                        chromosome = data[chrom_index]
                                        position = int(data[pos_index])
                                        ref_allele = data[ref_index]
                                        alt_allele = data[alt_index]
                                        hgvs = self.convert_vcf_to_hgvs(reference_genome, reference_patch, chromosome, position, ref_allele, alt_allele)
                                        if hgvs:
                                            results.append((hgvs, p_value))

                    except (IndexError, ValueError) as e:
                        logger.warning(f'Error reading file {gwas_filepath}, on line {line_counter}: {e}')

        except IOError:
            logger.warning(f'Could not open file: {gwas_filepath}')

        return results

    def convert_vcf_to_hgvs(self, reference_genome, reference_patch, chromosome, position, ref_allele, alt_allele):
        try:
            ref_chrom_version = self.reference_chrom_versions[reference_genome][reference_patch][chromosome]
            ref_prefix = self.reference_prefixes[reference_genome]
        except KeyError:
            logger.warning(f'Reference chromosome and/or version not found: {reference_genome}.{reference_patch},{chromosome}')
            return ''
        
        len_ref = len(ref_allele)
        if alt_allele == '.':
            # deletions
            if len_ref == 1:
                variation = f'{position}del'
            else:
                variation = f'{position}_{position+len_ref-1}del'

        elif alt_allele.startswith('<'):
            # we know about these but don't support them yet
            return ''

        else:
            len_alt = len(alt_allele)
            if (len_ref == 1) and (len_alt == 1):
                # substitutions
                variation = f'{position}{ref_allele}>{alt_allele}'
            elif (len_alt > len_ref) and alt_allele.startswith(ref_allele):
                # insertions
                diff = len_alt - len_ref
                offset = len_alt - diff 
                variation = f'{position+offset-1}_{position+offset}ins{alt_allele[offset:]}'
            elif (len_ref > len_alt) and ref_allele.startswith(alt_allele):
                # deletions
                diff = len_ref - len_alt
                offset = len_ref - diff
                if diff == 1:
                    variation = f'{position+offset}del'
                else:
                    variation = f'{position+offset}_{position+offset+diff-1}del'

            else:
                logger.warning(f'Format of variant not recognized for hgvs conversion: {ref_allele} to {alt_allele}')
                return ''

        # assume vcf has integers and not X or Y for now
        #if chromosome == 'X':
        #    chromosome = '23'
        #elif chromosome == 'Y':
        #    chromosome = '24'
        
        if len(chromosome) == 1:
            chromosome = f'0{chromosome}'

        hgvs = f'{ref_prefix}{chromosome}.{ref_chrom_version}:g.{variation}'
        return hgvs

    def prepopulate_gwascatalog_cache(self):
        self.gwascatalog.prepopulate_cache()

    def prepopulate_variant_cache(self, hgvs_file_path, reference_genome='GRCh37', reference_patch='p1'):
        variant_info = self.get_variants_from_gwas(hgvs_file_path, 1, reference_genome, reference_patch, impute2_cutoff=0, alt_af_min=0, alt_af_max=1)
        batch_of_ids = []
        for hgvs_id, p_value in variant_info:
            cached = self.cache.get(f'synonymize(HGVS:{hgvs_id})')
            if cached is None:
                batch_of_ids.append(hgvs_id)

            # this is a pretty arbitrary chunk size, should this be parallelized?
            if len(batch_of_ids) == 10000:
                self.process_variant_synonymization_cache(batch_of_ids)
                batch_of_ids = []
                
        if batch_of_ids:
            self.process_variant_synonymization_cache(batch_of_ids)

        return len(variant_info)

    def process_variant_synonymization_cache(self, batch_of_hgvs):
        batch_synonyms = self.clingen.get_batch_of_synonyms(batch_of_hgvs)
        for hgvs_id, synonyms in batch_synonyms.items():
            key = f'synonymize({hgvs_id})'
            self.cache.set(key, synonyms)

            dbsnp_labled_ids = []
            caid_labled_id = None
            for syn in synonyms:
                if syn.identifier.startswith('DBSNP'):
                    dbsnp_labled_ids.append(syn)
                elif syn.identifier.startswith('CAID'):
                    caid_labled_id = syn

            if caid_labled_id:
                synonyms.remove(caid_labled_id)
                self.cache.set(f'synonymize({caid_labled_id.identifier})', synonyms)
                synonyms.add(caid_labled_id)

            for dbsnp_labled_id in dbsnp_labled_ids:
                synonyms.remove(dbsnp_labled_id)
                self.cache.set(f'synonymize({dbsnp_labled_id.identifier})', synonyms)
                synonyms.add(dbsnp_labled_id)

    def prepopulate_variant_annotation_cache(self, batch_of_nodes):
        batch_annotations = self.myvariant.batch_sequence_variant_to_gene(batch_of_nodes)
        for seq_var_id, annotations in batch_annotations.items():
            key = f'myvariant.sequence_variant_to_gene({seq_var_id})'
            self.cache.set(key, annotations)

    def create_mwas_graph(self, source_nodes, mwas_file_names, mwas_file_directory, p_value_cutoff, analysis_id=None):
        metabolites_processed = 0
        predicate = LabeledID(identifier=f'RO:0002609', label=f'related_to')
        #pool = Pool(processes=8)

        for source_node in source_nodes:
            filepath = f'{mwas_file_directory}/{mwas_file_names[source_node.id]}'
            identifiers, p_values = self.get_metabolite_identifiers_from_mwas(filepath, p_value_cutoff)
            if identifiers:
                self.rosetta.synonymizer.synonymize(source_node)
                with BufferedWriter(self.rosetta) as writer:
                    writer.write_node(source_node)

                labled_metabolite_ids = []
                for metabolite_id in identifiers:
                    p_value = p_values.get(metabolite_id.identifier)
                    metabolite_node = KNode(metabolite_id.identifier, name=metabolite_id.label, type=node_types.CHEMICAL_SUBSTANCE)
                    self.rosetta.synonymizer.synonymize(metabolite_node)
                    labled_metabolite_ids.append(LabeledID(identifier=metabolite_node.id, label=metabolite_node.name))
                    new_edge = self.write_new_association(source_node, metabolite_node, predicate, p_value, analysis_id=analysis_id)

                    self.write_new_association(source_node, metabolite_node, predicate, p_value)
                
                #partial_run_one = partial(find_connections, node_types.CHEMICAL_SUBSTANCE, node_types.GENE)
                #pool.map(partial_run_one, labled_metabolite_ids)

                #partial_run_one = partial(find_connections, node_types.CHEMICAL_SUBSTANCE, node_types.DISEASE_OR_PHENOTYPIC_FEATURE)
                #pool.map(partial_run_one, labled_metabolite_ids)

                metabolites_processed += len(labled_metabolite_ids)

        #pool.close()
        #pool.join()

        logger.info(f'create_mwas_graph complete - {metabolites_processed} significant metabolites found and processed.')

    def load_metabolite_info(self, metabolites_file_path, file_names_postfix='', file_name_truncation=None):
               
        metabolite_nodes = []
        file_names_by_id = {}
        try:
            with open(metabolites_file_path) as f:
                csv_reader = csv.reader(f)
                headers = next(csv_reader)
                line_counter = 0
                try:
                    label_index = headers.index('biochemical')
                    file_name_index = headers.index('metabolite_std')
                    hmdb_index = headers.index('hmdb')
                    pubchem_index = headers.index('pubchem')
                    kegg_index = headers.index('kegg')
                except ValueError:
                    logger.warning(f'Error reading file headers for {metabolites_file_path}')
                    return metabolite_nodes, file_names_by_id

                for data in csv_reader:
                    try:
                        line_counter += 1 
                        if data[pubchem_index] != '#N/A':
                            m_id = f'PUBCHEM:{data[pubchem_index]}'
                        elif data[hmdb_index] != '#N/A':
                            m_id = f'HMDB:{data[hmdb_index]}'
                        elif data[kegg_index] != '#N/A':
                            m_id = f'KEGG.COMPOUND:{data[kegg_index]}'
                        else:
                            continue

                        m_label = data[label_index]
                        m_filename = f'{data[file_name_index]}{file_names_postfix}'
                        # temporary workaround for truncated incorrect names
                        m_filename = m_filename[0:file_name_truncation]
                        file_names_by_id[m_id] = m_filename

                        new_metabolite_node = KNode(m_id, name=m_label, type=node_types.CHEMICAL_SUBSTANCE)
                        metabolite_nodes.append(new_metabolite_node)

                        self.metabolite_labled_id_lookup[m_filename] = LabeledID(identifier=m_id, label=m_label)

                    except (KeyError) as e:
                        logger.warning(f'metabolites_file ({metabolites_file_path}) could not be parsed: {e}')

        except (IOError) as e:
            logger.warning(f'metabolites_file ({metabolites_file_path}) could not be loaded: {e}')

        return metabolite_nodes, file_names_by_id

    def get_metabolite_identifiers_from_mwas(self, mwas_filepath, p_value_cutoff):
        metabolite_ids = []
        corresponding_p_values = {}
        try:
            with open(mwas_filepath) as f:
                csv_reader = csv.reader(f)
                headers = next(csv_reader)
                line_counter = 0
                name_index = -1
                pval_index = -1
                for header in headers:
                    if header == 'TRAIT':
                        name_index = headers.index(header)
                    elif ('pval' in header.lower()) or ('pvalue' in header.lower()):
                        pval_index = headers.index(header)

                if (name_index < 0) or (pval_index < 0):
                    logger.warning(f'Error reading file headers for {mwas_filepath} - {headers}')
                    return new_ids, corresponding_p_values

                for data in csv_reader:
                    try:
                        line_counter += 1
                        p_value_string = data[pval_index]
                        if (p_value_string != 'NA'):
                            p_value = float(p_value_string) 
                            if p_value <= p_value_cutoff:
                                m_name = data[name_index]
                                if m_name in self.metabolite_labled_id_lookup:
                                    m_labeled_id = self.metabolite_labled_id_lookup[m_name]
                                    metabolite_ids.append(m_labeled_id)
                                    corresponding_p_values[m_labeled_id.identifier] = p_value
                                else:
                                    logger.warning(f'Could not find real id for metabolite {m_name} in {mwas_filepath}')  

                    except IndexError as e:
                        logger.warning(f'Error parsing file {mwas_filepath}, on line {line_counter}: {e}')
                    except ValueError as e:
                        logger.warning(f'Error converting {p_value_string} to float in {mwas_filepath}')

        except IOError:
            logger.warning(f'Could not open file: {mwas_filepath}')

        return metabolite_ids, corresponding_p_values

    def write_new_association(self, source_node, associated_node, predicate, p_value, analysis_id=None):

        if self.concept_model:
            standard_predicate = self.concept_model.standardize_relationship(predicate)
        else:
            logger.warning('OBH builder: concept_model was missing, predicate standardization failed')
            standard_predicate = predicate

        props={'p_value': p_value}
        if analysis_id:
            props['analysis_id'] = analysis_id
        ctime = time.time()
        new_edge = KEdge(source_id=source_node.id,
                     target_id=associated_node.id,
                     provided_by='Obesity_Hub',
                     ctime=ctime,
                     original_predicate=predicate,
                     standard_predicate=standard_predicate,
                     input_id=source_node.id,
                     publications=None,
                     url=None,
                     properties=props)
        
        with BufferedWriter(self.rosetta) as writer:
            writer.write_node(associated_node)
            writer.write_edge(new_edge)

        return new_edge

    def quality_control_check(self, file_path, p_value_threshold=0, max_hits=500000, delimiter=',', p_value_column='PVALUE'):
        try:
            with open(file_path) as f:
                csv_reader = csv.reader(f, delimiter=delimiter, skipinitialspace=True)
                headers = next(csv_reader)
                line_counter = 0
                significant_hits = 0
                p_values = []

                try:
                    p_value_index = headers.index(p_value_column)
                except ValueError:
                    logger.warning(f'QC Error reading file headers for {file_path}')
                    return False

                for data in csv_reader:
                    try:
                        line_counter += 1 
                        p_value = data[p_value_index]
                        if (p_value != 'NA'):
                            p_value = float(p_value)
                            p_values.append(p_value)
                            if (p_value <= p_value_threshold):
                                significant_hits += 1
                                if (significant_hits > max_hits):
                                    return False

                    except (ValueError, IndexError) as e:
                        logger.warning(f'QC file error ({file_path}) line {line_counter} could not be parsed: {e}')

        except (IOError) as e:
            logger.warning(f'QC check file ({file_path}) could not be loaded: {e}')
            return False
        except (csv.Error) as e:
            logger.warning(f'csv error in ({file_path}): {e}')

        return True

def find_connections(input_type, output_type, identifier):
    path = f'{input_type},{output_type}'
    run(path,identifier.label,identifier.identifier,None,None,None,'greent.conf')

def get_ordered_names_from_csv(file_path, name_header):
    ordered_names = []
    with open(file_path) as f:
        csv_reader = csv.reader(f, skipinitialspace=True)
        headers = next(csv_reader)
        try:
            name_index = headers.index(name_header)
        except ValueError:
            logger.warning(f'Error reading file headers for {file_path}')
            return []

        for line in csv_reader:
            try:
                ordered_names.append(line[name_index])
            except (ValueError, IndexError) as e:
                logger.warning(f'file error ({file_path}) could not be parsed: {e}')
    return ordered_names

if __name__=='__main__':

    obh = ObesityHubBuilder(Rosetta(), debug=True)
    #p_value_cutoff = 1e-5
    #gwas_directory = '/example_directory'
    #obesity_id = 'HP:0001513'
    #obesity_node = KNode(obesity_id, name='Obesity', type=node_types.DISEASE_OR_PHENOTYPIC_FEATURE)
    #associated_nodes = [obesity_node]
    #associated_file_names = {obesity_id: 'example_gwas_file'}
    #obh.create_gwas_graph(associated_nodes, associated_file_names, gwas_directory, p_value_cutoff, analysis_id='testing_gwas')
   