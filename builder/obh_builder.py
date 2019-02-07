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
        self.concept_model = rosetta.type_graph.concept_model

        # for files that come in without real ids
        # populate this with the labels they do have and their real IDs if we can find them
        self.metabolite_labled_id_lookup = {}

    def create_gwas_graph(self, source_nodes, gwas_file_names, gwas_file_directory, p_value_cutoff, p_value_median_threshold=0.525, max_hits=10000, reference_genome='GRCh37', reference_patch='p1'):
        variants_processed = 0
        predicate = LabeledID(identifier=f'RO:0002609', label=f'related_to')
        pool = Pool(processes=10)

        for source_node in source_nodes:
            filepath = f'{gwas_file_directory}/{gwas_file_names[source_node.id]}'
            if not self.quality_control_check(filepath, p_value_cutoff, p_value_median_threshold, max_hits, delimiter='\t'):
                continue
            identifiers, p_values = self.get_hgvs_identifiers_from_gwas(filepath, p_value_cutoff, reference_genome, reference_patch)
            if len(identifiers) > 0:
                self.rosetta.synonymizer.synonymize(source_node)
                with BufferedWriter(self.rosetta) as writer:
                    writer.write_node(source_node)

                for seq_var_id in identifiers:
                    p_value = p_values.get(seq_var_id.identifier)
                    self.write_new_association(source_node, seq_var_id, node_types.SEQUENCE_VARIANT, predicate, p_value)
                    
                partial_run_one = partial(find_connections, node_types.SEQUENCE_VARIANT, node_types.GENE)
                pool.map(partial_run_one, identifiers)

                partial_run_one = partial(find_connections, node_types.SEQUENCE_VARIANT, node_types.DISEASE_OR_PHENOTYPIC_FEATURE)
                pool.map(partial_run_one, identifiers)

                variants_processed += len(identifiers)

        pool.close()
        pool.join()

        logger.info(f'create_gwas_graph complete - {variants_processed} significant variants found and processed.')

    def get_hgvs_identifiers_from_gwas(self, gwas_filepath, p_value_cutoff, reference_genome, reference_patch):
        new_ids = []
        corresponding_p_values = {}
        try:
            with open(gwas_filepath) as f:
                headers = next(f).split()
                line_counter = 0
                try:
                    pval_index = headers.index('PVALUE')
                    chrom_index = headers.index('CHROM')
                    pos_index = headers.index('POS')
                    ref_index = headers.index('REF')
                    alt_index = headers.index('ALT')
                except ValueError:
                    logger.warning(f'Error reading file headers for {gwas_filepath}')
                    return new_ids, corresponding_p_values

                for line in f:
                    try:
                        line_counter += 1
                        data = line.split()
                        p_value_string = data[pval_index]
                        if (p_value_string != 'NA'):
                            p_value = float(p_value_string)
                            if (p_value <= p_value_cutoff):
                                chromosome = data[chrom_index]
                                position = data[pos_index]
                                ref_allele = data[ref_index]
                                alt_allele = data[alt_index]
                                hgvs = self.convert_vcf_to_hgvs(reference_genome, reference_patch, chromosome, position, ref_allele, alt_allele)
                                if hgvs:
                                    curie_hgvs = f'HGVS:{hgvs}'
                                    new_ids.append(LabeledID(identifier=curie_hgvs, label=f'Variant(hgvs): {hgvs}'))
                                    corresponding_p_values[curie_hgvs] = p_value

                    except (IndexError, ValueError) as e:
                        logger.warning(f'Error reading file {gwas_filepath}, on line {line_counter}: {e}')

        except IOError:
            logger.warning(f'Could not open file: {gwas_filepath}')

        return new_ids, corresponding_p_values

    def convert_vcf_to_hgvs(self, reference_genome, reference_patch, chromosome, position, ref_allele, alt_allele):
        try:
            ref_chrom_version = self.reference_chrom_versions[reference_genome][reference_patch][chromosome]
            ref_prefix = self.reference_prefixes[reference_genome]

        except KeyError:
            logger.warning(f'Reference chromosome and/or version not found: {reference_genome}.{reference_patch},{chromosome}')
            return ''

        if chromosome == 'X':
            chromosome = '23'
        elif chromosome == 'Y':
            chromosome = '24'
        elif len(chromosome) == 1:
            ref_prefix = f'{ref_prefix}0'

        len_ref =  len(ref_allele) 
        len_alt = len(alt_allele)
            
        if len_alt == 1 or not alt_allele:
            # deletions
            if alt_allele == '.' or not alt_allele:
                if len_ref is 1:
                    variation = f'{position}del'
                else:
                    variation = f'{position}_{int(position)+len_ref-1}del'
            elif len_ref == 2 and (ref_allele[0] == alt_allele[0]):
                variation = f'{int(position)+1}del'
            elif len_ref > 2 and (ref_allele[0] == alt_allele[0]):
                variation = f'{int(position)+1}_{int(position)+len_ref-1}del'
            else:
                # substitutions
                variation = f'{position}{ref_allele}>{alt_allele}'

        # insertions
        elif (len_alt > len_ref) and (len_ref == 1) and (ref_allele[0] == alt_allele[0]):
            variation = f'{position}_{int(position)+1}ins{alt_allele[1:]}'

        else:
            logger.warning(f'Format of variant not recognized for hgvs conversion: {ref_allele} to {alt_allele}')
            return ''

        hgvs = f'{ref_prefix}{chromosome}.{ref_chrom_version}:g.{variation}'
        return hgvs

    def create_mwas_graph(self, source_nodes, mwas_file_names, mwas_file_directory, p_value_cutoff):
        metabolites_processed = 0
        predicate = LabeledID(identifier=f'RO:0002609', label=f'related_to')
        pool = Pool(processes=8)

        for source_node in source_nodes:
            filepath = f'{mwas_file_directory}/{mwas_file_names[source_node.id]}'
            identifiers, p_values = self.get_metabolite_identifiers_from_mwas(filepath, p_value_cutoff)
            if len(identifiers) > 0:
                self.rosetta.synonymizer.synonymize(source_node)
                with BufferedWriter(self.rosetta) as writer:
                    writer.write_node(source_node)

                for metabolite_id in identifiers:
                    p_value = p_values.get(metabolite_id.identifier)
                    self.write_new_association(source_node, metabolite_id, node_types.CHEMICAL_SUBSTANCE, predicate, p_value)
                    
                partial_run_one = partial(find_connections, node_types.CHEMICAL_SUBSTANCE, node_types.GENE)
                pool.map(partial_run_one, identifiers)

                partial_run_one = partial(find_connections, node_types.CHEMICAL_SUBSTANCE, node_types.DISEASE_OR_PHENOTYPIC_FEATURE)
                pool.map(partial_run_one, identifiers)

                metabolites_processed += len(identifiers)

        pool.close()
        pool.join()

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

    def write_new_association(self, source_node, associated_node_id, associated_node_type, predicate, p_value):
        
        associated_node = KNode(associated_node_id.identifier, name=associated_node_id.label, type=associated_node_type)
        self.rosetta.synonymizer.synonymize(associated_node)

        if self.concept_model:
            standard_predicate = self.concept_model.standardize_relationship(predicate)
        else:
            logger.warning('obesity builder: concept_model was missing, predicate standardization failed')
            standard_predicate = predicate

        props={'p_value': p_value}
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

    def quality_control_check(self, file_path, p_value_threshold, p_value_median_threshold, max_hits, delimiter=',', p_value_column='PVALUE'):
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

                if median(p_values) > p_value_median_threshold:
                        return False

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

    #metabolites_file = './sample_metabolites.csv'
    #gwas_directory = '.'

    #create a graph with just one node / file
    #p_value_cutoff = 1e-5
    #pa_id = 'EFO:0003940'
    #pa_node = KNode(pa_id, name='Physical Activity', type=node_types.DISEASE_OR_PHENOTYPIC_FEATURE)
    #associated_nodes = [pa_node]
    #associated_file_names = {pa_id:''}
    #gwas_directory = '.'
    #obh.create_gwas_graph(associated_nodes, associated_file_names, gwas_directory, p_value_cutoff)

    #create a graph with a file of many nodes (optional, sort the nodes first)
    #metabolites_file = '/home/emorris/metabolite_info.csv'
    #metabolite_nodes, metabolite_file_names = obh.load_metabolite_info(metabolites_file, file_names_postfix="_scale")

    #ordered_names = get_ordered_names_from_csv('/projects/sequence_analysis/vol1/obesity_hub/PA/MWAS/SOL_metabolomics_std_GPAQ_PAG2008YN_09132018_sorted.csv', 'TRAIT')
    #ordered_metabolite_nodes = []
    #for name in ordered_names:
    #    for node in metabolite_nodes:
    #        if metabolite_file_names[node.id] == name:
    #            ordered_metabolite_nodes.append(node)
    #            continue

    # this is run twice due to having truncated names for the actual file names 
    #throwaway_nodes, real_file_names = obh.load_metabolite_info(metabolites_file, file_names_postfix="_scale", file_name_truncation=32)
    #gwas_directory = '/projects/sequence_analysis/vol1/obesity_hub/metabolomics/aggregate_results'
    #obh.create_gwas_graph(ordered_metabolite_nodes, real_file_names, gwas_directory, 1e-10, p_value_median_threshold=0.525, max_hits=10000)
    
    # create a mwas graph
    #p_value_cutoff = 1e-5
    #metabolites_file = '/home/emorris/metabolite_info.csv'
    #pa_id = 'EFO:0003940'
    #pa_node = KNode(pa_id, name='Physical Activity', type=node_types.DISEASE_OR_PHENOTYPIC_FEATURE)
    #associated_nodes = [pa_node]
    #associated_file_names = {pa_id:'sample_mwas'}
    #mwas_directory = '.'
    #metabolite_nodes, metabolite_file_names = obh.load_metabolite_info(metabolites_file, file_names_postfix="_scale")
    #obh.create_mwas_graph(associated_nodes, associated_file_names, mwas_directory, p_value_cutoff)


