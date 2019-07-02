from greent.rosetta import Rosetta
from greent import node_types
from greent.util import Text, LoggingUtil
from greent.graph_components import KNode, KEdge
from builder.buildmain import run
from builder.question import LabeledID
from multiprocessing import Pool
from greent.export import BufferedWriter
from statistics import median
from collections import namedtuple
import logging, time, csv, tabix, pickle, gzip

logger = LoggingUtil.init_logging(__name__, level=logging.DEBUG)

SequenceVariant = namedtuple('SequenceVariant', ['hgvs', 'build', 'chrom', 'pos', 'ref', 'alt'])

class ObesityHubBuilder(object):

    reference_chrom_labels = {
        'HG19': {
            'p1': {
                1: 'NC_000001.10',
                2: 'NC_000002.11',
                3: 'NC_000003.11',
                4: 'NC_000004.11',
                5: 'NC_000005.9',
                6: 'NC_000006.11',
                7: 'NC_000007.13',
                8: 'NC_000008.10',
                9: 'NC_000009.11',
                10: 'NC_000010.10',
                11: 'NC_000011.9',
                12: 'NC_000012.11',
                13: 'NC_000013.10',
                14: 'NC_000014.8',
                15: 'NC_000015.9',
                16: 'NC_000016.9',
                17: 'NC_000017.10',
                18: 'NC_000018.9',
                19: 'NC_000019.9',
                20: 'NC_000020.10',
                21: 'NC_000021.8',
                22: 'NC_000022.10',
                23: 'NC_000023.10',
                24: 'NC_000024.9'
            }
        },
        'HG38': {
            'p1': {
                1: 'NC_000001.11',
                2: 'NC_000002.12',
                3: 'NC_000003.12',
                4: 'NC_000004.12',
                5: 'NC_000005.10',
                6: 'NC_000006.12',
                7: 'NC_000007.14',
                8: 'NC_000008.11',
                9: 'NC_000009.12',
                10: 'NC_000010.11',
                11: 'NC_000011.10',
                12: 'NC_000012.12',
                13: 'NC_000013.11',
                14: 'NC_000014.9',
                15: 'NC_000015.10',
                16: 'NC_000016.10',
                17: 'NC_000017.11',
                18: 'NC_000018.10',
                19: 'NC_000019.10',
                20: 'NC_000020.11',
                21: 'NC_000021.9',
                22: 'NC_000022.11',
                23: 'NC_000023.11',
                24: 'NC_000024.10'
            }
        }
    }

    def __init__(self, rosetta, debug=False):
        self.rosetta = rosetta
        self.cache = rosetta.cache
        self.clingen = rosetta.core.clingen
        self.gwascatalog = rosetta.core.gwascatalog
        self.myvariant = rosetta.core.myvariant
        self.ensembl = rosetta.core.ensembl
        self.concept_model = rosetta.type_graph.concept_model

        # for files that come in without real ids
        # populate this with the labels they do have and their real IDs if we can find them
        self.metabolite_labled_id_lookup = {}

    def create_gwas_graph(self,
                        gwas_to_process,
                        project_id=None,
                        reference_genome='HG19',
                        reference_patch='p1',
                        max_hits=100000,
                        max_p_value=None,
                        verbose=False):

        all_significant_variants, gwas_state = self.create_or_load_gwas_project(project_id)

        try:
            for gwas in gwas_to_process:
                gwas_filepath = gwas['filepath']
                if gwas_filepath in gwas_state['gwas_analyses']:
                    if gwas['p_value_cutoff'] > gwas_state['gwas_analyses'][gwas_filepath]['p_value_cutoff']:
                        gwas_state['gwas_analyses'][gwas_filepath]['p_value_cutoff'] = gwas['p_value_cutoff']
                        gwas_state['gwas_analyses'][gwas_filepath]['needsVariants'] = True
                else:
                    gwas['needsVariants'] = True
                    gwas['needsAssociations'] = True
                    gwas_state['gwas_analyses'][gwas_filepath] = gwas
        except KeyError as e:
            logger.debug(f'OBH_Error: gwas information provided not formatted correctly - {e}')

        #self.log_gwas_state(gwas_state)

        num_gwas = len(gwas_state['gwas_analyses'])

        # grab all of the significant variants from all of the gwas files that need it
        file_counter = 0
        new_variants = {}
        for gwas_key, gwas_info in gwas_state['gwas_analyses'].items():
            #if not self.quality_control_check(gwas_node.filepath, gwas_node.p_value_cutoff, max_hits, delimiter='\t'):
            #    logger.debug(f'GWAS File: {gwas_node.filepath} did not pass QC.')
            #    continue
            file_counter += 1
            if gwas_info['needsVariants']:
                logger.info(f'OBH_Info: finding variants - file {file_counter} of {num_gwas}.')
                foundVariants = self.find_significant_variants_in_gwas(gwas_info['filepath'], gwas_info['p_value_cutoff'], reference_genome, reference_patch, variant_dictionary=all_significant_variants, new_variant_dictionary=new_variants)
                # this is True or False, succesfully finding zero variants should still be True
                if foundVariants:
                    gwas_info['needsVariants'] = False
            else:
                gwas_info_filepath = gwas_info["filepath"]
                logger.info(f'OBH_Info: significant variants already found for {gwas_info_filepath}.')

        if project_id:
            if new_variants:
                self.save_gwas_project_variants(project_id, all_significant_variants)
            self.save_gwas_project_state(project_id, gwas_state)

        #self.log_gwas_state(gwas_state)
        num_variants_processed = 0

        if 'variants_need_reprocessing' in gwas_state['flags']:
            logger.info('OBH_Info: flag indicated all variants need processing')
             # walk through and create synonymized sequence variant nodes, do some precaching, go ahead and write LD variant nodes
            self.prepopulate_variant_synonymization_cache(all_significant_variants)
            num_variants_processed = self.process_gwas_variants(all_significant_variants, all_significant_variants)
            gwas_state['flags'].remove('variants_need_reprocessing')
        elif new_variants:
            gwas_state['flags'].add('variants_need_reprocessing')
            # only process the new variants - 
            # TODO we could save the new variants in their own file, 
            # right now this flag would trigger processing all of them again if it doesn't finish
            self.prepopulate_variant_synonymization_cache(new_variants)
            num_variants_processed = self.process_gwas_variants(new_variants, all_significant_variants)
            gwas_state['flags'].remove('variants_need_reprocessing')

        #logger.info('new variants:')
        #self.log_variant_dictionary(new_variants)
        #logger.info('all variants:')
        #self.log_variant_dictionary(all_significant_variants)

        # save the project
        if project_id:
            self.save_gwas_project_state(project_id, gwas_state)
            if num_variants_processed > 0:
                self.save_gwas_project_variants(project_id, all_significant_variants)

        # variant nodes and their knowledge items are already written
        # next go into the files and find/write the gwas associations
        num_bad_gwas = 0
        gwas_counter = 0
        with BufferedWriter(self.rosetta) as writer:
            for gwas_key, gwas_info in gwas_state['gwas_analyses'].items():
                gwas_counter += 1
                try:
                    gwas_node = gwas_info['node']
                    gwas_filepath = gwas_info['filepath']
                    needsAllAssociations = gwas_info['needsAssociations']
                except KeyError as e:
                    num_bad_gwas += 1
                    logger.info(f'OBH_Error: gwas state malformed: {gwas_info}, {e}')
                    continue

                if needsAllAssociations:
                    if Text.get_curie(gwas_node.id) != 'METABOLON':
                        self.rosetta.synonymizer.synonymize(gwas_node)
                    logger.info(f'OBH_Info: starting gwas associations {gwas_counter} of {num_gwas}: {gwas_node.id}')
                    writer.write_node(gwas_node)
                    success = self.process_gwas_associations(writer, gwas_node, gwas_filepath, all_significant_variants, max_p_value=max_p_value, verbose=verbose)
                    if success:
                        gwas_info['needsAssociations'] = False
                    else:
                        num_bad_gwas += 1
                else:
                    self.process_gwas_associations(writer, gwas_node, gwas_filepath, new_variants, max_p_value=max_p_value, verbose=verbose)

        if project_id:
            self.save_gwas_project_state(project_id, gwas_state)

        logger.info(f'OBH_Builder: create_gwas_graph complete - {num_variants_processed} new variants')
        logger.info(f'OBH_Builder: {num_gwas} gwas analyses, {num_bad_gwas} bad/missing gwas files.')

        return True

    def create_or_load_gwas_project(self, project_id):

        new_gwas_state = {'gwas_analyses' : {}, 'flags' : {'variants_need_reprocessing'}}

        logger.info(f'Attempting to load gwas project {project_id}')
        if project_id == None:
            logger.info('No project id specified, running project with no saving.')
            return {}, new_gwas_state

        try:
            cached_variants_file_path = f'./cache/obh_{project_id}_gwas_variants.p'
            with open(cached_variants_file_path, "rb") as variants_file:
                significant_variants = pickle.load(variants_file)
                if significant_variants:
                    logger.info(f'Loaded cached significant variants for {project_id}.')
                else:
                    significant_variants = {}

        except (OSError, IOError) as e:
            logger.debug(f'OBH GWAS Builder could not load cached variants {e}')
            significant_variants = {}

        try:
            cached_gwas_state_path = f'./cache/obh_{project_id}_gwas_state.p'
            with open(cached_gwas_state_path, "rb") as state_file:
                gwas_state = pickle.load(state_file)
                if gwas_state:
                    logger.info(f'Loaded cached gwas state for {project_id}.')
                else:
                    gwas_state = new_gwas_state

        except (OSError, IOError) as e:
            logger.debug(f'OBH GWAS Builder could not load cached gwas state {e}')
            gwas_state = new_gwas_state

        return significant_variants, gwas_state

    def save_gwas_project_variants(self, project_id, significant_variants):
        try:
            cached_variants_file_path = f'./cache/obh_{project_id}_gwas_variants.p'
            with open(cached_variants_file_path, "wb" ) as variants_file:
                pickle.dump(significant_variants, variants_file)
        except (OSError, IOError) as e:
            logger.error(f'Could not write significant variants cache dump!!! {e}')

    def save_gwas_project_state(self, project_id, gwas_state):
        try:
            cached_gwas_state_path = f'./cache/obh_{project_id}_gwas_state.p'
            with open(cached_gwas_state_path, "wb" ) as state_file:
                pickle.dump(gwas_state, state_file)
        except (OSError, IOError) as e:
            logger.error(f'Could not write gwas state cache dump!!! {e}')

    def log_variant_dictionary(self, variant_dictionary, verbose=False):
        logger_string = ''
        variant_count = 0
        for chromosome, position_dict in variant_dictionary.items():
            chromosome_variant_count = 0
            for position, variants in position_dict.items():
                for variant in variants:
                    variant_count += 1
                    chromosome_variant_count += 1
                    if verbose:
                        logger_string += f'{variant}, '

            logger.info(f'(chromosome {chromosome} had {chromosome_variant_count} variants)')
        logger_string = logger_string.rstrip(', ')
        logger.info(f'Variant Dictionary ({variant_count} total variants)')
        if logger_string:
            logger.info(f'{logger_string}')

    def log_gwas_state(self, gwas_state, verbose=False):
        logger_string = ''
        for value in gwas_state['flags']:
            logger_string += value + ', '

        logger_string = logger_string.rstrip(', ')
        
        if verbose:
            logger_string += '\n'
        analyses_count = 0
        for key, value in gwas_state['gwas_analyses'].items():
            analyses_count += 1
            if verbose:
                logger_string += f'{value}, '

        logger_string = logger_string.rstrip(', ')
        logger.info(f'gwas state had {analyses_count} analyses')
        logger.info(f'{logger_string}')

    def get_gwas_data_from_indexed_file(self, 
                                    filepath, 
                                    chromosome, 
                                    position_start, 
                                    position_end,
                                    tabix_handler=None):
        # "not sure why tabix needs position -1" - according to PyVCF Docs
        # seems to be true for now
        position_start -= 1
        #
        try:
            if not tabix_handler:
                tabix_handler = tabix.open(filepath)
            records = tabix_handler.query(chromosome, position_start, position_end)
            return records
        except tabix.TabixError as e:
            logger.info(f'OBH_Error: TabixError ({filepath}) chromosome({chromosome}) positions({position_start}-{position_end})')
            return None 

    def find_significant_variants_in_gwas(self, 
                                        gwas_filepath, 
                                        p_value_cutoff, 
                                        reference_genome, 
                                        reference_patch, 
                                        variant_dictionary={},
                                        new_variant_dictionary={},
                                        impute2_cutoff=0.5, 
                                        alt_af_min=0.01, 
                                        alt_af_max=0.99):
        sig_variants_found = 0
        sig_variants_failed_conversion = 0
        sig_variants_duplicates = 0
        try:
            if gwas_filepath.endswith('.gz'):
                isGzip = True
                f = gzip.open(gwas_filepath, mode='rt')
            else:
                isGzip = False
                f = open(gwas_filepath)

            with f:
                headers = next(f).split()
                try:
                    pval_index = headers.index('PVALUE')
                    chrom_index = headers.index('CHROM')
                    pos_index = headers.index('POS')
                    ref_index = headers.index('REF')
                    alt_index = headers.index('ALT')
                except ValueError:
                    logger.error(f'OBH_Error: Error reading file headers for {gwas_filepath}')
                    return False
                
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
                                #logger.info(f'found a sig var in {data})')

                                # TODO we're assuming 23 and 24 instead of X and Y here
                                chromosome = int(data[chrom_index])
                                position = int(data[pos_index])
                                ref_allele = data[ref_index]
                                alt_allele = data[alt_index]
                                
                                # TODO set up dictionary with chromosomes already present to avoid this
                                if chromosome not in variant_dictionary:
                                    variant_dictionary[chromosome] = {}
                               
                                if position not in variant_dictionary[chromosome]:
                                    variant_dictionary[chromosome][position] = []

                                already_converted = False
                                for variant_info in variant_dictionary[chromosome][position]:
                                    variant = variant_info[1]
                                    # TODO is there a way to do this faster? this has to be super slow
                                    # (it would only happen for variants at the same position though so not a huge deal)
                                    if (variant.ref == ref_allele) and (variant.alt == alt_allele):
                                        already_converted = True
                                        sig_variants_duplicates += 1
                                        break

                                if not already_converted:
                                    hgvs = self.convert_vcf_to_hgvs(reference_genome, reference_patch, chromosome, position, ref_allele, alt_allele)
                                    if hgvs:
                                        new_variant = SequenceVariant(hgvs, reference_genome, chromosome, position, ref_allele, alt_allele)
                                        variant_dictionary[chromosome][position].append([None, new_variant])

                                        if chromosome not in new_variant_dictionary:
                                            new_variant_dictionary[chromosome] = {}
                                        if position not in new_variant_dictionary[chromosome]:
                                            new_variant_dictionary[chromosome][position] = []
                                        new_variant_dictionary[chromosome][position].append([None, new_variant])
                                        sig_variants_found += 1
                                    else:
                                        sig_variants_failed_conversion += 1

                    except (IndexError, ValueError) as e:
                        logger.warning(f'OBH_Error: Error reading file {gwas_filepath}, on line {line_counter}: {e}')

        except IOError:
            logger.error(f'OBH_Error: Could not open file: {gwas_filepath}')
            return False

        gwas_filename = gwas_filepath.rsplit('/',1)[-1]
        logger.info(f'OBH_Info: in {gwas_filename} {sig_variants_found} significant variants found and converted.')
        logger.info(f'OBH_Info: in {gwas_filename} {sig_variants_duplicates} significant variants found that were duplicates.')
        logger.info(f'OBH_Info: in {gwas_filename} {sig_variants_failed_conversion} other significant variants failed to convert to hgvs.')
        return True

    def convert_vcf_to_hgvs(self, reference_genome, reference_patch, chromosome, position, ref_allele, alt_allele):
        try:
            ref_chromosome = self.reference_chrom_labels[reference_genome][reference_patch][chromosome]
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
        
        hgvs = f'{ref_chromosome}:g.{variation}'
        return hgvs

    def prepopulate_gwascatalog_cache(self):
        self.gwascatalog.prepopulate_cache()

    def prepopulate_variant_synonymization_cache(self, variant_dict):
        # walk through and batch synonymize any variants that need it
        uncached_variants = []
        for chromosome, position_dict in variant_dict.items():
            for position, variants in position_dict.items():
                for variant_info in variants:
                    variant = variant_info[1]
                    if self.cache.get(f'synonymize(HGVS:{variant.hgvs})') is None:
                        uncached_variants.append(variant.hgvs)

                    if len(uncached_variants) == 10000:
                        self.process_variant_synonymization_cache(uncached_variants)
                        uncached_variants = []
                
        if uncached_variants:
            self.process_variant_synonymization_cache(uncached_variants)

    def process_variant_synonymization_cache(self, batch_of_hgvs):
        batch_synonyms = self.clingen.get_batch_of_synonyms(batch_of_hgvs)
        with self.cache.redis.pipeline() as redis_pipe:
            count = 0
            for hgvs_curie, synonyms in batch_synonyms.items():
                key = f'synonymize({hgvs_curie})'
                redis_pipe.set(key, pickle.dumps(synonyms))
                count += 1

                caid_labled_id = None
                for syn in synonyms:
                    if syn.identifier.startswith('CAID'):
                        caid_labled_id = syn
                        synonyms.remove(caid_labled_id)
                        redis_pipe.set(f'synonymize({caid_labled_id.identifier})', pickle.dumps(synonyms))
                        synonyms.add(caid_labled_id)
                        count += 1
                        break

                if count == 8000:
                    redis_pipe.execute()
                    count = 0

            if count > 0:
                redis_pipe.execute()

    def prepopulate_variant_annotation_cache(self, batch_of_nodes):
        batch_annotations = self.myvariant.batch_sequence_variant_to_gene(batch_of_nodes)
        with self.cache.redis.pipeline() as redis_pipe:
            for seq_var_curie, annotations in batch_annotations.items():
                key = f'myvariant.sequence_variant_to_gene({seq_var_curie})'
                redis_pipe.set(key, pickle.dumps(annotations))
            redis_pipe.execute()

    def process_gwas_variants(self, new_variants, all_variants):
        new_variant_labled_ids = []
        redis_counter = 0
        variants_processed_counter = 0
        uncached_variant_annotation_nodes = []
        with BufferedWriter(self.rosetta) as writer, self.cache.redis.pipeline() as redis_pipe:
            for chromosome, position_dict in new_variants.items():
                for position, variants in position_dict.items():
                    for variant_index, variant_info in enumerate(variants):
                        variant = variant_info[1]
                        curie_hgvs = f'HGVS:{variant.hgvs}'
                        variant_node = KNode(curie_hgvs, name=variant.hgvs, type=node_types.SEQUENCE_VARIANT)
                        self.rosetta.synonymizer.synonymize(variant_node)

                        # TODO do this with an annotater
                        sequence_location = [variant.build, str(variant.chrom), str(variant.pos)]
                        variant_node.properties['sequence_location'] = sequence_location

                        writer.write_node(variant_node)

                        variant_info[0] = variant_node
                        all_variants[chromosome][position][variant_index][0] = variant_node

                        new_variant_labled_ids.append(LabeledID(identifier=variant_node.id, label=variant_node.name))

                        # check if myvariant key exists in cache, otherwise add it to buffer for batch precaching calls
                        if self.cache.get(f'myvariant.sequence_variant_to_gene({variant_node.id})') is None:
                            uncached_variant_annotation_nodes.append(variant_node)
                            if len(uncached_variant_annotation_nodes) == 1000:
                                self.prepopulate_variant_annotation_cache(uncached_variant_annotation_nodes)
                                uncached_variant_annotation_nodes = []

                        # ensembl cant handle batches, and for now NEEDS to be precached individually here
                        # (the properties on the nodes needed by ensembl wont be available to the runner)
                        nearby_cache_key = f'ensembl.sequence_variant_to_gene({variant_node.id})'
                        cached_nearby_genes = self.cache.get(nearby_cache_key)
                        if cached_nearby_genes is None:
                            nearby_genes = self.ensembl.sequence_variant_to_gene(variant_node)
                            redis_pipe.set(nearby_cache_key, pickle.dumps(nearby_genes))
                            redis_counter += 1

                        # sequence variant to sequence variant is never 'run' so go ahead and cache AND write these relationships
                        #linkage_cache_key = f'ensembl.sequence_variant_to_sequence_variant({variant_node.id})'
                        #linked_variant_edge_nodes = self.cache.get(linkage_cache_key)
                        #if linked_variant_edge_nodes is None:
                        #    linked_variant_edge_nodes = self.ensembl.sequence_variant_to_sequence_variant(variant_node)
                        #    redis_pipe.set(linkage_cache_key, pickle.dumps(linked_variant_edge_nodes))
                        #    redis_counter += 1

                        #for ld_edge_node in linked_variant_edge_nodes:
                        #    writer.write_node(ld_edge_node[1])
                        #    writer.write_edge(ld_edge_node[0])

                        if redis_counter > 500:
                            redis_pipe.execute()
                            redis_counter = 0

                        variants_processed_counter += 1
                        
            if redis_counter > 0:
                redis_pipe.execute()

        if uncached_variant_annotation_nodes:
            self.prepopulate_variant_annotation_cache(uncached_variant_annotation_nodes)

        # all precaching is done, run the runner in pooled chunks for all new variants
        if new_variant_labled_ids:
            chunked_labled_variant_ids = [new_variant_labled_ids[i:i + 1000] for i in range(0, len(new_variant_labled_ids), 1000)]
            pool = Pool(4)
            pool.map(run_seq_to_gene, chunked_labled_variant_ids)
            pool.map(run_seq_to_disease, chunked_labled_variant_ids)
            pool.close()
            pool.join()

        return variants_processed_counter

    def process_gwas_associations(self, writer, gwas_node, gwas_filepath, significant_variants, max_p_value=None, verbose=False):
        # walk through and for each variant find it's row in the specific gwas file to grab the p value
        missing_variants_count = 0
        try:
            tabix_handler = tabix.open(f'{gwas_filepath}')
        except tabix.TabixError as e:
            logger.debug(f'Could not open tabix file {gwas_filepath}')
            return False

        predicate = LabeledID(identifier=f'RO:0002609', label=f'related_to')
        for chromosome, position_dict in significant_variants.items():
            for position, variants in position_dict.items():
                gwas_data = self.get_gwas_data_from_indexed_file(f'{gwas_filepath}', f'{chromosome}', position, position, tabix_handler=tabix_handler)
                for variant_info in variants:
                    variant = variant_info[1]
                    found_variant = False
                    if gwas_data:
                        for data in gwas_data:
                            # TODO these indexes should be dynamic, right now assume our cleaned SUGEN format
                            if (variant.ref == data[3]) and (variant.alt == data[4]):
                                try:
                                    p_value = float(data[5])
                                    beta = float(data[6])
                                    found_variant = True
                                except ValueError as e:
                                    logger.warning(f'OBH_Error: Bad p value or beta in file {gwas_filepath}: {e}')
                                    continue

                                if (max_p_value == None) or (p_value <= max_p_value):
                                    namespace = gwas_filepath.rsplit('/', 1)[-1]
                                    # write the relationship, in this case both nodes already exist and it just creates and writes the edge
                                    self.write_new_association(writer, gwas_node, variant_info[0], predicate, p_value, strength=beta, namespace=namespace, node_exists=True)

                    if not found_variant:
                        missing_variants_count += 1
        if missing_variants_count > 0:
            logger.warning(f'OBH_Error: {gwas_filepath} had {missing_variants_count} missing variants!')

        return True

    def create_mwas_graph(self, source_nodes, mwas_file_names, mwas_file_directory, p_value_cutoff):
        metabolites_processed = 0
        predicate = LabeledID(identifier=f'RO:0002609', label=f'related_to')

        with BufferedWriter(self.rosetta) as writer:
            for source_node in source_nodes:
                filename = mwas_file_names[source_node.id]
                filepath = f'{mwas_file_directory}/{filename}'
                identifiers, p_values, beta_values = self.get_metabolite_identifiers_from_mwas(filepath, p_value_cutoff)
                if identifiers:
                    self.rosetta.synonymizer.synonymize(source_node)
                    writer.write_node(source_node)

                labled_metabolite_ids = []
                for metabolite_id in identifiers:
                    p_value = p_values.get(metabolite_id.identifier)
                    beta = beta_values.get(metabolite_id.identifier)
                    metabolite_node = KNode(metabolite_id.identifier, name=metabolite_id.label, type=node_types.CHEMICAL_SUBSTANCE)
                    self.rosetta.synonymizer.synonymize(metabolite_node)
                    labled_metabolite_ids.append(LabeledID(identifier=metabolite_node.id, label=metabolite_node.name))
                    new_edge = self.write_new_association(writer, source_node, metabolite_node, predicate, p_value, strength=beta, namespace=filename)
                
                metabolites_processed += len(labled_metabolite_ids)

        logger.info(f'create_mwas_graph complete - {metabolites_processed} significant metabolites found and processed.')

    def load_metabolite_info(self, metabolite_info_file_path, p_value_cutoff=1, file_names_directory='', file_names_postfix='',  file_name_extension='', file_name_truncation=None):
               
        metabolite_nodes = []
        try:
            with open(metabolite_info_file_path) as f:
                csv_reader = csv.reader(f)
                headers = next(csv_reader)
                try:
                    label_index = headers.index('biochemical')
                    file_name_index = headers.index('metabolite_std')
                    hmdb_index = headers.index('hmdb')
                    pubchem_index = headers.index('pubchem')
                    kegg_index = headers.index('kegg')
                except ValueError:
                    logger.warning(f'Error reading file headers for {metabolite_info_file_path}')
                    return metabolite_nodes

                metabolon_id = 1
                for data in csv_reader:
                    try:
                        if data[pubchem_index] != '#N/A':
                            m_id = f'PUBCHEM:{data[pubchem_index]}'
                        elif data[hmdb_index] != '#N/A':
                            m_id = f'HMDB:{data[hmdb_index]}'
                        elif data[kegg_index] != '#N/A':
                            m_id = f'KEGG.COMPOUND:{data[kegg_index]}'
                        else:
                            m_id = f'METABOLON:{metabolon_id}'
                            metabolon_id += 1

                        m_label = data[label_index]
                        m_filename = f'{data[file_name_index]}{file_names_postfix}'
                        # temporary workaround for truncated incorrect names
                        m_filename = m_filename[0:file_name_truncation]

                        if file_name_extension:
                            m_filename = m_filename + file_name_extension

                        if file_names_directory:
                            m_filename = file_names_directory + '/' + m_filename

                        new_metabolite_node = KNode(m_id, name=m_label, type=node_types.CHEMICAL_SUBSTANCE)
                        metabolite_info = {"node": new_metabolite_node, "filepath" : m_filename, "p_value_cutoff" : p_value_cutoff}
                        metabolite_nodes.append(metabolite_info)

                        if m_filename in self.metabolite_labled_id_lookup:
                            pass
                            #logger.debug(f'metabolite look up already had {m_filename}')
                        else:
                            self.metabolite_labled_id_lookup[m_filename] = LabeledID(identifier=m_id, label=m_label)

                    except (KeyError) as e:
                        logger.warning(f'metabolites_file ({metabolite_info_file_path}) could not be parsed: {e}')

        except (IOError) as e:
            logger.warning(f'metabolites_file ({metabolite_info_file_path}) could not be loaded: {e}')

        logger.info(f'metabolites_file ({metabolite_info_file_path}) had {len(metabolite_nodes)} metabolites.')
        return metabolite_nodes

    def get_metabolite_identifiers_from_mwas(self, mwas_filepath, p_value_cutoff):
        metabolite_ids = []
        corresponding_p_values = {}
        corresponding_beta_values = {}
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
                    elif 'beta' in header.lower():
                        beta_index = headers.index(header)

                if (name_index < 0) or (pval_index < 0):
                    logger.warning(f'Error reading file headers for {mwas_filepath} - {headers}')
                    return metabolite_ids, corresponding_p_values, corresponding_beta_values

                for data in csv_reader:
                    try:
                        line_counter += 1
                        p_value_string = data[pval_index]
                        if (p_value_string != 'NA'):
                            p_value = float(p_value_string) 
                            if p_value <= p_value_cutoff:
                                m_name = data[name_index]
                                if m_name in self.metabolite_labled_id_lookup:
                                    beta_value = data[beta_index]
                                    m_labeled_id = self.metabolite_labled_id_lookup[m_name]
                                    metabolite_ids.append(m_labeled_id)
                                    corresponding_p_values[m_labeled_id.identifier] = p_value
                                    corresponding_beta_values[m_labeled_id.identifier] = beta_value
                                else:
                                    logger.warning(f'Could not find real id for metabolite {m_name} in {mwas_filepath}')  

                    except IndexError as e:
                        logger.warning(f'Error parsing file {mwas_filepath}, on line {line_counter}: {e}')
                    except ValueError as e:
                        logger.warning(f'Error converting {p_value_string} to float in {mwas_filepath}')

        except IOError:
            logger.warning(f'Could not open file: {mwas_filepath}')

        return metabolite_ids, corresponding_p_values, corresponding_beta_values

    def write_new_association(self, writer, source_node, associated_node, predicate, p_value, strength=None, namespace=None, node_exists=False):

        if self.concept_model:
            standard_predicate = self.concept_model.standardize_relationship(predicate)
        else:
            logger.warning('OBH builder: concept_model was missing, predicate standardization failed')
            standard_predicate = predicate

        provided_by = 'OBH_Builder'
        props={'p_value': p_value}
        if namespace:
            props['namespace'] = namespace
        if strength:
            props['strength'] = strength

        ctime = time.time()
        new_edge = KEdge(source_id=source_node.id,
                     target_id=associated_node.id,
                     provided_by=provided_by,
                     ctime=ctime,
                     original_predicate=predicate,
                     standard_predicate=standard_predicate,
                     input_id=source_node.id,
                     publications=None,
                     url=None,
                     properties=props)
        
        if not node_exists:
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
                        logger.warning(f'quality_control_check file error ({file_path}) line {line_counter} could not be parsed: {e}')

        except (IOError) as e:
            logger.warning(f'QC check file ({file_path}) could not be loaded: {e}')
            return False
        except (csv.Error) as e:
            logger.warning(f'csv error in ({file_path}): {e}')

        return True

def run_seq_to_disease(labled_variant_ids):
    path = f'{node_types.SEQUENCE_VARIANT},{node_types.DISEASE_OR_PHENOTYPIC_FEATURE}'
    run(path,'','',None,None,None,'greent.conf', identifier_list=labled_variant_ids)

def run_seq_to_gene(labled_variant_ids):
    path = f'{node_types.SEQUENCE_VARIANT},{node_types.GENE}'
    run(path,'','',None,None,None,'greent.conf', identifier_list=labled_variant_ids)

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

    p_value_cutoff = .05

    obesity_id = 'HP:0001513'
    obesity_node = KNode(obesity_id, name='Obesity', type=node_types.DISEASE_OR_PHENOTYPIC_FEATURE)
    gwas_info1 = {"node": obesity_node, "filepath" : "/Example/path/sample_sugen.gz", "p_value_cutoff" : p_value_cutoff}
    
    pa_id = 'EFO:0003940'
    pa_node = KNode(pa_id, name='Physical Activity', type=node_types.DISEASE_OR_PHENOTYPIC_FEATURE)
    gwas_info2 = {"node": pa_node, "filepath" : "/Example/path/sample_sugen2.gz", "p_value_cutoff" : p_value_cutoff}

    asthma_id = 'EFO:0000270'
    asthma_node = KNode(asthma_id, name='Asthma', type=node_types.DISEASE_OR_PHENOTYPIC_FEATURE)
    gwas_info3 = {"node": asthma_node, "filepath" : "/Example/path/sample_sugen3.gz", "p_value_cutoff" : p_value_cutoff}

    gwas_info = [gwas_info1, gwas_info2, gwas_info3]

    obh.create_gwas_graph(gwas_info, project_id='testing')    


