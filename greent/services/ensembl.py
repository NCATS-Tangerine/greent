import requests
from greent import node_types
from greent.graph_components import KNode, LabeledID
from greent.service import Service
from greent.util import Text, LoggingUtil
from collections import namedtuple
import logging,json,sqlite3,os,requests,pickle

logger = LoggingUtil.init_logging(__name__, level=logging.DEBUG)

EnsemblGene = namedtuple('EnsemblGene', ['ensembl_id', 'ensembl_name', 'chromosome', 'start_position', 'end_position', 'gene_biotype', 'description'])

class Ensembl(Service):
    
    def __init__(self, context):
        super(Ensembl, self).__init__("ensembl", context)
        self.clingen = context.core.clingen
        self.cache = context.cache
        self.redis = context.cache.redis
        self.var_to_gene_predicate = LabeledID(identifier=f'GAMMA:0000102', label=f'nearby_variant_of')
        self.var_to_var_predicate = LabeledID(identifier=f'NCIT:C16798', label=f'linked_to')

        self.gene_db_successfully_created = False
        self.gene_db_path = os.path.join(os.path.dirname(__file__), 'genes.sqlite3')

        self.persistent_conn = None
        self.all_gene_annotations = None

        # we assume the order of attributes from this url -
        # if we change this we need to change the indexing in create_genes_db below
        self.ensembl_genes_url = """http://www.ensembl.org/biomart/martservice?query=<?xml version="1.0" encoding="UTF-8"?>
                                    <!DOCTYPE Query>
                                    <Query  virtualSchemaName = "default" formatter = "TSV" header = "0" uniqueRows = "0" count = "" datasetConfigVersion = "0.6" >
                                        <Dataset name = "hsapiens_gene_ensembl" interface = "default" >
                                            <Attribute name = "ensembl_gene_id" />
                                            <Attribute name = "gene_biotype" />
                                            <Attribute name = "external_gene_name" />
                                            <Attribute name = "start_position" />
                                            <Attribute name = "end_position" />
                                            <Attribute name = "description" />
                                            <Attribute name = "chromosome_name" />
                                        </Dataset>
                                    </Query>"""
        self.gene_batch_url = 'http://www.ensembl.org/biomart/martservice'
        
        self.check_if_already_done_sql = "SELECT name FROM sqlite_master WHERE type='table' AND name='genes';"
        
        self.genes_table_sql = """CREATE TABLE IF NOT EXISTS genes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ensembl_id text, 
        gene_name text, 
        chromosome INTEGER,
        start_pos INTEGER, 
        end_pos INTEGER, 
        gene_type text,
        description text);"""

        self.genes_table_ensembl_id_index_sql = "CREATE UNIQUE INDEX ensembl_ids on genes(ensembl_id);"
        self.genes_table_composite_index_sql = "CREATE INDEX gene_composite on genes(chromosome, start_pos, end_pos, ensembl_id);"

        self.gene_entry_sql = """INSERT INTO genes 
        (ensembl_id, gene_name, chromosome, start_pos, end_pos, gene_type, description) 
        VALUES (?,?,?,?,?,?,?);"""
        
        self.gene_range_select_sql = """SELECT ensembl_id, start_pos, end_pos
        FROM genes WHERE chromosome = ? AND ((? >= start_pos AND ? <= end_pos)
        OR (? >= start_pos AND ? <= end_pos) OR (? <= start_pos AND ? >= end_pos));"""

        self.gene_ensembl_id_select_sql = "SELECT * FROM genes WHERE ensembl_id = ?"

    def create_or_connect_to_genes_db(self):
        
        if not self.gene_db_successfully_created:
            self.create_genes_db()
        
        if not self.persistent_conn:
            self.persistent_conn = sqlite3.connect(self.gene_db_path, detect_types=sqlite3.PARSE_DECLTYPES)
            self.persistent_conn.row_factory = sqlite3.Row

        return self.persistent_conn

    def create_genes_db(self):
        try:
            db_conn = sqlite3.connect(self.gene_db_path)
            db_cursor = db_conn.cursor()

            db_cursor.execute(self.check_if_already_done_sql)
            if db_cursor.fetchone() is not None:
                # TODO we assume this means it's complete - it could have been created but not filled out
                self.gene_db_successfully_created = True
                return True

            # ensembl_genes are of type EnsemblGene namedtuple - that has to match the SQL parameters in gene_entry_sql
            ensembl_genes = self.retrieve_all_genes()
           
            with db_conn:
                db_conn.execute(self.genes_table_sql)
                db_conn.executemany(self.gene_entry_sql, ensembl_genes)
                db_conn.execute(self.genes_table_ensembl_id_index_sql)
                db_conn.execute(self.genes_table_composite_index_sql)

            db_conn.close()
            logger.info(f'Ensembl created a gene database with {len(ensembl_genes)} entries!')
            self.gene_db_successfully_created = True
            return True

        except sqlite3.Error as e:
            logger.error(f'Ensembl had a database error: {e}')

    def retrieve_all_genes(self):
        genes_response = requests.get(self.ensembl_genes_url)
        if genes_response.status_code == 200:
            genes_data = genes_response.text.splitlines()
            if len(genes_data) > 1:
                ensembl_genes = []
                for gene_line in genes_data[1:]:
                    # gene_info is a EnsemblGene 
                    gene_info = self.parse_biomart_gene_data(gene_line)
                    if gene_info:
                        ensembl_genes.append(gene_info)
                return ensembl_genes
            else:
                logger.error(f'Ensembl biomart genes call didnt find any matches! Thats not right!')
                return False
        else:
            logger.error(f'Ensembl non-200 response from biomart genes call: {genes_response.status_code})')
            return None

    def parse_biomart_gene_data(self, gene_line):
        gene_data = gene_line.split('\t')
        try:
            # we assume the index order is predictable from ensembl_genes_url above - if we change that we need to change this
            ensembl_id = gene_data[0]
            gene_type = gene_data[1]
            ensembl_name = gene_data[2]
            start_position = int(gene_data[3])
            end_position = int(gene_data[4])
            description = gene_data[5]
            chromosome = gene_data[6]
            return EnsemblGene(ensembl_id, ensembl_name, chromosome, start_position, end_position, gene_type, description)

        except (IndexError, ValueError) as e:
            logger.error(f'Ensembl biomart genes call had an issue with one line: {e})')

        return None

    def sequence_variant_to_gene(self, variant_node):
        
        flanking_region_size = 500000
        results = []

        found_valid_robokop_key = False
        robokop_ids = variant_node.get_synonyms_by_prefix('ROBO_VARIANT')
        if not robokop_ids:
            logger.debug(f'ensembl: robokop variant key not found for variant: {variant_node.id}')
            return results
        else:
            try:
                for robokop_key in robokop_ids:
                    robokop_data = Text.un_curie(robokop_key).split('|')
                    reference_genome = robokop_data[0]
                    if reference_genome == 'HG38':
                        found_valid_robokop_key = True
                    else:
                        continue
                    chromosome = robokop_data[1]
                    start_position = int(robokop_data[2])
                    end_position = int(robokop_data[3])
            except IndexError as e:
                logger.debug(f'ensembl: robokop variant key not set properly for variant: {variant_node.id} - {robokop_ids[0]}')
                return results

        if not found_valid_robokop_key:
            logger.debug(f'ensembl: latest robokop variant key not found for variant: {variant_node.id}')
            return results

        flanking_min = start_position - flanking_region_size
        if flanking_min < 0:
            flanking_min = 0
        flanking_max = end_position + flanking_region_size

        db_conn = self.create_or_connect_to_genes_db()
        db_cursor = db_conn.cursor()

        #logger.info(f'looking for genes overlapping {flanking_min}-{flanking_max}')

        db_cursor.execute(self.gene_range_select_sql, (chromosome, flanking_min, flanking_min, flanking_max, flanking_max, flanking_min, flanking_max))

        genes_in_region = db_cursor.fetchall()
        for gene_id_text, gene_start, gene_end in genes_in_region:
            #cast this to make neo4j happy
            gene_id = str(gene_id_text)
            #logger.info(f'Found matching gene: {gene_id},{gene_start},{gene_end}')
            gene_node = KNode(f'ENSEMBL:{gene_id}', name=f'{gene_id}', type=node_types.GENE)
            if start_position < gene_start:
                distance = gene_start - start_position
            elif end_position > gene_end:
                distance = end_position - gene_end
            else:
                distance = 0
            props = {'distance' : distance}
            edge = self.create_edge(variant_node, gene_node, 'ensembl.sequence_variant_to_gene', variant_node.id, self.var_to_gene_predicate, url=self.gene_batch_url, properties=props)
            results.append((edge, gene_node))

        logger.info(f'ensembl sequence_variant_to_gene found {len(results)} results for {variant_node.id}')

        return results

    def sequence_variant_to_sequence_variant(self, variant_node):
        ld_url = '/ld/human/'
        options_url = '?r2=0.8'
        population = '1000GENOMES:phase_3:MXL'

        return_results = []
        with self.redis.pipeline() as redis_pipe:
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
                        new_rsid_node = None
                        is_new_dbsnp = False
                        synonyms = self.cache.get(f'synonymize({new_variant_curie})') 
                        if synonyms is None:
                            new_rsid_node = KNode(new_variant_curie, name=f'{new_variant_id}', type=node_types.SEQUENCE_VARIANT)
                            synonyms = self.clingen.get_synonyms_by_other_ids(new_rsid_node)
                            redis_pipe.set(f'synonymize({new_variant_curie})', pickle.dumps(synonyms))
                            is_new_dbsnp = True
                        caid_count = 0
                        caid_node = None
                        for synonym in synonyms:
                            if Text.get_curie(synonym.identifier) == 'CAID':
                                caid_count += 1
                                caid_node = KNode(synonym.identifier, name=f'{synonym.label}', type=node_types.SEQUENCE_VARIANT)
                                edge = self.create_edge(variant_node, caid_node, 'ensembl.sequence_variant_to_sequence_variant', dbsnp_curie, self.var_to_var_predicate, url=query_url, properties=props)
                                return_results.append((edge, caid_node))
                                found_caid = True
                        # if caid_count > 2 we can't cache it easily right now so we skip it and let synonymizer do it later
                        if caid_count == 1 and is_new_dbsnp:
                            # assume we didn't cache the CAID yet if the dbsnp is new and do it if needed
                            if self.cache.get(f'synonymize({caid_node.id})') is None:
                                redis_pipe.set(f'synonymize({caid_node.id})',  pickle.dumps(synonyms))
                        elif caid_count == 0:
                            if not new_rsid_node:
                                new_rsid_node = KNode(new_variant_curie, name=f'{new_variant_id}', type=node_types.SEQUENCE_VARIANT)
                            edge = self.create_edge(variant_node, new_rsid_node, 'ensembl.sequence_variant_to_sequence_variant', dbsnp_curie, self.var_to_var_predicate, url=query_url, properties=props)
                            return_results.append((edge, new_rsid_node))

                #elif query_response.status_code == 429:
                #   handle the rate limiting by waiting and retrying
                #
                else:
                    logger.error(f'Ensembl returned a non-200 response for {variant_node.id}: {query_response.status_code})')
            redis_pipe.execute()

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

    def get_all_ensembl_gene_annotations(self):
        if self.all_gene_annotations:
            return self.all_gene_annotations
        else:
            ensembl_genes = self.retrieve_all_genes()
            all_gene_annotations = {}
            for gene in ensembl_genes:
                all_gene_annotations[gene.ensembl_id] = {
                    'ensembl_name' : gene.ensembl_name,
                    'chromosome' : gene.chromosome,
                    'start_position' : gene.start_position,
                    'end_position' : gene.end_position,
                    'gene_biotype' : gene.gene_biotype,
                    'description' : gene.description
                }
            self.all_gene_annotations = all_gene_annotations
            return all_gene_annotations

    def get_ensembl_gene_annotations(self, ensembl_id):
        if not self.all_gene_annotations:
            self.get_all_ensembl_gene_annotations()

        if ensembl_id in self.all_gene_annotations:
            return self.all_gene_annotations[ensembl_id]
        else:
            return {'ensembl_error' : f'{ensembl_id} not found.'}