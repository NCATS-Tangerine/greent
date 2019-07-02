import requests
from ftplib import FTP
from greent import node_types
from greent.graph_components import KNode, LabeledID
from greent.service import Service
from greent.util import Text, LoggingUtil
import logging,json,pickle,re
from collections import defaultdict

logger = LoggingUtil.init_logging(__name__, logging.DEBUG)

class GWASCatalog(Service):
    def __init__(self, context):
        super(GWASCatalog, self).__init__("gwascatalog", context)
        self.is_cached_already = False

    def prepopulate_cache(self):

        query_url = 'ftp.ebi.ac.uk/pub/databases/gwas/releases/latest/gwas-catalog-associations_ontology-annotated.tsv'
        ftpsite = 'ftp.ebi.ac.uk'
        ftpdir = '/pub/databases/gwas/releases/latest'
        ftpfile = 'gwas-catalog-associations_ontology-annotated.tsv'
        ftp = FTP(ftpsite)
        ftp.login()
        ftp.cwd(ftpdir)
        gwas_catalog = []
        ftp.retrlines(f'RETR {ftpfile}', gwas_catalog.append)
        ftp.quit()

        try:
            file_headers = gwas_catalog[0].split('\t')
            pub_med_index = file_headers.index('PUBMEDID')
            p_value_index = file_headers.index('P-VALUE')
            snps_index = file_headers.index('SNPS')
            #snp_allele_index = file_headers.index('STRONGEST SNP-RISK ALLELE')
            trait_ids_index = file_headers.index('MAPPED_TRAIT_URI')
            trait_names_index = file_headers.index('MAPPED_TRAIT')
        except (IndexError, ValueError) as e:
            logger.error(f'GWAS Catalog failed to prepopulate_cache ({e})')
            return []

        corrupted_lines = 0
        missing_variant_ids = 0
        missing_phenotype_ids = 0
        rsids_without_caids = 0
        variant_to_pheno_cache = defaultdict(set)
        rsid_to_nodes_lookup = {}

        progress_counter = 0
        total_lines = len(gwas_catalog)

        with self.context.cache.redis.pipeline() as redis_pipe:
            trait_uri_pattern = re.compile(r'[^,\s]+')
            trait_name_pattern = re.compile(r'[^,]+')
            snp_pattern = re.compile(r'[^,;x*\s]+')
            for line in gwas_catalog[1:]:
                line = line.split('\t')
                try:
                    pubmed_id = line[pub_med_index]
                    p_value = float(line[p_value_index])
                   
                    trait_uris = trait_uri_pattern.findall(line[trait_ids_index])
                    trait_names = [trait_name.strip() for trait_name in trait_name_pattern.findall(line[trait_names_index])]
                    snps = snp_pattern.findall(line[snps_index])
                    #snp_allele_pairs = re.findall(r'[^,;x\s]+', line[snp_allele_index])

                except (IndexError, ValueError) as e:
                    corrupted_lines += 1
                    logger.debug(f'GWASCatalog corrupted line: {e} ')
                    continue

                has_trait_names = False
                if len(trait_uris) == len(trait_names):
                    has_trait_names = True
                else:
                    logger.debug(f'GWASCatalog had # of trait uris that did not match # of trait names: {trait_uris} {trait_names}')

                #has_snp_alleles = False
                #snp_alleles = []
                #if len(snps) == len(snp_allele_pairs):
                #    for pair in snp_allele_pairs:
                #        try:
                #            allele = pair.split('-',1)[1]
                #            has_snp_alleles = True
                #        except IndexError as e:
                #            logger.debug(f'GWASCatalog could not split snp-allele: {pair}')
                #            allele = '?'
                #        snp_alleles.append(allele)
                #else:
                #    logger.debug(f'GWASCatalog had # of snps that did not match # of snp alleles: {line}')
                #    pass

                if not (trait_uris and snps):
                    corrupted_lines += 1
                    logger.debug(f'GWASCatalog corrupted line: {line}')
                    continue
                else:
                    traits = []
                    for n, trait_uri in enumerate(trait_uris):
                        try:
                            trait_id = trait_uri.rsplit('/', 1)[1]
                            # ids show up like EFO_123, Orphanet_123, HP_123 
                            if trait_id.startswith('EFO'):
                                curie_trait_id = f'EFO:{trait_id[4:]}'
                            elif trait_id.startswith('Orp'):
                                curie_trait_id = f'ORPHANET:{trait_id[9:]}'
                            elif trait_id.startswith('HP'):
                                curie_trait_id = f'HP:{trait_id[3:]}'
                            elif trait_id.startswith('NCIT'):
                                curie_trait_id = f'NCIT:{trait_id[5:]}'
                            elif trait_id.startswith('MONDO'):
                                curie_trait_id = f'MONDO:{trait_id[6:]}'
                            elif trait_id.startswith('GO'):
                                # Biological process or activity
                                # 5k+ of these 
                                missing_phenotype_ids += 1
                                continue
                            else:
                                missing_phenotype_ids += 1
                                logger.warning(f'{trait_uri} not a recognized trait format')
                                continue

                            if has_trait_names:
                                traits.append((curie_trait_id, trait_names[n]))
                            else:
                                traits.append((curie_trait_id, curie_trait_id))

                        except IndexError as e:
                            logger.warning(f'trait uri index error:({trait_uri}) not splittable')

                    variant_nodes = set()
                    for n, snp in enumerate(snps):
                        if snp.startswith('rs'):
                            dbsnp_curie = f'DBSNP:{snp}'
                            #if has_snp_alleles:
                            #    snp_allele = snp_alleles[n]
                            if snp in rsid_to_nodes_lookup:
                                temp_variant_nodes = rsid_to_nodes_lookup[snp]
                            else:
                                #if has_snp_alleles and (snp_allele != '?'):
                                #    synonyms = self.context.cache.get(f'synonymize({dbsnp_curie}({snp_allele}))') 
                                #    if synonyms is None:
                                #        synonyms = self.context.core.clingen.get_synonyms_by_rsid_with_sequence(snp, snp_allele)
                                #        self.context.cache.set(f'synonymize({dbsnp_curie}({snp_allele}))', synonyms)
                                #else:
                                synonyms = self.context.cache.get(f'synonymize({dbsnp_curie})') 
                                if synonyms is None:
                                    dbsnp_node = KNode(dbsnp_curie, type=node_types.SEQUENCE_VARIANT)
                                    synonyms = self.context.core.clingen.get_synonyms_by_other_ids(dbsnp_node)
                                    redis_pipe.set(f'synonymize({dbsnp_curie})', pickle.dumps(synonyms))

                                if len(synonyms) == 0:
                                    #logger.debug(f'GWASCatalog could not find synonyms for {dbsnp_curie} with allele {snp_allele}')
                                    logger.debug(f'GWASCatalog could not find synonyms for {dbsnp_curie}')
                                    pass
                                
                                temp_variant_nodes = []
                                caid_count = 0
                                for synonym in synonyms:
                                    if Text.get_curie(synonym.identifier) == 'CAID':
                                        caid_count += 1
                                        caid_id = Text.un_curie(synonym.identifier)
                                        temp_variant_nodes.append(KNode(synonym.identifier, name=synonym.label, type=node_types.SEQUENCE_VARIANT))

                                if caid_count == 1:
                                    # multiple CAIDs per rsid makes precaching the rsid and caids in one call complex,
                                    # here we can cache rsids that only returned one CAID easily, so do that
                                    # others will meet synonymization later but it should be a small percentage of them
                                    # TODO we could/probably should split the synonyms returned for rsid calls then we could cache those cases too
                                    redis_pipe.set(f'synonymize(CAID:{caid_id})', pickle.dumps(synonyms))

                                if not temp_variant_nodes:
                                    rsids_without_caids += 1
                                    temp_variant_nodes.append(KNode(dbsnp_curie, name=snp, type=node_types.SEQUENCE_VARIANT))

                                rsid_to_nodes_lookup[snp] = temp_variant_nodes

                            for node in temp_variant_nodes:
                                variant_nodes.add(node)
                        else:
                            # these are variants that don't have an rsid, should we try to create HGVS?
                            #logger.info(f'gwascatalog variant {snp} not recognized')
                            missing_variant_ids += 1
                            pass

                    if traits and variant_nodes:
                        props = {'pvalue' : p_value}
                        for variant_node in variant_nodes:
                            for trait_id, trait_name in traits:
                                variant_to_pheno_cache[variant_node].add(self.create_variant_to_phenotype_components(
                                                                                query_url, 
                                                                                variant_node, 
                                                                                trait_id, 
                                                                                trait_name, 
                                                                                pubmed_id=pubmed_id, 
                                                                                properties=props))
                progress_counter += 1
                if progress_counter % 1000 == 0:
                    redis_pipe.execute()
                    percent_complete = (progress_counter / total_lines) * 100
                    logger.info(f'GWASCatalog progress: {int(percent_complete)}%')

            redis_pipe.execute()

        if corrupted_lines:
            logger.debug(f'GWASCatalog file had {corrupted_lines} corrupted lines!')
        if missing_variant_ids:
            logger.debug(f'GWASCatalog precaching could not ID {missing_variant_ids} variant ids')
        if missing_phenotype_ids:
            logger.debug(f'GWASCatalog precaching could not ID {missing_phenotype_ids} phenotype ids')
        if rsids_without_caids:
            logger.debug(f'GWASCatalog precaching had {rsids_without_caids} rsids without caids')

        # add every sequence variant -> disease or phenotype edge and node to the cache
        with self.context.cache.redis.pipeline() as redis_pipe:
            for variant_node, phenotypes in variant_to_pheno_cache.items():
                gwascatalog_key = f'gwascatalog.sequence_variant_to_disease_or_phenotypic_feature({variant_node.id})'
                redis_pipe.set(gwascatalog_key, pickle.dumps(phenotypes))
            redis_pipe.execute()

            self.context.cache.set('sparse(gwascatalog.sequence_variant_to_disease_or_phenotypic_feature)', True)

        self.is_cached_already = True

        return variant_to_pheno_cache

    def is_precached(self):
        return self.context.cache.get('sparse(gwascatalog.sequence_variant_to_disease_or_phenotypic_feature)')

    def create_variant_to_phenotype_components(self, query_url, variant_node, phenotype_id, phenotype_label, pubmed_id=None, properties={}):
        
        phenotype_node = KNode(phenotype_id, name=phenotype_label, type=node_types.DISEASE_OR_PHENOTYPIC_FEATURE)
        
        pubs = []
        if pubmed_id:
            pubs.append(f'PMID:{pubmed_id}')

        predicate = LabeledID(identifier=f'gwascatalog:has_phenotype',label=f'has_phenotype')
        edge = self.create_edge(
            variant_node, 
            phenotype_node, 
            'gwascatalog.sequence_variant_to_disease_or_phenotypic_feature', 
            variant_node.id,
            predicate,
            url=query_url, 
            properties=properties, 
            publications=pubs)
        return (edge, phenotype_node)

    def create_phenotype_to_variant_components(self, query_url, phenotype_node, variant_id, variant_label, pubmed_id=None, properties={}):
        
        variant_node = KNode(variant_id, name=variant_label, type=node_types.SEQUENCE_VARIANT)
        
        pubs = []
        if pubmed_id:
            pubs.append(f'PMID:{pubmed_id}')

        predicate = LabeledID(identifier=f'RO:0002609', label=f'related_to')
        edge = self.create_edge(
            phenotype_node,
            variant_node, 
            'gwascatalog.disease_or_phenotypic_feature_to_sequence_variant', 
            phenotype_node.id,
            predicate, 
            url=query_url, 
            properties=properties, 
            publications=pubs)
        return (edge, variant_node)

    def sequence_variant_to_disease_or_phenotypic_feature(self, variant_node):
        
        # TODO - reconsider this approach - we could return None instead of [] for empty results
        # so that program could handle this sparse array more intelligently (ie not write all the empty results)
        if not self.is_cached_already:
            if not self.is_precached():
                self.prepopulate_cache()
            else:
                self.is_cached_already = True

        cached_response = self.context.cache.get(f'gwascatalog.sequence_variant_to_disease_or_phenotypic_feature({variant_node.id})')
        if cached_response is not None:
            return cached_response
        else:
            return []

        """
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
                    logger.warning(f'problem parsing results from GWASCatalog: {e}')"""

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
        ## TODO this could support Orphanet etc
        return_results = []
        trait_ids = phenotype_node.get_synonyms_by_prefix('EFO')
        trait_prefix = 'EFO'
        if not trait_ids:
             trait_ids = phenotype_node.get_synonyms_by_prefix('ORPHANET')
             trait_prefix = 'Orphanet'
        if not trait_ids:
             trait_ids = phenotype_node.get_synonyms_by_prefix('HP')
             trait_prefix = 'HP'
        for trait_id in trait_ids:
            query_url = f'{self.url}efoTraits/{trait_prefix}_{Text.un_curie(trait_id)}/associations?projection=associationByEfoTrait'
            query_json = self.query_service(query_url)
            if query_json:
                try:
                    for association in query_json['_embedded']['associations']:
                        variant_nodes = []
                        for snp in association['snps']:
                            variant_rsid = snp['rsId']
                            variant_nodes.append(KNode(
                                            f'DBSNP:{variant_rsid}', 
                                            name=f'{variant_rsid}', 
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

                            predicate = LabeledID(identifier=f'RO:0002609', label=f'related_to')
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
        if query_response.status_code == 200:
            query_json = query_response.json()
            return query_json
        else:
            logger.warning(f'GWAS Catalog returned a non-200 response({query_response.status_code}) calling ({query_url})')
            return {}
            
