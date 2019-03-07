import requests
from ftplib import FTP
from greent import node_types
from greent.graph_components import KNode, LabeledID
from greent.service import Service
from greent.util import Text, LoggingUtil
import logging,json
from collections import defaultdict

logger = LoggingUtil.init_logging(__name__, logging.DEBUG)

class GWASCatalog(Service):
    def __init__(self, context, rosetta):
        super(GWASCatalog, self).__init__("gwascatalog", context)
        self.synonymizer = rosetta.synonymizer

    def prepopulate_cache(self):
        if self.context.cache.get('sparse(gwascatalog.sequence_variant_to_disease_or_phenotypic_feature)'):
            return 0
        query_url = 'ftp.ebi.ac.uk/pub/databases/gwas/releases/latest/gwas-catalog-associations_ontology-annotated.tsv'
        ftpsite = 'ftp.ebi.ac.uk'
        ftpdir = '/pub/databases/gwas/releases/latest'
        ftpfile = 'gwas-catalog-associations_ontology-annotated.tsv'
        temp_gwas_file = 'temp_gwas_catalog.tsv'
        ftp = FTP(ftpsite)
        ftp.login()
        ftp.cwd(ftpdir)
        gwas_catalog = []
        ftp.retrlines(f'RETR {ftpfile}', gwas_catalog.append)
        ftp.quit()

        if gwas_catalog:
            file_headers = gwas_catalog[0].split('\t')
            try:
                pub_med_index = file_headers.index('PUBMEDID')
                p_value_index = file_headers.index('P-VALUE')
                snps_index = file_headers.index('SNPS')
                trait_ids_index = file_headers.index('MAPPED_TRAIT_URI')
                trait_names_index = file_headers.index('MAPPED_TRAIT')
            except:
                logger.error('GWAS Catalog failed to prepopulate_cache (bad headers in the file)')
                return None

            corrupted_lines = 0
            missing_variant_ids = 0
            missing_phenotype_ids = 0
            variant_to_pheno_cache = defaultdict(set)
            rsid_to_node_lookup = {}
            #for line in iterator:
            for line in gwas_catalog[1:]:
                line = line.split('\t')
                try:
                    pubmed_id = line[pub_med_index]
                    p_value = float(line[p_value_index])
                    trait_uris = [trait.strip() for trait in line[trait_ids_index].split(',')]
                    trait_names = [trait_name.strip() for trait_name in line[trait_names_index].split(',')]
                    snps = [snp.strip() for snp in line[snps_index].replace(';',' ').replace(',',' ').replace(' x ',' ').split()]

                except (IndexError, ValueError) as e:
                    corrupted_lines += 1
                    logger.debug(f'GWASCatalog corrupted line: {line}')
                    continue

                # this should check to make sure we have everything we need for it to be worth adding
                if not (trait_uris and snps and (len(trait_uris) == len(trait_names))):
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
                            elif trait_id.startswith('GO'):
                                # Biological process or activity
                                # 5k+ of these 
                                missing_phenotype_ids += 1
                                continue
                            else:
                                missing_phenotype_ids += 1
                                logger.warning(f'{trait_uri} not a recognized trait format')
                                continue

                            traits.append((curie_trait_id, trait_names[n]))
                        except IndexError as e:
                            logger.warning(f'{trait_uri} not splittable for ID, from {line}')

                    variants = set()
                    for snp in snps:
                        if snp.startswith('rs'):
                            if snp in rsid_to_node_lookup:
                                temp_node = rsid_to_node_lookup[snp]
                            else:
                                temp_node = KNode(f'DBSNP:{snp}', type=node_types.SEQUENCE_VARIANT)
                                self.synonymizer.synonymize(temp_node)
                                rsid_to_node_lookup[snp] = temp_node
                            
                            caids = temp_node.get_synonyms_by_prefix('CAID')
                            if caids:
                                for caid in caids:
                                    variants.add(caid)
                            else:
                                variants.add(temp_node.id)
                        else:
                            # these are variants that don't have an rsid, should we try to create HGVS?
                            missing_variant_ids += 1
                            pass

                    if traits and variants:
                        props = {'pvalue' : p_value}
                        for variant_id in variants:
                            temp_variant_node = KNode(variant_id, name=variant_id, type=node_types.SEQUENCE_VARIANT)
                            var_to_phen_key = f'gwascatalog.sequence_variant_to_disease_or_phenotypic_feature({variant_id})'
                            for trait_id, trait_name in traits:
                                variant_to_pheno_cache[var_to_phen_key].add(self.create_variant_to_phenotype_components(
                                                                                query_url, 
                                                                                temp_variant_node, 
                                                                                trait_id, 
                                                                                trait_name, 
                                                                                pubmed_id=pubmed_id, 
                                                                                properties=props))

            if corrupted_lines:
                logger.debug(f'GWASCatalog batch file had {corrupted_lines} corrupted lines!')
            if missing_variant_ids:
                logger.debug(f'GWASCatalog batch could not ID {missing_variant_ids} variant ids')
            if missing_phenotype_ids:
                logger.debug(f'GWASCatalog batch could not ID {missing_phenotype_ids} phenotype ids')

            if variant_to_pheno_cache:
                for new_key, new_value in variant_to_pheno_cache.items():
                    self.context.cache.set(new_key, new_value)
                self.context.cache.set('sparse(gwascatalog.sequence_variant_to_disease_or_phenotypic_feature)', True)

            return True

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
        if self.context.cache.get('sparse(gwascatalog.sequence_variant_to_disease_or_phenotypic_feature)'):
            cached_response = self.context.cache.get(f'gwascatalog.sequence_variant_to_disease_or_phenotypic_feature({variant_node.id})')
            if cached_response is not None:
                return cached_response
            else:
                return []

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
            
