import requests
from greent import node_types
from greent.graph_components import KNode, LabeledID
from greent.service import Service
from greent.util import Text, LoggingUtil
import logging,json

logger = LoggingUtil.init_logging(__name__, logging.DEBUG)

class MyChem(Service):

    def __init__(self, context):
        super(MyChem, self).__init__("mychem", context)

    def get_adverse_events(self,drug_node):
        #Don't need to worry about paging in this one, since we'll just return one drug (the one we're asking for)
        #and mychem pages by drug.
        chemblids = drug_node.get_synonyms_by_prefix('CHEMBL')
        return_results = []
        for cid in chemblids:
            ident = Text.un_curie(cid)
            murl = f'{self.url}query?q=chembl.molecule_hierarchy.molecule_chembl_id:{ident}&fields=aeolus'
            result = requests.get(murl).json()
            for hit in result['hits']:
                #import json
                #print(json.dumps(hit,indent=4))
                if 'aeolus' in hit:
                    aeolus = hit['aeolus']
                    for outcome in aeolus['outcomes']:
                        #I think it makes sense to do some filtering here.  I don't want anything unless the lower
                        # CI bound is > 1, and if I have enough counts (at least 5)
                        if outcome['case_count'] <=5:
                            continue
                        if min(outcome['prr_95_ci']) > 1:
                            predicate = LabeledID(identifier="RO:0003302", label="causes_or_contributes_to")
                        elif max(outcome['prr_95_ci']) < 1:
                            predicate = LabeledID(identifier="RO:0002559", label="prevents")
                        else:
                            continue
                        meddra_id = f"MedDRA:{outcome['meddra_code']}"
                        obj_node = KNode(meddra_id, type=node_types.DISEASE_OR_PHENOTYPE, name=outcome['name'])
                        props={'prr':outcome['prr'], 'ror': outcome['ror'], 'case_count': outcome['case_count']}
                        edge = self.create_edge(drug_node, obj_node, 'mychem.get_adverse_events',  cid, predicate, url = murl, properties=props)
                        return_results.append( (edge, obj_node) )
        return return_results

    def query(self,url):
        result = requests.get(url).json()
        return result

    def page_calls(self,url,nper):
        newurl=url+f'&size={nper}'
        response = self.query(newurl)
        if 'hits' not in response:
            return []
        all_hits = response['hits']
        num_hits = response['total']
        while len(all_hits) < num_hits:
            lall = len(all_hits)
            print(lall, num_hits)
            url_page = newurl+f'&from={len(all_hits)}'
            response = self.query(url_page)
            if 'hits' in response:
                all_hits += response['hits']
        return all_hits

    def get_drug_from_adverse_events(self,input_node):
        """Given a node (drug or phenotype), find chemicals that have a high or low rate of causing the node
        concept as an adverse event"""
        meddras = input_node.get_labeled_ids_by_prefix('MedDRA')
        return_results = []
        for meddra in meddras:
            mname = meddra.label
            murl = f'{self.url}query?q=aeolus.outcomes.name:{mname}'
            hits = self.page_calls(murl,100)
            for hit in hits:
                #import json
                #print(json.dumps(hit,indent=4))
                if 'aeolus' in hit:
                    aeolus = hit['aeolus']
                    for outcome in aeolus['outcomes']:
                        #I think it makes sense to do some filtering here.  I don't want anything unless the lower
                        # CI bound is > 1, and if I have enough counts (at least 5)
                        if (outcome['name'] != mname):
                            continue
                        print(outcome['name'], outcome['case_count'], outcome['prr_95_ci'])
                        if outcome['case_count'] > 5 and min(outcome['prr_95_ci']) > 1:
                            predicate = LabeledID(identifier="RO:0003302", label="causes_or_contributes_to")
                        elif outcome['case_count'] > 5 and max(outcome['prr_95_ci']) < 1:
                            predicate = LabeledID(identifier="RO:0002559", label="prevents")
                        else:
                            continue
                        drug_node=self.make_drug_node(hit)
                        if drug_node is None:
                            continue
                            #obj_node = KNode(meddra_id, node_type = node_types.DISEASE_OR_PHENOTYPE, name=outcome['name'])
                        props={'prr':outcome['prr'], 'ror': outcome['ror'], 'case_count': outcome['case_count']}
                        edge = self.create_edge(drug_node, input_node, 'mychem.get_adverse_events', mname , predicate, url = murl, properties=props)
                        return_results.append( (edge, drug_node) )
        return return_results

    def make_drug_node(self,hit_element):
        """Given a 'hit' from the mychem result, construct a drug node.  Try to get it with chembl, and if that
        fails, chebi.  Failing that, complain bitterly."""
        if 'chembl' in hit_element:
            chembl=hit_element['chembl']
            return KNode(f"CHEMBL:{chembl['molecule_chembl_id']}", type=node_types.DRUG, name=chembl['pref_name'])
        if 'chebi' in hit_element:
            chebi = hit_element['chebi']
            return KNode(chebi['chebi_id'], type=node_types.DRUG, name=chebi['chebi_name'])
        logger.error('hit from mychem.info did not return a chembl or a chebi element')
        logger.error(f'got these keys: {list(hit_element.keys())}')
        return None

