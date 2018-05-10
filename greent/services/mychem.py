import requests
from greent import node_types
from greent.graph_components import KNode, KEdge, LabeledID
from greent.service import Service
from greent.util import Text


class MyChem(Service):

    def __init__(self, context):
        super(MyChem, self).__init__("mychem", context)

    def get_adverse_events(self,drug_node):
        chemblids = drug_node.get_synonyms_by_prefix('CHEMBL')
        return_results = []
        for cid in chemblids:
            ident = Text.un_curie(cid)
            murl = f'{self.url}query?q=chembl.molecule_hierarchy.molecule_chembl_id:{ident}'
            result = requests.get(murl).json()
            for hit in result['hits']:
                if 'aeolus' in hit:
                    aeolus = hit['aeolus']
                    for outcome in aeolus['outcomes']:
                        #I think it makes sense to do some filtering here.  I don't want anything unless the lower
                        # CI bound is > 1, and if I have enough counts (at least 5)
                        if outcome['case_count'] > 5 and min(outcome['prr_95_ci']) > 1:
                            meddra_id = f"MEDDRA:{outcome['meddra_code']}"
                            predicate = LabeledID(identifier="RO:0003302",label= "causes_or_contributes_to")
                            obj_node = KNode(meddra_id, node_type = node_types.DISEASE_OR_PHENOTYPE)
                            props={'prr':outcome['prr'], 'ror': outcome['ror'], 'case_count': outcome['case_count']}
                            edge = self.create_edge(drug_node, obj_node, 'mychem.get_adverse_events',  cid, predicate, url = murl, properties=props)
                            return_results.append( (edge, obj_node) )
        return return_results



