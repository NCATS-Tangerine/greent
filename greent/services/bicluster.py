import logging
import requests
from datetime import datetime as dt
from greent.service import Service
from greent.graph_components import KNode, LabeledID
from greent.util import Text, LoggingUtil
from greent import node_types

logger = LoggingUtil.init_logging(__name__, level=logging.DEBUG)

class Bicluster(Service):
    """ Interface to the Comparative Toxicogenomic Database data set."""
    def __init__(self, context):
        super(Bicluster, self).__init__("bicluster", context)

    def standardize_predicate(self, predicate, sourcenode=None, targetnode=None):
        """CTD has a little more work to do than the standard service."""
        if '|' not in predicate.label:
            return self.concept_model.standardize_relationship(predicate)
        parts = predicate.label.split('|')
        goodparts = list(filter(lambda p:'reaction' not in p and 'cotreatment' not in p, parts))
        if len(goodparts) != 1:
            return self.concept_model.standardize_relationship(LabeledID(identifier='CTD:interacts_with', label='interacts_with'))
        #Change the modifier to "affects" to deal with the fact that we don't know what the deleted part does.
        thing = self.term_parents[goodparts[0].split('^')[1]]
        new_id = f'CTD:affects^{thing}'
        return self.concept_model.standardize_relationship(LabeledID(identifier=new_id, label=new_id))


    def gene_to_tissues(self, drug):
        output = []
        identifiers = drug.get_synonyms_by_prefix('NCBIGENE')
        for identifier in identifiers:
            url=f"{self.url}/RNAseqDB_bicluster_gene_to_tissue_gene/ncbigene:{Text.un_curie(identifier)}/"
            obj = requests.get(url).json ()
            for r in obj:
                anatomy_id = r['col_enrich_UBERON']
                if anatomy_id == '':
                    continue
                predicate = LabeledID(identifier='RO:0002610', label='correlated with')
                anat_node = KNode(anatomy_id, type=node_types.ANATOMICAL_ENTITY)
                if sum([s in predicate.identifier for s in self.g2d_strings]) > 0:
                    subject = gene_node
                    object = drug
                else:
                    subject = drug
                    object = gene_node
                edge = self.create_edge(subject,object,'ctd.drug_to_gene',identifier,predicate,
                                        publications=[f"PMID:{r['PubMedIDs']}"],url=url,properties=props)
                output.append( (edge,gene_node) )
        return output


