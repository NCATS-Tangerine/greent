import pronto
import requests
import os
from collections import defaultdict
from reasoner.graph_components import KEdge,KNode
from service import Service
from service import ServiceContext

class HumanPhenotypeOntology (Service):
    def __init__(self, context):
        super(HumanPhenotypeOntology, self).__init__('hpo', context)
        hpo_data = 'hpo.obo'
        if not os.path.exists (hpo_data):
            #url = "http://purl.obolibrary.org/obo/hp.obo"
            print ("Downloading human phenotype ontology: {0}".format (self.url))
            r = requests.get(self.url, stream=True)
            with open(hpo_data, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024): 
                    if chunk: # filter out keep-alive new chunks
                        f.write(chunk)
                    
        print ("Loading human phenotype ontology")
        self.ont = pronto.Ontology (hpo_data)
        self.mesh_to_pheno = defaultdict(lambda:[])
        for term in self.ont:
            xref = None
            if 'xref' in term.other:
                for xref in term.other['xref']:
                    if xref.startswith ("MSH:"):
                        logger.debug ("xref: {0} -> {1}".format (term.id, xref))
                        self.mesh_to_pheno[xref.upper()].append (term.id.upper()) #term.id.upper ()].append (xref)
        ''' print (ont.json) '''
        
    def mesh_to_phenotype (self, term):
        result = self.mesh_to_pheno.get (term.identifier.replace ('MESH:', 'MSH:'), []) \
                 if term.identifier.startswith ("MESH:") else []
        return [ ( self.get_edge({ 'mesh_id' : r }), KNode(r, 'D') ) for r in result ]
                   
    def get_term (self, term_id):
        return self.ont[term_id]
    

hpo = HumanPhenotypeOntology (ServiceContext.create_context ())
term = hpo.get_term ('HP:0002099')

def dump_term (t):
    print (t)
    #print (t['def'])
    for c in t.rchildren ():
        print ("  child: {}".format (c))
        #print ("  child: {}".format (c['def']))
        dump_term (c)
    for r in t.relations:
        print ("relation: {}".format (r))
        
print (hpo.mesh_to_phenotype (KNode("DOID:2841", "")))
print (hpo.mesh_to_phenotype (KNode("MESH:D001249", "")))
