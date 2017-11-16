from collections import defaultdict
from csv import DictReader
import logging
import pronto
import requests
import os
from greent.graph_components import KNode,KEdge,elements_to_json
from greent import node_types
from greent.service import Service

class DiseaseOntology (Service):
    """ We use the Translator Disease Ontology service for other purposes but here, we fill a few gaps
    and keep our options open for digging into the DO further. Also, we map from DOID to Pharos disease ids. """

    def __init__(self, context): #obo_resource="http://purl.obolibrary.org/obo/doid.obo"):
        super(DiseaseOntology, self).__init__('diseaseontology', context)
        self.disease_ontology_data = 'doid.obo'
        self.initialized = False
        self.pharos_map = None
        self.pmap = None

    def load (self):
        """ Load the ontolgy. """
        if not os.path.exists (self.disease_ontology_data):
            url = self.url
            print ("Downloading disease ontology: {0}".format (url))
            r = requests.get(url, stream=True)
            with open(self.disease_ontology_data, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024): 
                    if chunk: # filter out keep-alive new chunks
                        f.write(chunk)

        self.ont = pronto.Ontology (self.disease_ontology_data)
        self.doid_to_mesh_map = defaultdict(lambda:[])
        for term in self.ont:
            xref = None
            if 'xref' in term.other:
                for xref in term.other['xref']:
                    if xref.startswith ("MESH:"):
                        self.doid_to_mesh_map[term.id.upper ()].append (xref)
        self.initialized = True

    def doid_to_mesh (self, doid):
        if not self.initialized:
            self.load ()
        return self.doid_to_mesh_map [doid]

    def graph_doid_to_mesh (self, doid):
        """ Convert a DOID to an equivalent MeSH term. """
        mesh_ids = self.doid_to_mesh (doid.identifier)
        return [ ( self.get_edge (predicate='synonym'),
                   KNode(identifier=i.replace ("MESH:", "MESH.DISEASE:"), node_type=node_types.DISEASE) ) for i in mesh_ids ]

    def doid_or_umls_to_pharos(self,doid):
        """ Convert a doid to a pharos id. Perhaps there's a public service that does this but in the
        mean time, we'll roll our own. """
        if not self.pmap:
            self.pmap = defaultdict(list)
            with open(os.path.join(os.path.dirname(__file__), 'pharos.id.all.txt'),'r') as inf:     #'pharos.id.txt','r') as inf:
                rows = DictReader(inf,dialect='excel-tab')
                for row in rows:
                    if row['DOID'] != '':
                        doidlist = row['DOID'].split(',')
                        for d in doidlist:
                            self.pmap[d.upper()].append(row['PharosID'])
        pharos_list = self.pmap[doid.identifier]
        if len(pharos_list) == 0:
            #logging.getLogger('application').warn('Unable to translate doid: %s into a Pharos ID' % doid)
            return []
        return list(map(lambda v : (
            KEdge('local','doid_to_pharos', is_synonym=True),
            KNode(identifier="PHAROS.DISEASE:{0}".format (v), node_type=node_types.DISEASE) ), pharos_list ))

def test():
    k = KNode("DOID:11476",node_types.DISEASE)
    from service import ServiceContext
    do = DiseaseOntology(ServiceContext.create_context())
    r=do.doid_or_umls_to_pharos(k)
    print (r)

    
if __name__ == "__main__":
    #do = DiseaseOntology ()
    #print (do.doid_to_mesh ("DOID:2841"))
    test()
