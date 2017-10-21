from collections import defaultdict
from csv import DictReader
import logging
import pronto
import requests
import os
from reasoner.graph_components import KNode,KEdge,elements_to_json

class DiseaseOntology (object):
    def __init__(self, obo_resource="http://purl.obolibrary.org/obo/doid.obo"):
        self.disease_ontology_data = 'doid.obo'
        self.initialized = False
        self.obo_resource = obo_resource
        self.pharos_map = None
        self.pmap = None
    def load (self):
        if not os.path.exists (self.disease_ontology_data):
            url = self.obo_resource
            print ("Downloading disease ontology: {0}".format (url))
            r = requests.get(url, stream=True)
            with open(self.disease_ontology_data, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024): 
                    if chunk: # filter out keep-alive new chunks
                        f.write(chunk)                
        #print ("Loading disease ontology")
        self.ont = pronto.Ontology (self.disease_ontology_data)
        ''' print (ont.json) '''


        self.doid_to_mesh_map = defaultdict(lambda:[])
        for term in self.ont:
            xref = None
            if 'xref' in term.other:
                for xref in term.other['xref']:
                    if xref.startswith ("MESH:"):
                        #print ("doid: {0} -> {1}".format (term.id.upper (), xref))
                        self.doid_to_mesh_map[term.id.upper ()].append (xref)
        self.initialized = True

    def doid_to_mesh (self, doid):
        if not self.initialized:
            self.load ()
        return self.doid_to_mesh_map [doid]
    def doid_to_pharos(self,doid):
        if not self.pmap:
            self.pmap = defaultdict(list)
            with open(os.path.join(os.path.dirname(__file__), 'pharos.id.txt'),'r') as inf:     #'pharos.id.txt','r') as inf:
                rows = DictReader(inf,dialect='excel-tab')
                for row in rows:
                    if row['DOID'] != '':
                        doidlist = row['DOID'].split(',')
                        for d in doidlist:
                            self.pmap[d].append(row['PharosID'])
        pharos_list = self.pmap[doid.identifier]
        if len(pharos_list) == 0:
            logging.getLogger('application').warn('Unable to translate %s into Pharos ID' % doid)
            return None
        return list(map(lambda v : ( KEdge('doid->pharos','D'), KNode(identifier=v, node_type='pharos_disease_id') ), pharos_list))
    
    def doid_to_pharos0(self, doid):
        """Convert a subject with a DOID into a Pharos Disease ID"""
        #TODO: This relies on a pretty ridiculous caching of a map between pharos ids and doids.  
        #      As Pharos improves, this will not be required, but for the moment I don't know a better way.
        if self.pharos_map == None:
            self.pharos_map = {}
            with open(os.path.join(os.path.dirname(__file__), 'pharos.id.txt'),'r') as inf:
                rows = DictReader(inf,dialect='excel-tab')
                for row in rows:
                    if row['DOID'] != '':
                        self.pharos_map[row['DOID']] = row['PharosID']
        return self.pharos_map[doid] if doid in self.pharos_map else None
    
if __name__ == "__main__":
    do = DiseaseOntology ()
    print (do.doid_to_mesh ("DOID:2841"))
    print (do.doid_to_pharos("DOID:2841"))
