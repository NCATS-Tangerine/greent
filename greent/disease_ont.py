from collections import defaultdict
import pronto
import requests
import os

class DiseaseOntology (object):
    def __init__(self):
        disease_ontology_data = 'doid.obo'
        if not os.path.exists (disease_ontology_data):
            url = "http://purl.obolibrary.org/obo/doid.obo"
            print ("Downloading disease ontology: {0}".format (url))
            r = requests.get(url, stream=True)
            with open(disease_ontology_data, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024): 
                    if chunk: # filter out keep-alive new chunks
                        f.write(chunk)                
        print ("Loading disease ontology")
        ont = pronto.Ontology (disease_ontology_data)
        ''' print (ont.json) '''
        self.doid_to_mesh_map = defaultdict(lambda:[])
        for term in ont:
            xref = None
            if 'xref' in term.other:
                for xref in term.other['xref']:
                    if xref.startswith ("MESH:"):
                        #print ("doid: {0} -> {1}".format (term.id.upper (), xref))
                        self.doid_to_mesh_map[term.id.upper ()].append (xref)

    def doid_to_mesh (self, doid):
        return self.doid_to_mesh_map [doid]
    

#do = DiseaseOntology ()
#print (do.doid_to_mesh_map ["DOID:2841"])
