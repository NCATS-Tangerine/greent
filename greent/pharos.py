import asyncio
import concurrent.futures
import requests
import json
import logging
import sys
import traceback
from collections import defaultdict
from collections import namedtuple
from csv import DictReader
from greent.disease_ont import DiseaseOntology
from greent.util import Munge
from greent.util import LoggingUtil
from greent.async import AsyncUtil
from greent.async import Operation
from reasoner.graph_components import KEdge, KNode
from simplejson.scanner import JSONDecodeError

logger = LoggingUtil.init_logging (__name__, logging.DEBUG)

class Pharos(object):
    def __init__(self, url="https://pharos.nih.gov/idg/api/v1"):
        self.url = url
        self.disease_ontology = DiseaseOntology ()
    def request (self, url):
        #print ("pharos url: {}".format (url))
        response = None
        try:
            response = requests.get (url).json ()
        except:
            traceback.print_exc ()
        return response
        
    def query (self, query):
        return self.request ("{0}/{1}".format (self.url, query))
    def target_to_id (self, target):
        target_map = self.query (query="targets/{0}".format (Munge.gene (target)))
        return target_map["id"] if "id" in target_map else None
    def target_to_disease (self, target_sym):
        target_id = self.target_to_id (Munge.gene (target_sym))
        links = self.query (query="targets({0})/links".format (target_id))
        result = []
        for k in links:
            if k['kind'] == "ix.idg.models.Disease":
                for p in k['properties']:
                    #print (p)
                    if p['label'] == 'IDG Disease':
                        result.append (p['term'])
        return result
    
    def disease_map (self, disease_id):
        return self.query (query="diseases({0})".format (disesase_id))

    def make_doid_id (self, obj):
        if not obj:
            return None
        result = obj
        if isinstance (obj, KNode):
            result = obj.identifier
        if isinstance (result, list):
            if len(result) == 1:
                result = result[0]
        if result:
            if isinstance(result, str):
                if result.startswith ('DOID:'):
                    result = result.replace ('DOID:', '')
        return result

    def translate(self, subject_node):
        """Convert a subject with a DOID into a Pharos Disease ID"""
        #TODO: This relies on a pretty ridiculous caching of a map between pharos ids and doids.  
        #      As Pharos improves, this will not be required, but for the moment I don't know a better way.
        pmap = defaultdict(list)
        with open('pharos.id.txt','r') as inf:
            rows = DictReader(inf,dialect='excel-tab')
            for row in rows:
                if row['DOID'] != '':
                    doidlist = row['DOID'].split(',')
                    for d in doidlist:
                        pmap[d].append(row['PharosID'])
        doid = subject_node.identifier
        pharos_list = pmap[doid]
        if len(pharos_list) == 0:
            logging.getLogger('application').warn('Unable to translate %s into Pharos ID' % doid)
            return None
        return pharos_list

    def target_to_hgnc(self, target_id):
        """Convert a pharos target id into an HGNC ID.
        
        The call does not return the actual name for the gene, so we do not provide it.
        There are numerous other synonyms that we could also cache, but I don't see much benefit here"""
        result = None
        #node = self.make_doid_id (target_id)
        try:
            #print ('https://pharos.nih.gov/idg/api/v1/targets(%s)/synonyms' % target_id)
            r = requests.get('https://pharos.nih.gov/idg/api/v1/targets(%s)/synonyms' % target_id)
            result = r.json()
            for synonym in result:
                if synonym['label'] == 'HGNC':
                    result = synonym['term']
        except:
            #traceback.print_exc ()
            pass
        return result

    def disease_get_gene(self, subject):
        pharosids = self.translate (subject)
        print ("pharos ids: {}".format (pharosids))
        original_edge_nodes=[]
        for pharosid in pharosids:
            logger.debug ('pharos> https://pharos.nih.gov/idg/api/v1/diseases(%s)?view=full' % pharosid)
            r = requests.get('https://pharos.nih.gov/idg/api/v1/diseases(%s)?view=full' % pharosid)
            result = r.json()
            for link in result['links']:
                if link['kind'] != 'ix.idg.models.Target':
                    logger.info('Pharos disease returning new kind: %s' % link['kind'])
                else:
                    pharos_target_id = int(link['refid'])
                    pharos_edge = KEdge( 'pharos', 'queried', {'properties': link['properties']} )
                    original_edge_nodes.append( (pharos_edge, pharos_target_id) )

        #Pharos returns target ids in its own numbering system. Collect other names for it.
        resolved_edge_nodes = []
        index = 0
        for edge, pharos_target_id  in original_edge_nodes:
            #logger.debug ("edge: %s", edge)
            hgnc = self.target_to_hgnc(pharos_target_id)
            index = index + 1
            if hgnc is not None:
                #logger.debug ("making hgnc node {0}".format (hgnc))
                hgnc_node = KNode(hgnc, 'G')
                if index < 10:
                    logger.debug ("            hgnc-node: %s", hgnc_node)
                resolved_edge_nodes.append((edge,hgnc_node))
            else:
                logging.getLogger('application').warn('Did not get HGNC for pharosID %d' % pharos_target_id)

        return resolved_edge_nodes

class AsyncPharos(Pharos):

    def __init__(self, url):
        super (AsyncPharos, self).__init__(url)
        
    def disease_get_gene(self, subject):
        pharosids = subject.identifier
        original_edge_nodes=[]
        def process_pharos_response (r):
            try:
                result = r.json()
                for link in result['links']:
                    if link['kind'] != 'ix.idg.models.Target':
                        logger.info('Pharos disease returning new kind: %s' % link['kind'])
                    else:
                        pharos_target_id = int(link['refid'])
                        pharos_edge = KEdge( 'pharos', 'queried', {'properties': link['properties']} )
                        original_edge_nodes.append( (pharos_edge, pharos_target_id) )
            except JSONDecodeError as e:
                pass #logger.error ("got exception %s", e)
        AsyncUtil.execute_parallel_requests (
            urls=[ "https://pharos.nih.gov/idg/api/v1/diseases(%s)?view=full" % p for p in pharosids ],
            response_processor=process_pharos_response)

        logger.debug ("          Getting hgnc ids for pharos id: {}".format (pharosids))
        resolved_edge_nodes = []
        HGNCRequest = namedtuple ('HGNCRequest', [ 'pharos_target_id', 'edge' ])
        index = 0
        def process_hgnc_request (request):
            nonlocal index
            index += 1
            url = "https://pharos.nih.gov/idg/api/v1/targets(%s)/synonyms" % request.pharos_target_id
            if index < 3:
                logger.debug ("        hgnc_url:  {0}".format (url))
            return (requests.get (url).json (), request.edge)
        def process_hgnc_response (response):
            result = response[0]
            edge = response[1]
            hgnc = None
            for synonym in result:
                if synonym['label'] == 'HGNC':
                    hgnc = synonym['term']
            if hgnc is not None:
                hgnc_node = KNode(hgnc, 'G')
                resolved_edge_nodes.append((edge,hgnc_node))
        AsyncUtil.execute_parallel_operations (
            operations=[ Operation(process_hgnc_request, HGNCRequest(pharos_target_id, edge)) for edge, pharos_target_id in original_edge_nodes ], #[:1],
            response_processor=process_hgnc_response)
        
        return resolved_edge_nodes




    

#Poking around on the website there are about 10800 ( a few less )
def build_disease_translation():
    """Write to disk a table mapping Pharos disease ID to DOID (and other?) so we can reverse lookup"""
    with open('pharos.id.txt','w') as pfile:
        pfile.write('PharosID\tDOID\n')
        for pharosid in range(1,10800):
            r = requests.get('https://pharos.nih.gov/idg/api/v1/diseases(%d)/synonyms' % pharosid).json()
            doids = []
            for synonym in r:
                if synonym['label'] == 'DOID':
                    doids.append(synonym['term'])
            if len(doids) > 1:
                #print(doids)
                import json
                #print( json.dumps(r,indent=4) )
                exit()
            elif len(doids) == 0:
                doids.append('')
            pfile.write('%d\t%s\n' % (pharosid, doids[0]))

def test_disese_gene_for_output():
    """Call a function so that we can examine the output"""
    pharosid=455
    r = requests.get('https://pharos.nih.gov/idg/api/v1/diseases(%d)?view=full' % pharosid)
    result = r.json()
    import json
    with open('testpharos.txt','w') as outf:
        json.dump(result,outf,indent=4)

def test_hgnc_for_output():
    """Call a function so that we can examine the output"""
    pharosid=91
    r = requests.get('https://pharos.nih.gov/idg/api/v1/targets(%d)/synonyms' % pharosid)
    result = r.json()
    import json
    with open('testpharos.txt','w') as outf:
        json.dump(result,outf,indent=4)
    
def test ():
#    with open("pharos.txt", "r") as stream:
#        obj = json.loads (stream.read ())
#        print (json.dumps (obj, indent=2))        
    pharos = Pharos ()
    diseases = pharos.target_to_disease ("CACNA1A")
    print (diseases)


        
if __name__ == '__main__':
    test ()
    build_disease_translation ()
    
