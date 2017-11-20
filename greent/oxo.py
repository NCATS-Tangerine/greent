import json
import requests
from greent.service import Service
from greent.graph_components import KNode, KEdge
from greent import node_types

class OXO(Service):

    """ Generic id translation service. Essentially a highly generic synonym finder. """
    def __init__(self, context): #url="https://www.ebi.ac.uk/spot/oxo/api/search?size=500"):
        super(OXO, self).__init__("oxo", context)
        self.build_valid_curie_prefixes()

    def build_valid_curie_prefixes(self):
        """Query for the current valid list of input curies"""
        #size defaults to 40...
        url = "https://www.ebi.ac.uk/spot/oxo/api/datasources?size=10000"
        response = requests.get (url).json ()
        self.curies = set()
        for ds in response['_embedded']['datasources']:
            self.curies.add(ds['prefix'])
            self.curies.update( ds['alternatePrefix'] )
        self.curies.add('MESH')

    def is_valid_curie_prefix(self, cp):
        return cp in self.curies

    def request (self, url, obj):
        return requests.post (self.url,
                              data=json.dumps (obj, indent=2),
                              headers={ "Content-Type" : "application/json" }).json ()

    def query (self, ids, distance=2):        
        return self.request (
            url = self.url,
            obj = {
                "ids"           : ids,
                "mappingTarget" : [],
                "distance"      : str(distance),
                "size"          : 10000
            })
    
    def get_synonyms( self, identifier, distance=2 ):
        """ Find all synonyms for a curie for a given distance . """
        result = []
        response = self.query (ids=[ identifier ], distance=distance)
        searchResults = response['_embedded']['searchResults']
        if len(searchResults) > 0 and searchResults[0]['queryId'] == identifier:
            others = searchResults[0]['mappingResponseList']
        return others

    def get_specific_synonym( self, identifier, prefix, distance=2 ):
        synonyms = self.get_synonyms( identifier, distance )
        return list( filter( lambda x: x['targetPrefix'] == prefix ,synonyms) )

    def get_specific_synonym_expanding(self, identifier, prefix):
        for i in range(1,4):
            synonyms = self.get_specific_synonym( identifier, prefix, distance=i )
            if len(synonyms) > 0:
                return synonyms
        return []

    def mesh_to_other (self, mesh_id):
        """ Find connections from a mesh id to other vocabulary domains. """
        return get_synonyms( mesh_id )
    
    def compile_results(self, fname, ntype, searchResults):
        result = []
        for other in searchResults:
            result.append( ( KEdge('oxo',fname, is_synonym=True),
                           KNode(identifier=other['curie'], node_type=ntype  )) )
        return result

    def efo_to_doid(self, efo_node):
        searchResults = self.get_specific_synonym( efo_node.identifier, 'DOID' )
        return self.compile_results('efo_to_doid',node_types.DISEASE, searchResults)

    def efo_to_umls(self, efo_node):
        searchResults = self.get_specific_synonym( efo_node.identifier, 'UMLS' )
        return self.compile_results('efo_to_umls',node_types.DISEASE, searchResults)

    def umls_to_doid(self, umls_node):
        searchResults = self.get_specific_synonym( umls_node.identifier, 'DOID' )
        return self.compile_results('umls_to_doid',node_types.DISEASE, searchResults)

    def ncit_to_hp(self, ncit_node):
        searchResults = self.get_specific_synonym( ncit_node.identifier, 'HP' )
        return self.compile_results('efo_to_umls',node_types.DISEASE, searchResults)

def test():
    from service import ServiceContext
    oxo = OXO(ServiceContext.create_context())
    r=oxo.query(['CL:85'])
    import json
    print (json.dumps(r, indent=4) )

def test2():
    from service import ServiceContext
    oxo = OXO(ServiceContext.create_context())
    r=oxo.efo_to_doid(KNode('EFO:0000764', node_types.DISEASE))
    print( r )

def test3():
    from service import ServiceContext
    oxo = OXO(ServiceContext.create_context())
    r=oxo.get_synonyms('EFO:0000764')
    import json
    print( json.dumps(r,indent=2) )

def test4():
    from service import ServiceContext
    oxo = OXO(ServiceContext.create_context())
    r=oxo.get_specific_synonym_expanding('DOID:9352', 'ICD9CM')
    import json
    print( json.dumps(r,indent=2) )


if __name__ == '__main__':
    test4()

'''

Reference: a conversation with OXO:

General:

Request URL:http://www.ebi.ac.uk/spot/oxo/api/search?size=500
Request Method:POST
Status Code:200 OK
Remote Address:193.62.193.80:80
Referrer Policy:no-referrer-when-downgrade

Response Headers view source

Cache-Control:no-cache, no-store, max-age=0, must-revalidate
Content-Type:application/json;charset=UTF-8
Date:Mon, 25 Sep 2017 14:24:54 GMT
Expires:0
Pragma:no-cache
Server:Apache-Coyote/1.1
Strict-Transport-Security:max-age=0
Transfer-Encoding:chunked
X-Application-Context:application
X-Content-Type-Options:nosniff
X-Frame-Options:DENY
X-XSS-Protection:1; mode=block


Request Headers view source

Accept:application/json, text/javascript, */*; q=0.01
Accept-Encoding:gzip, deflate
Accept-Language:en-US,en;q=0.8
Connection:keep-alive
Content-Length:58
Content-Type:application/json; charset=UTF-8
Cookie:_ga=GA1.3.1876641436.1506348116; _gid=GA1.3.425452358.1506348116; _gat=1
Host:www.ebi.ac.uk
Origin:http://www.ebi.ac.uk
Referer:http://www.ebi.ac.uk/spot/oxo/search
User-Agent:Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.101 Safari/537.36
X-Requested-With:XMLHttpRequest

Query String Parameters view source

view URL encoded
size:500


Request Payload view source

{ids: ["MESH:D001249"], mappingTarget: [], distance: "2"}







RESPONSE:


{
  "_embedded" : {
    "searchResults" : [ {
      "queryId" : "MESH:D001249",
      "querySource" : null,
      "curie" : "MeSH:D001249",
      "label" : "Asthma",
      "mappingResponseList" : [ {
        "curie" : "MedDRA:10003553",
        "label" : "Neutrophilic asthma",
        "sourcePrefixes" : [ "UMLS" ],
        "targetPrefix" : "MedDRA",
        "distance" : 2
      }, {
        "curie" : "HP:0002099",
        "label" : "Asthma",
        "sourcePrefixes" : [ "HP" ],
        "targetPrefix" : "HP",
        "distance" : 1
      }, {
        "curie" : "UMLS:C3714497",
        "label" : "Reactive airway disease",
        "sourcePrefixes" : [ "HP", "UMLS" ],
        "targetPrefix" : "UMLS",
        "distance" : 2
      }, {
        "curie" : "OMIM:608584",
        "label" : "ASTHMA-RELATED TRAITS, SUSCEPTIBILITY TO, 2",
        "sourcePrefixes" : [ "EFO" ],
        "targetPrefix" : "OMIM",
        "distance" : 2
      }, {
        "curie" : "OMIM:611960",
        "label" : "ASTHMA-RELATED TRAITS, SUSCEPTIBILITY TO, 7",
        "sourcePrefixes" : [ "EFO" ],
        "targetPrefix" : "OMIM",
        "distance" : 2
      }, {
        "curie" : "SNOMEDCT:278517007",
        "label" : "Asthmatic bronchitis",
        "sourcePrefixes" : [ "BAO", "DOID", "UMLS" ],
        "targetPrefix" : "SNOMEDCT",
        "distance" : 2
      }, {
        "curie" : "OMIM:600807",
        "label" : "ASTHMA, SUSCEPTIBILITY TO",
        "sourcePrefixes" : [ "EFO", "BAO", "DOID" ],
        "targetPrefix" : "OMIM",
        "distance" : 2
      }, {
        "curie" : "SNOMEDCT:266365004",
        "label" : "",
        "sourcePrefixes" : [ "BAO", "DOID" ],
        "targetPrefix" : "SNOMEDCT",
        "distance" : 2
      }, {
        "curie" : "SNOMEDCT:187687003",
        "label" : "Asthma",
        "sourcePrefixes" : [ "BAO", "DOID", "UMLS" ],
        "targetPrefix" : "SNOMEDCT",
        "distance" : 2
      }, {
        "curie" : "OMIM:611064",
        "label" : "ASTHMA-RELATED TRAITS, SUSCEPTIBILITY TO, 5",
        "sourcePrefixes" : [ "EFO" ],
        "targetPrefix" : "OMIM",
        "distance" : 2
      }, {
        "curie" : "EFO:0000270",
        "label" : "asthma",
        "sourcePrefixes" : [ "EFO" ],
        "targetPrefix" : "EFO",
        "distance" : 1
      }, {
        "curie" : "KEGG:05310",
        "label" : "",
        "sourcePrefixes" : [ "BAO", "DOID" ],
        "targetPrefix" : "KEGG",
        "distance" : 2
      }, {
        "curie" : "SNOMEDCT:155574008",
        "label" : "Asthma",
        "sourcePrefixes" : [ "BAO", "DOID", "UMLS" ],
        "targetPrefix" : "SNOMEDCT",
        "distance" : 2
      }, {
        "curie" : "ICD10CM:J45.909",
        "label" : "Unspecified asthma, uncomplicated",
        "sourcePrefixes" : [ "BAO", "DOID" ],
        "targetPrefix" : "ICD10CM",
        "distance" : 2
      }, {
        "curie" : "SNOMEDCT:266398009",
        "label" : "Asthma: [NOS] or [attack]",
        "sourcePrefixes" : [ "BAO", "DOID" ],
        "targetPrefix" : "SNOMEDCT",
        "distance" : 2
      }, {
        "curie" : "ICD10CM:J45.90",
        "label" : "Late onset asthma",
        "sourcePrefixes" : [ "BAO", "DOID" ],
        "targetPrefix" : "ICD10CM",
        "distance" : 2
      }, {
        "curie" : "NCIt:C28397",
        "label" : "Asthma",
        "sourcePrefixes" : [ "EFO", "BAO", "DOID", "UMLS" ],
        "targetPrefix" : "NCIt",
        "distance" : 2
      }, {
        "curie" : "ICD9CM:493.9",
        "label" : "",
        "sourcePrefixes" : [ "BAO", "DOID" ],
        "targetPrefix" : "ICD9CM",
        "distance" : 2
      }, {
        "curie" : "DOID:2841",
        "label" : "asthma",
        "sourcePrefixes" : [ "BAO", "DOID" ],
        "targetPrefix" : "DOID",
        "distance" : 1
      }, {
        "curie" : "SNOMEDCT:195967001",
        "label" : "Hyperreactive airway disease",
        "sourcePrefixes" : [ "EFO", "BAO", "DOID", "HP", "UMLS" ],
        "targetPrefix" : "SNOMEDCT",
        "distance" : 2
      }, {
        "curie" : "SNOMEDCT:195979001",
        "label" : "",
        "sourcePrefixes" : [ "BAO", "DOID" ],
        "targetPrefix" : "SNOMEDCT",
        "distance" : 2
      }, {
        "curie" : "ICD9CM:493",
        "label" : "Asthma",
        "sourcePrefixes" : [ "EFO", "BAO", "DOID", "UMLS" ],
        "targetPrefix" : "ICD9CM",
        "distance" : 2
      }, {
        "curie" : "SNOMEDCT:21341004",
        "label" : "Asthmatic bronchitis",
        "sourcePrefixes" : [ "BAO", "DOID", "UMLS" ],
        "targetPrefix" : "SNOMEDCT",
        "distance" : 2
      }, {
        "curie" : "SNOMEDCT:155579003",
        "label" : "Asthma: [NOS] or [attack]",
        "sourcePrefixes" : [ "BAO", "DOID" ],
        "targetPrefix" : "SNOMEDCT",
        "distance" : 2
      }, {
        "curie" : "ICD10CM:J45",
        "label" : "Asthma",
        "sourcePrefixes" : [ "BAO", "DOID", "UMLS" ],
        "targetPrefix" : "ICD10CM",
        "distance" : 2
      }, {
        "curie" : "UMLS:C0004096",
        "label" : "Asthma",
        "sourcePrefixes" : [ "UMLS" ],
        "targetPrefix" : "UMLS",
        "distance" : 1
      }, {
        "curie" : "SNOMEDCT:991000119106",
        "label" : "Reactive airway disease",
        "sourcePrefixes" : [ "HP" ],
        "targetPrefix" : "SNOMEDCT",
        "distance" : 2
      }, {
        "curie" : "OMIM:MTHU003537",
        "label" : "Asthma",
        "sourcePrefixes" : [ "UMLS" ],
        "targetPrefix" : "OMIM",
        "distance" : 2
      }, {
        "curie" : "SNOMEDCT:195983001",
        "label" : "(Asthma: [exercise induced] or [allergic NEC] or [NOS]) or (allergic bronchitis NEC)",
        "sourcePrefixes" : [ "BAO", "DOID" ],
        "targetPrefix" : "SNOMEDCT",
        "distance" : 2
      }, {
        "curie" : "OMIM:607277",
        "label" : "ASTHMA-RELATED TRAITS, SUSCEPTIBILITY TO, 1",
        "sourcePrefixes" : [ "EFO" ],
        "targetPrefix" : "OMIM",
        "distance" : 2
      } ],
      "_links" : {
        "self" : {
          "href" : "http://www.ebi.ac.uk/spot/oxo/api/terms/MeSH:D001249"
        },
        "mappings" : {
          "href" : "http://www.ebi.ac.uk/spot/oxo/api/mappings?fromId=MeSH:D001249"
        }
      }
    } ]
  },
  "_links" : {
    "self" : {
      "href" : "http://www.ebi.ac.uk/spot/oxo/api/search"
    }
  },
  "page" : {
    "size" : 500,
    "totalElements" : 1,
    "totalPages" : 1,
    "number" : 0
  }
}
'''
