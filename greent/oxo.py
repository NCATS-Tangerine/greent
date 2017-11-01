import json
import requests
from greent.service import Service

class OXO(Service):

    """ Generic id translation service. Essentially a highly generic synonym finder. """
    def __init__(self, context): #url="https://www.ebi.ac.uk/spot/oxo/api/search?size=500"):
        super(OXO, self).__init__("oxo", context)
        #self.url = url

    def request (self, url, obj):
        return requests.post (self.url,
                              data=json.dumps (obj, indent=2),
                              headers={ "Content-Type" : "application/json" }).json ()
    def query (self, ids):        
        return self.request (
            url = self.url,
            obj = {
                "ids"           : ids,
                "mappingTarget" : [],
                "distance"      : "2"
            })
    
    def mesh_to_other (self, mesh_id):
        """ Find connections from a mesh id to other vocabulary domains. """
        result = []
        response = self.query (ids=[ mesh_id ])
        searchResults = response['_embedded']['searchResults']
        if len(searchResults) > 0 and searchResults[0]['queryId'] == mesh_id:
            others = searchResults[0]['mappingResponseList']
            result = list(map(lambda v : v['curie'], others))
        return result
    
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
