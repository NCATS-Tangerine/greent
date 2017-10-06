# GreenT

GreenT is a library of interfaces to biomedical and environmental data services.

Developed at the University of North Carolina at Chapel Hill, the API provides a Python package, Python interface, and a GraphQL service.

## Installation

```
(virtualenv)$ pip install greent
```

## Running the GraphQL Server:
```
(virtualenv)$ python -m greent.app
```

## Usage Example

```
from greent.client import GraphQL
translator = GraphQL ()    
Translation = namedtuple ('Translation', [ 'thing', 'domain_a', 'domain_b' ])
translations = [
  Translation ("Imatinib",     "http://chem2bio2rdf.org/drugbank/resource/Generic_Name", "http://chem2bio2rdf.org/uniprot/resource/gene"),      
  Translation ("CDC25A",       "http://chem2bio2rdf.org/uniprot/resource/gene",          "http://chem2bio2rdf.org/kegg/resource/kegg_pathway"), 
  Translation ("CACNA1A",      "http://chem2bio2rdf.org/uniprot/resource/gene",          "http://pharos.nih.gov/identifier/disease/name"),      
  Translation ("Asthma",       "http://identifiers.org/mesh/disease/name",               "http://identifiers.org/mesh/drug/name"),              
  Translation ("DOID:2841",    "http://identifiers.org/doid",                            "http://identifiers.org/mesh/disease/id"),             
  Translation ("MESH:D001249", "http://identifiers.org/mesh",                            "http://identifiers.org/doi")
]
def test_translations (self):
  for index, translation in enumerate (self.translations):
    result = self.translator.translate (
      thing=translation.thing,
      domain_a=translation.domain_a,
      domain_b=translation.domain_b)
```

By default, the constructor above will use the public GraphQL API instance hosted at RENCI: 
```
https://stars-app.renci.org/greent/graphql
```

## Services

GreenT currently presents four main services. More information coming on these soon.

### ChemBio

This is a set of endpoints relating to the Chem2Bio2RDF data set.

### Clinical

A set of clinical derived data with no personally identifiable information.

### Environmental Exposures

Data derived from the CMAQ model.

### Chemotext

Mention data for terms in PubMed's Medline meta data.

