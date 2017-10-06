# GreenT

![Autobuild](https://travis-ci.org/NCATS-Tangerine/greent.svg?branch=master)

GreenT is a library of interfaces to biomedical and environmental data services.

## Services

### Primary

GreenT currently presents the following primary services:

| **API**       | Version |   Description                                               |
| --------------|---------|-------------------------------------------------------------|
| **ChemBio**   | 0.2     | Chemical and biological data via [chem2bio2rdf](http://stars-blazegraph.renci.org/blazegraph/#query)               |
| **Exposures** | 0.2     | Environmental exposures modeled by [CMAQ](https://exposures.renci.org/v1/ui/#/default)   |
| **Clinical**  | 0.2     | De-identified clinical data                                 |
| **Endotype**  | 0.1     | Data driven [disease classification machine learning model](https://endotypes.renci.org/v1/ui/)   |

### Secondary

GreenT aggregates some data via the following services. None of these is used to their full potential but we have a start.

| **API**               | Version |   Description                                               |
| ----------------------|---------|-------------------------------------------------------------|
| **Chemotext**         | 0.1     | Chemical and biological data via [chem2bio2rdf](http://cheminfov.informatics.indiana.edu:8080/c2b2r/)   |
| **Pharos**            | 0.1     | Drug / Gene / Disease information from the [NIH](https://pharos.nih.gov/idg/api)  |
| **OXO**               | 0.1     | Identifier [XRef service](https://www.ebi.ac.uk/spot/oxo/)  |
| **Disease Ontology**  | 0.1     | Ontology of [disease](http://disease-ontology.org/)         |

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
    result = translator.translate (
      thing=translation.thing,
      domain_a=translation.domain_a,
      domain_b=translation.domain_b)
```

By default, the constructor above will use the public GraphQL API instance hosted at RENCI: 
```
https://stars-app.renci.org/greent/graphql
```

Constants for referring to vocabulary terms can be found in the [greent.translator.Vocab](https://github.com/NCATS-Tangerine/greent/blob/master/greent/translator.py#L13) module.
