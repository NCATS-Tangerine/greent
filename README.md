# GreenT

![Autobuild](https://travis-ci.org/NCATS-Tangerine/greent.svg?branch=master)

GreenT is a library of interfaces to biomedical and environmental data services.

## Services

The data sources GreenT provides access to are highly heterogeneous in terms of technologies and data types. We have multiple kinds of graph databases, tabuar data, spatio-temporal environmental data, and machine learning models. 

At the same time, we value presenting a coherent, usable interface to users. 

So all of the services described below are available via
* A **Python** API
* A **GraphQL** REST API
* The graphical GraphQL **query editor**

GreenT currently presents some data from the following services:

| **API**            | Technology | Provider     |  Description                                                    |
| -------------------|------------|--------------|-----------------------------------------------------------------|
| **ChemBio**        | SPARQL     | U-Indiana/RENCI| Chemical and biological [data](http://cheminfov.informatics.indiana.edu:8080/c2b2r/) via [chem2bio2rdf](http://stars-blazegraph.renci.org/blazegraph/#query)               |
| **Exposures**      | OpenAPI    | UNC-IE-RENCI | Environmental exposures modeled by [CMAQ](https://exposures.renci.org/v1/ui/#/default)   |
| **Clinical**       | OpenAPI    | UNC-CDW      | De-identified clinical data                                 |
| **Endotype**       | OpenAPI    | UNC-CDW-RENCI| Data driven [disease classification / machine learning model](https://endotypes.renci.org/v1/ui/)    |
| **Chemotext**      |  Neo4J     | UNC-ESHELMAN | Chemical and biological data via [chem2bio2rdf](http://cheminfov.informatics.indiana.edu:8080/c2b2r/)   |
| **Pharos**         |  OpenAPI   | NIH          | Drug / Gene / Disease information from the [NIH](https://pharos.nih.gov/idg/api)  |
| **OXO**            |  REST API  | EMBL-EBI     | Identifier [XRef service](https://www.ebi.ac.uk/spot/oxo/)  |
| **DiseaseOntology**|OBO Ontology| UofM / IGS   | Ontology of [disease](http://disease-ontology.org/)         |

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
$ PYTHONPATH=$PWD/.. python rosetta.py
/Users/scox/dev/venv/trans/lib/python3.6/site-packages/cachier/mongo_core.py:24: UserWarning: Cachier warning: pymongo was not found. MongoDB cores will not work.
  "Cachier warning: pymongo was not found. MongoDB cores will not work.")
** Initializing async pharos
2017-10-23 11:00:42,627 rosetta.py get_translations DEBUG: Mapped types: ['NAME', 'mesh_disease_id', 'mesh_disease_name', 'pharos_disease_id', 'DOID'] : ['hetio_anatomy']
2017-10-23 11:00:42,627 rosetta.py get_translations DEBUG: Translation(obj: KNode(id=NAME:diabetes,type=D) type_a: NAME type_b: hetio_anatomy desc:  then:  response: '')
2017-10-23 11:00:42,627 rosetta.py get_translations DEBUG: Translation(obj: KNode(id=NAME:diabetes,type=D) type_a: mesh_disease_id type_b: hetio_anatomy desc:  then:  response: '')
2017-10-23 11:00:42,627 rosetta.py get_translations DEBUG: Translation(obj: KNode(id=NAME:diabetes,type=D) type_a: mesh_disease_name type_b: hetio_anatomy desc:  then:  response: '')
2017-10-23 11:00:42,627 rosetta.py get_translations DEBUG: Translation(obj: KNode(id=NAME:diabetes,type=D) type_a: pharos_disease_id type_b: hetio_anatomy desc:  then:  response: '')
2017-10-23 11:00:42,628 rosetta.py get_translations DEBUG: Translation(obj: KNode(id=NAME:diabetes,type=D) type_a: DOID type_b: hetio_anatomy desc:  then:  response: '')
2017-10-23 11:00:42,628 rosetta.py get_transitions DEBUG:   path: ['http://identifiers.org/string', 'http://identifiers.org/doid', 'http://identifiers.org/mesh/disease/id', 'http://identifiers.org/uniprot', 'http://identifier.org/hetio/anatomy']
2017-10-23 11:00:42,628 rosetta.py get_transitions DEBUG:   steps: [('http://identifiers.org/string', 'http://identifiers.org/doid'), ('http://identifiers.org/doid', 'http://identifiers.org/mesh/disease/id'), ('http://identifiers.org/mesh/disease/id', 'http://identifiers.org/uniprot'), ('http://identifiers.org/uniprot', 'http://identifier.org/hetio/anatomy')]
2017-10-23 11:00:42,628 rosetta.py get_transitions DEBUG:     trans: ('http://identifiers.org/string', 'http://identifiers.org/doid', {'data': {'op': 'tkba.name_to_doid'}})
2017-10-23 11:00:42,628 rosetta.py get_transitions DEBUG:     trans: ('http://identifiers.org/doid', 'http://identifiers.org/mesh/disease/id', {'data': {'op': 'disease_ontology.graph_doid_to_mesh'}})
2017-10-23 11:00:42,628 rosetta.py get_transitions DEBUG:     trans: ('http://identifiers.org/mesh/disease/id', 'http://identifiers.org/uniprot', {'data': {'op': 'chembio.graph_get_genes_by_disease'}})
2017-10-23 11:00:42,628 rosetta.py get_transitions DEBUG:     trans: ('http://identifiers.org/uniprot', 'http://identifier.org/hetio/anatomy', {'data': {'op': 'hetio.gene_to_anatomy'}})
2017-10-23 11:00:42,628 rosetta.py translate DEBUG:             invoke(cyc:1): tkba.name_to_doid(KNode(id=NAME:diabetes,type=D)) => 
2017-10-23 11:00:44,965 rosetta.py translate DEBUG:               response>: [(KEdge(edge_source=tkba,edge_type=queried), KNode(id=DOID:9744,type=D))]
2017-10-23 11:00:44,965 rosetta.py translate DEBUG:             invoke(cyc:1): disease_ontology.graph_doid_to_mesh(KNode(id=DOID:9744,type=D)) => 
2017-10-23 11:00:45,661 rosetta.py translate DEBUG:               response>: [(KEdge(edge_source=doid->mesh,edge_type=queried), KNode(id=MESH:D003922,type=D))]
2017-10-23 11:00:45,661 rosetta.py translate DEBUG:             invoke(cyc:1): chembio.graph_get_genes_by_disease(KNode(id=MESH:D003922,type=D)) => 
2017-10-23 11:00:46,846 rosetta.py translate DEBUG:               response>: [(KEdge(edge_source=c2b2r,edge_type=diseaseToGene), KNode(id=UNIPROT:AKR1B1,type=G)), (KEdge(edge_source=c2b2r...
2017-10-23 11:00:46,847 rosetta.py translate DEBUG:             invoke(cyc:1): hetio.gene_to_anatomy(KNode(id=UNIPROT:AKR1B1,type=G)) => 
2017-10-23 11:00:47,051 rosetta.py translate DEBUG:               response>: [(KEdge(edge_source=gene-anat,edge_type=queried), KNode(id=UBERON:0002390,type=A)), (KEdge(edge_source=gene-an...
2017-10-23 11:00:47,051 rosetta.py translate DEBUG:             invoke(cyc:2): hetio.gene_to_anatomy(KNode(id=UNIPROT:GMDS,type=G)) => 
2017-10-23 11:00:47,394 rosetta.py translate DEBUG:               response>: [(KEdge(edge_source=gene-anat,edge_type=queried), KNode(id=UBERON:0002048,type=A)), (KEdge(edge_source=gene-an...
```

By default, the constructor above will use the public GraphQL API instance hosted at RENCI: 
```
https://stars-app.renci.org/greent/graphql
```
To use a development server instead, you can install the module and run:
```
python -m greent.app
```

Then use http://localhost:5000 as the URL for the GraphQL client.

Also, note that you can send arbitrary GraphQL queries to the client with syntax like this:
```
translator.query ({
   "query" : "<query_text>"
   })
```
Constants for referring to vocabulary terms can be found in the [greent.translator.Vocab](https://github.com/NCATS-Tangerine/greent/blob/master/greent/translator.py#L13) module.
