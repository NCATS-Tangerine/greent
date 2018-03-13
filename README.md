# GreenT

GreenT began as a library of interfaces to biomedical and environmental data services.

It is now host to Rosetta, the data access layer of the Gamma reasoner.

Rosetta coordinates semantically annotated data sources into a metadata graph. That graph can be queried to generate programs to perform complex data retrieval tasks.

## Installation

Install and start Neo4J 3.2.6.
```
$ <neo4j-install>/bin/neo4j start
```
Clone the repository.

```
$ git clone <repo>
$ cd repo/greent
$ pip install -r requirements.txt
```
Initialize the type graph. This imports the graph of Translator services, overlays local service configurations, and imports locally defined services. It configures all of these according to the biolink-model.
```
$ PYTHONPATH=$PWD/.. rosetta.py --delete-type-graph --initialize-type-graph --debug
```
Via the Neo4J interface at http://localhost:7474/browser/ query the entire type graph:

```
match (m)--(n) return *
```
Query a particular path:
```
MATCH (n:named_thing)-[a]->(d:disease)-[b]->(g:gene) RETURN *
```
In the returned graph, nodes are biolink-model concepts and edges contain attributes indicating the service to invoke. 

## Python API

This simple snippet demonstrates usage via the Python API:

```

from greent.graph_components import elements_to_json
from greent.rosetta import Rosetta
...
rosetta = Rosetta ()
...
knowledge = get_translator().clinical_outcome_pathway_app(drug=drug,
                                                              disease=disease)
result = [ elements_to_json (e) for e in knowledge ]
return jsonify (result)
```

## Web API

We will be publshing an OpenAPI interface to the graph. 

For now, run 
```
$ git clone https://github.com/NCATS-Tangerine/smartBag
$ cd robokop-interfaces/greent
$ PYTHONPATH=$PWD/..:$PWD/../.. python api/server.py
```
To start the server. Usage examples coming soon.

Caveat: The repo is undergoing substantial development so please expect delays.


