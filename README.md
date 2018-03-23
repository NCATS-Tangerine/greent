# GreenT

GreenT began as a library of interfaces to biomedical and environmental data services.

It is now host to Rosetta, the data access layer of the Gamma reasoner.

Rosetta coordinates semantically annotated data sources into a metadata graph. That graph can be queried to generate programs to perform complex data retrieval tasks.

## Installation

### Graph Database
Install and start Neo4J 3.2.6.
```
$ <neo4j-install>/bin/neo4j start
```
### Cache
Install and start Redis 4.0.8
```
~/app/redis-4.0.8/src/redis-server
```
### App
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
knowledge_graph = rosetta.construct_knowledge_graph(**{
         "inputs" : {
            "disease" : [
               "DOID:2841"
            ]
         },            
         "query" :
         """MATCH (a:disease),(b:gene), p = allShortestPaths((a)-[*]->(b))
         WHERE NONE (r IN relationships(p) WHERE type(r) = 'UNKNOWN' OR r.op is null) 
         RETURN p"""
      })
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

## Caching

We cache in Redis. Objects are serialized using Python's pickle scheme. 

### List cache contents
To find out what operations(id) combinations are cached:
```
$ ~/app/redis-4.0.8/src/redis-cli --raw keys '*ctd*'
```
### Delete specific keys
To delete specific keys or patterns of keys from the cache:
```
$ ~/app/redis-4.0.8/src/redis-cli --raw keys '*' | xargs ~/app/redis-4.0.8/src/redis-cli --raw del
```

