# Rosetta

Rosetta is the data access layer of the Gamma reasoner.

Rosetta coordinates semantically annotated data sources into a metadata graph. That graph can be queried to generate programs to perform complex data retrieval tasks. It looks like this:
![rosetta](https://github.com/NCATS-Gamma/robokop-interfaces/blob/master/rosetta.png)

Blue nodes are semantic types from the [biolink-model](https://biolink.github.io/biolink-model/)

## Installation

### Graph Database
Install and start Neo4J 3.2.6.
```
$ <neo4j-install-dir>/bin/neo4j start
```
### Cache
Install and start Redis 4.0.8
```
<redis-install-dir>/src/redis-server
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

rosetta = Rosetta ()
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

## Adding Edges

To add a data source to the knowledge map:

#### Build a Service
1. Build a smartAPI interface to your data. 
2. Publish a public network endpoint to your data.
3. Register your [smartAPI](https://github.com/NCATS-Tangerine/translator-api-registry) at the Translator Registry.
4. For the immediate time frame, you'll also need to build a small Python shim to your service. In the future, we'll derive  information from the registry to invoke your service programmatically. For an example, see the [CTD service](https://github.com/NCATS-Gamma/robokop-interfaces/blob/master/greent/services/ctd.py). This is a stub for this [smartAPI endpoint](https://ctdapi.renci.org/apidocs/#/default).

#### Configure Service Endpoint
1. Add your service endpoint URL to the configuration files following the CTD pattern.
   * Add to [greent.conf](https://github.com/NCATS-Gamma/robokop-interfaces/blob/master/greent/greent.conf) used for local development.
   * And [greent-dev.conf](https://github.com/NCATS-Gamma/robokop-interfaces/blob/master/greent/greent-dev.conf) used in the continuous integration environment.

#### Instantiate The Service
Instantiate your service, following the lazy loading pattern, in [core.py](https://github.com/NCATS-Gamma/robokop-interfaces/blob/master/greent/core.py)


#### Edit the Configuration
This YAML file links types in the biolink-model. Each link includes a predicate and the name of an operation.
Operations are named:
```
<objectName>.<methodName>
```
where <objectName> is a member of core.py, the central service manager.
         
1. Find the "@operators" tag in the configuration file.
2. Find the biolink-model element for the source type to your service.
3. Follow the pattern in the configuration to enter your predicate (link) and operator (op)

#### Rebuild the Knowledge Map
```
$ PYTHONPATH=$PWD/.. rosetta.py --delete-type-graph --initialize-type-graph --debug
```
