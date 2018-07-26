# Rosetta

Rosetta is the knowledge map and service invocation tier of the Gamma reasoner.

Rosetta coordinates semantically annotated data sources into a metadata graph. That graph can be queried to generate programs to perform complex data retrieval tasks. It looks like this:
![rosetta](https://github.com/NCATS-Gamma/robokop-interfaces/blob/master/rosetta.png)

Blue nodes are semantic types from the [biolink-model](https://biolink.github.io/biolink-model/)

## Installation

### Graph Database
[Download](https://neo4j.com/download/), install, and start Neo4J 3.2.6.
```
$ <neo4j-install-dir>/bin/neo4j start
```
### Cache
[Download](http://download.redis.io/releases/redis-4.0.8.tar.gz), install, and start Redis 4.0.8
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

## Web API

The web API presents two endpoints:

### Clinical Outcome Pathway: /cop
Given a drug name and a disease name, it returns a knowledge graph of the clinical outcome pathway.
#### Edge:
Each edge includes:
  * **subj** : A subject
  * **pred** : A predicate indicating the relation of the subject to the object.
  * **obj**  : An object of the relation.
  * **pmids** : One or more PubMed identifiers relevant to the statement.
#### Node:
Each node includes:
  * **id** : A numeric identifier used as a link to edges in the same graph.
  * **identifier** : A curie identifying an instance in an ontology.
  * **type** : A biolink-model type for the object.
  
### Query: /query
Given inputs and a Cypher query representing a shortest path between two concepts, generate a graph of items. More complex graphs can be composed by iteratively invoking this endpoint.
  * **inputs** : A key value pair where the key is a biolink-model concept and the value is a comma separated list of curies. eg, concept=curie:id[,curie:id]
  * **query** : A cypher query returning a path. 

## Python API

This simple snippet demonstrates usage via the Python API:

```

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
1. Reuse or develop a smartAPI interface to your data. 
2. Publish a public network endpoint to the API if none exists.
3. Register your smartAPI at the [Translator Registry](https://github.com/NCATS-Tangerine/translator-api-registry).
4. For now, build a Python stub to your service. Soon, we hope to derive this information from the registry to invoke  services programmatically. For an example stub, see the [CTD service](https://github.com/NCATS-Gamma/robokop-interfaces/blob/master/greent/services/ctd.py). This is a stub for this [smartAPI endpoint](https://ctdapi.renci.org/apidocs/#/default).

#### Configure Service Endpoint
1. Add your service endpoint URL to the configuration files following the CTD pattern.
   * Add to [greent.conf](https://github.com/NCATS-Gamma/robokop-interfaces/blob/master/greent/greent.conf) used for local development.
   * And [greent-dev.conf](https://github.com/NCATS-Gamma/robokop-interfaces/blob/master/greent/greent-dev.conf) used in the continuous integration environment.

#### Instantiate The Service
Instantiate your service, following the lazy loading pattern, in [core.py](https://github.com/NCATS-Gamma/robokop-interfaces/blob/master/greent/core.py)


#### Edit the Configuration
This [YAML file](https://github.com/NCATS-Gamma/robokop-interfaces/blob/master/greent/rosetta.yml) links types in the biolink-model. Each link includes a predicate and the name of an operation.
Operations are named:
```
<objectName>.<methodName>
```
where <objectName> is a member of core.py, the central service manager.
         
1. Find the "@operators" tag in the configuration file.
2. Find the [biolink-model element](https://github.com/NCATS-Gamma/robokop-interfaces/blob/master/greent/conf/biolink-model.yaml) for the source type to your service.
3. Follow the pattern in the configuration to enter your predicate (link) and operator (op)

#### Rebuild the Knowledge Map
```
$ PYTHONPATH=$PWD/.. rosetta.py --delete-type-graph --initialize-type-graph --debug
```

You should now be able to write cypher queries for Rosetta that use the biolink-model names specified in the rosetta.yml config file that are connected by your new service.
