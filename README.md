# Rosetta

Rosetta is the knowledge map and service invocation tier of the Gamma reasoner.

Rosetta coordinates semantically annotated data sources into a metadata graph. That graph can be queried to generate programs to perform complex data retrieval tasks. It looks like this:
![rosetta](https://github.com/NCATS-Gamma/robokop-interfaces/blob/master/rosetta.png)

Blue nodes are semantic types from the [biolink-model](https://biolink.github.io/biolink-model/)

## Installation

### Prerequisites
[Install Docker](https://www.docker.com/get-started) if not installed on your computer. 

Make a ``<workspace>`` directory. 

```
$ mkdir <workspace>
$ cd <workspace> 
```
We will setup our enviroment using [these](https://github.com/NCATS-Gamma/robokop#environment-settings) enviroment settings. Copy and save them to ``<workspace>/shared/robokop.env``.

If you would like to run the neo4j and redis-cache instance on the same host as the app you can use these values. If you choose to have Neo4j and Redis hosted on a different host you would change these values.
```
# neo4j host name  
NEO4J_HOST=neo4j
# cache host name
CACHE_HOST=request_cache
```
 

If you wish to test robokopkg instance on a smaller computer, you can modify the following values to fit your hardware, but the default values are the ones used on [RobokopKG](http://robokopkg.renci.org).

```
NEO4J_HEAP_MEMORY
NEO4J_HEAP_MEMORY_INIT
NEO4J_CACHE_MEMORY
```

And finally, set the password variables found on the bottom section of the file. 

Run the following to make sure that your terminal is set up with the enviroment variable before running docker commands.

```
$ set -a 
$ source <workspace>/shared/robokop.env
```

From the ``<workspace>`` directory, clone the repository.
```
$ git clone https://github.com/NCATS-Gamma/robokop-interfaces.git
$ cd robokop-interfaces
```

### Graph Database
The graph and concept map will be stored in a Neo4j server instance. Start the Neo4j instance with:

```
[robokop-interfaces/] $ docker-compose  -f deploy/graph/docker-compose.yml up -d
```

Optionally you can load our latest build of the knowledge graph available at [RobokopKG](http://robokopkg.renci.org). Once you download a version of dump file best suited, run the following commands:

```
[robokop-interfaces/] $ cp <dump_file> <workspace>/neo4j_data/
[robokop-interfaces/] $ cd deploy/graph
[robokop-interfaces/deploy/graph] $ ../robokopkg/scripts/reload.sh -f <dump_file_name_only> -c ../robokopkg/scripts/docker-compose-backup.yml
[robokop-interfaces/deploy/graph] $ cd <workspace>/robokop-interfaces/ 
```

### Cache
Start the Redis container.
```
[robokop-interfaces/] $ docker-compose -f deploy/cache/docker-compose.yml up -d 
```

### App

Now that the backend for the App is up we can start the app containers.

##### Building the container
We need to build the container with the current user and group permissions so that log file ownership and the code directory does not get elevated. 

```
[robokop-interfaces/] $ cd deploy
[robokop-interfaces/deploy] $ docker build --build-arg UID=$(id -u) --build-arg GID=$(id -g) -t robokop_interfaces .
```

##### Composing up

```
[robokop-interfaces/deploy] $ docker-compose up -d 
```


#### Initial Database setup

If you have not imported database dump into your neo4j instance, you will need to run the following command to initialize the type graph. This imports the graph of Translator services, overlays local service configurations, and imports locally defined services. It configures all of these according to the biolink-model.

```
$ docker exec $(docker ps -f name=interfaces -q) bash -c "source robokop-interfaces/deploy/setenv.sh && robokop-interfaces/initialize_type_graph.sh"
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
$  docker exec $(docker ps -f name=request_cache -q) redis-cli -p $CACHE_PORT -a $CACHE_PASSWORD --raw keys '*' 
```
### Delete specific keys
To delete specific keys or patterns of keys from the cache:
```
$  docker exec $(docker ps -f name=request_cache -q) redis-cli -p $CACHE_PORT -a $CACHE_PASSWORD --raw keys '*' | \
xargs docker exec $(docker ps -f name=request_cache -q) redis-cli -p $CACHE_PORT -a $CACHE_PASSWORD --raw del
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
where ``objectName`` is a member of core.py, the central service manager.
         
1. Find the ``@operators`` tag in the configuration file.
2. Find the [biolink-model element](https://github.com/NCATS-Gamma/robokop-interfaces/blob/master/greent/conf/biolink-model.yaml) for the source type to your service.
3. Follow the pattern in the configuration to enter your predicate (link) and operator (op)

#### Rebuild the Knowledge Map
```
$ PYTHONPATH=$PWD/.. rosetta.py --delete-type-graph --initialize-type-graph --debug
```

You should now be able to write cypher queries for Rosetta that use the biolink-model names specified in the rosetta.yml config file that are connected by your new service.
