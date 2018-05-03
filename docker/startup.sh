#!/bin/bash
### every exit != 0 fails the script
set -e

cd $ROBOKOP_HOME/robokop-interfaces

export TRANSLATOR_SERVICES_ROSETTAGRAPH_URL="bolt://$NEO4J_HOST:$NEO4J_BOLT_PORT"
export CELERY_BROKER_URL="redis://$REDIS_HOST:$REDIS_PORT/$BUILDER_REDIS_DB"
export CELERY_RESULT_BACKEND="redis://$REDIS_HOST:$REDIS_PORT/$BUILDER_REDIS_DB"
export FLOWER_BROKER_API="redis://$REDIS_HOST:$REDIS_PORT/$BUILDER_REDIS_DB"
export FLOWER_PORT="$BUILDER_FLOWER_PORT"
export REDIS_DB="$GREENT_REDIS_DB"

# set up Neo4j type graph
./initialize_type_graph.sh

cd -
exec "$@"