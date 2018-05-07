#!/bin/bash
### every exit != 0 fails the script
set -e

cd $ROBOKOP_HOME/robokop-interfaces
source ./deploy/setenv.sh

# set up Neo4j type graph
./initialize_type_graph.sh

cd - > /dev/null # squash output
exec "$@"