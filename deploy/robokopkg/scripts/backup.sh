#!/bin/bash

compose_file="scripts/docker-compose-backup.yml"
graph_compose_file="docker-compose.yml"

function printHelp(){
    echo "
        This script will take the back up of neo4j database. 
        Usage:
            ./backup.sh -c ./docker-compose-backup.yml 
        Arguments:
            -c   backup-compose-file       docker compose file.
            -f   graph-compose file        docker compose file.
            -h   help                      display this message.

    "
}


while getopts :hc:f: opt; do
    case $opt in 
        h) 
        printHelp
        exit
        ;;
        c) 
        compose_file=$OPTARG
        ;;
        f)
        graph_compose_file=$OPTARG
        ;;
        \?) 
        echo "Invalid option -$OPTARG" 
        printHelp
        exit 1
        ;;
    esac
done

docker kill $(docker ps -f name=neo4j -q) 

docker-compose -f $compose_file up -d

# ------------- back up process start 


# when killing containers New neo4j complains 'Active Logical log detected', 
# and needs a clean shutdown :/

docker exec $(docker ps -f name=neo4j -q) bash -c "bin/neo4j start; bin/neo4j stop"


# 
docker exec $(docker ps -f name=neo4j -q) bash bin/neo4j-admin dump --to data/graph.db.dump


# kill back-upper container
docker kill $(docker ps -f name=neo4j -q)
# ------------------ back up complete
# compose main file
docker-compose -f $graph_compose_file up -d
