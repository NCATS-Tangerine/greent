#!/bin/sh

function printHelp(){
    echo "
        A simple script to take off a running neo4j container and reload a dump file into it
        then bring it back up. File argument is optional. graph.latest.db.dump will be loaded if 
        -f is not provided.
        Arguments:
            -f    file          file name to reload eg `-f graph.latest.db.dump` .
            
            -h    help          display this message.

    "
}

# Default to latest if args are not provided
backup_file='graph.db.latest.dump'

while getopts :hf: opt; do
    case $opt in 
        h) 
        printHelp
        exit
        ;;
        f) 
        backup_file=$OPTARG
        ;;
        \?) 
        echo "Invalid option -$OPTARG" 
        printHelp
        exit 1
        ;;
    esac
done

docker kill $(docker ps -f name=neo4j -q)

docker-compose -f scripts/docker-compose-backup.yml up -d

docker exec $(docker ps -f name=neo4j -q) bash bin/neo4j-admin load --from /data/$backup_file --force true

docker kill $(docker ps -f name=neo4j -q)

docker-compose up -d


