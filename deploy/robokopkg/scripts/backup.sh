#!/bin/sh

docker kill $(docker ps -f name=neo4j -q) 

docker-compose -f docker-compose-backup.yml up -d

# ------------- back up process start 

export backup_time=$(date +"%m%d%y-%I-%M-%S")

# 
docker exec $(docker ps -f name=neo4j -q) bash bin/neo4j-admin dump --to data/graph.db.$backup_time.dump

docker exec $(docker ps -f name=neo4j -q)  ln -s /data/graph.db.$backup_time.dump data/graph.db.latest.dump

# kill back-upper container
docker kill $(docker ps -f name=neo4j -q)
# ------------------ back up complete
# compose main file
docker-compose up -d 

