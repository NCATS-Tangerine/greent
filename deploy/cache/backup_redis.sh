#!/bin/sh

export $(cat ../../../shared/robokop.env | grep -v ^# | xargs)

docker exec $(docker ps -f name=cache -q) redis-cli -p $CACHE_PORT -a $CACHE_PASSWORD save

export backup_time=$(date +"%m%d%y-%I-%M-%S")

docker exec $(docker ps -f name=cache -q) mv dump.rdb dump.$backup_time.rdb

docker exec $(docker ps -f name=cache -q) rm dump.latest.rdb
docker exec $(docker ps -f name=cache -q) ln -s dump.$backup_time.rdb dump.latest.rdb