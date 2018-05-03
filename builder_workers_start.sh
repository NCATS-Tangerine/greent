#!/bin/bash

cd $ROBOKOP_HOME/robokop-interfaces
export PYTHONPATH=$ROBOKOP_HOME/robokop-interfaces

export TRANSLATOR_SERVICES_ROSETTAGRAPH_URL="bolt://$NEO4J_HOST:$NEO4J_BOLT_PORT"
export CELERY_BROKER_URL="redis://$REDIS_HOST:$REDIS_PORT/$BUILDER_REDIS_DB"
export CELERY_RESULT_BACKEND="redis://$REDIS_HOST:$REDIS_PORT/$BUILDER_REDIS_DB"
export FLOWER_BROKER_API="redis://$REDIS_HOST:$REDIS_PORT/$BUILDER_REDIS_DB"
export FLOWER_PORT="$BUILDER_FLOWER_PORT"
export REDIS_DB="$GREENT_REDIS_DB"

if [[ -z CELERY_BROKER_URL ]]; then
    export CELERY_BROKER_URL="redis://$REDIS_HOST:$REDIS_PORT/$BUILDER_REDIS_DB"
fi
if [[ -z CELERY_RESULT_BACKEND ]]; then
    export CELERY_RESULT_BACKEND="redis://$REDIS_HOST:$REDIS_PORT/$BUILDER_REDIS_DB"
fi
echo "Starting worker..."
celery multi start \
    updater@robokop \
    -A builder.api.tasks.celery \
    -l info \
    -c:1 1 \
    -Q:1 update
# Equivalent to:
# celery \
#     -A builder.api.tasks.celery \
#     -l info \
#     -Q update \
#     -n updater@robokop \
#     worker
echo "Worker started."

function cleanup {
    echo "Stopping worker..."
    celery multi stop updater@robokop
    echo "Worker stopped."
}
trap cleanup EXIT

sleep 3
# PID=`cat updater.pid`
# updater is not a child process of this shell, somehow, so `wait`-ing for it will not work

# while [ -e "updater.pid" ];
while :
do
   sleep 10 & # sleep in background
   wait $! # wait for last background process - can be interrupted by trap!
   echo "Sleeping..."
done
