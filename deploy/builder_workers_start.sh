#!/bin/bash

cd $ROBOKOP_HOME/robokop-interfaces

echo "Starting worker..."
celery multi start \
    updater@robokop \
    -A builder.api.tasks.celery \
    -l info \
    -c:1 $BUILDER_NUM_WORKERS \
    -Q:1 update
    -O:1 fair
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

# sleep 3
# PID=`cat updater.pid`
# updater is not a child process of this shell, somehow, so `wait`-ing for it will not work

# while [ -e "updater.pid" ];
while :
do
   sleep 10 & # sleep in background
   wait $! # wait for last background process - can be interrupted by trap!
   echo "Everything is probably fine..."
done
