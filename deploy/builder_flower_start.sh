#!/bin/bash

cd $ROBOKOP_HOME/robokop-interfaces

exec flower \
    -A builder.api.tasks.celery \
    --broker=$CELERY_BROKER_URL \
    --broker_api=$CELERY_BROKER_URL