DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export ROBOKOP_HOME="$DIR/../.."
if [ "$DEPLOY" != "docker" ]; then
    export $(cat $ROBOKOP_HOME/shared/robokop.env | grep -v ^# | xargs)
fi

export PYTHONPATH=$ROBOKOP_HOME/robokop-interfaces
export CELERY_BROKER_URL="redis://$REDIS_HOST:$REDIS_PORT/$BUILDER_REDIS_DB"
export CELERY_RESULT_BACKEND="redis://$REDIS_HOST:$REDIS_PORT/$BUILDER_REDIS_DB"
export FLOWER_BROKER_API="redis://$REDIS_HOST:$REDIS_PORT/$BUILDER_REDIS_DB"
export FLOWER_PORT="$BUILDER_FLOWER_PORT"
export FLOWER_BASIC_AUTH=${FLOWER_USER}:${FLOWER_PASSWORD}
export SUPERVISOR_PORT=$BUILDER_SUPERVISOR_PORT

# for greent conf
export TRANSLATOR_SERVICES_ROSETTAGRAPH_URL="bolt://$NEO4J_HOST:$NEO4J_BOLT_PORT"
export REDIS_DB="$GREENT_REDIS_DB"