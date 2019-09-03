DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export ROBOKOP_HOME="$DIR/../.."
if [ "$DEPLOY" != "docker" ]; then
    export $(cat $ROBOKOP_HOME/shared/robokop.env | grep -v ^# | xargs)
fi

export PYTHONPATH=$ROBOKOP_HOME/robokop-interfaces
export BROKER_API="http://admin:$ADMIN_PASSWORD@$BROKER_HOST:$BROKER_MONITOR_PORT/api/"
export SUPERVISOR_PORT=$BUILDER_SUPERVISOR_PORT

# for greent conf
export TRANSLATOR_SERVICES_ROSETTAGRAPH_URL="bolt://$NEO4J_HOST:$NEO4J_BOLT_PORT"
export TRANSLATOR_SERVICES_ROSETTAGRAPH_NEO4J_PASSWORD="$NEO4J_PASSWORD"