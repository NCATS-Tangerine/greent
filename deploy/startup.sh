#!/bin/bash
### every exit != 0 fails the script
set -e

cd $ROBOKOP_HOME/robokop-interfaces
mkdir -p $ROBOKOP_HOME/task_logs
chmod 777 $ROBOKOP_HOME/task_logs

sleep 3s

if [ -n "$(find . -user "murphy" -prune)" ]; then
    echo "Files are owned by murphy."
else
    echo "Changing file ownership..."
    chown -R murphy:murphy $ROBOKOP_HOME
fi

echo "Setting up environment..."
source ./deploy/setenv.sh

echo "Initializing type graph..."
./initialize_type_graph.sh

echo "Finding and removing stray pid files..."
find . -prune -name "*.pid" -exec rm -rf {} \;

cd - > /dev/null
echo "Running supervisord..."
exec "$@"