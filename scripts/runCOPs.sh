#!/bin/bash

FILES=$(ls $1 | grep .*\.json$)
for FILE in $FILES
do
    echo $FILE
    curl -X POST -u $ADMIN_EMAIL:"$ADMIN_PASSWORD" http://robokop.renci.org/api/questions/ -d "@$1/$FILE" -H "Content-Type: application/json"
done
