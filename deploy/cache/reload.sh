

function printHelp(){
    echo "
        Simple script to reload redis from a saved rdb file. 
        Arguments:
            -f    file          file name to reload eg `-f graph.latest.db.dump` .
            
            -h    help          display this message.

    "
}


backup_file='dump.latest.rdb'

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


docker exec $(docker ps -f name=cache -q) cp $backup_file dump.rdb

docker restart $(docker ps -f name=cache -q) 

docker exec $(docker ps -f name=cache -q) rm dump.rdb