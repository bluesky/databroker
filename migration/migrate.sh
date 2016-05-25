#!/bin/bash

source activate migration

for i in "$@"
do
case $i in
    
    -m=*|--mongodir=*)
    MONGODIR="${i#*=}"
    ;;

    -t=*|--target=*)
    TARGET="${i#*=}"
    ;;
    
    -s=*|--source=*)
    SOURCE="${i#*=}"
    ;;
    *)
    ;;
esac
done
echo TARGET = ${TARGET}
echo SOURCE = ${SOURCE}
echo MONGODIR = ${MONGODIR}

mongorestore ${MONGODIR} --db ${SOURCE} --noIndexRestore

python v01migration.py --target ${TARGET} --source ${SOURCE}

python verify.py --old_db ${SOURCE} --new_db ${TARGET}
