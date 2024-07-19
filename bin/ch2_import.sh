#!/bin/sh
#
SCRIPT_PATH=$(dirname "$0")
SCRIPT_ROOT=$(cd "$SCRIPT_PATH/.." && pwd)
CLASSPATH="${SCRIPT_ROOT}/conf:${SCRIPT_ROOT}/lib/*"
EXTRA_ARG=""

while getopts "j" opt
do
  case $opt in
    j)
      EXTRA_ARG="-j"
      ;;
    \?)
      ;;
    esac
done

java -cp "$CLASSPATH" site.ycsb.db.couchbase3.ColumnarS3Load -p conf/db.properties $EXTRA_ARG
