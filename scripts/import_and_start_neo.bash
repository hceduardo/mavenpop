#! /bin/bash

set -e
NARGS=$#

function validate_inputs(){
  if [ -z "$NEO4J_HOME" ]; then
    echo "NEO4J_HOME env variable not set"
    exit 1
  fi

  if [ $NARGS -ne 3 ]; then
    echo "usage: $0 INPUT_NODES INPUT_RELS DB_NAME"
    exit 1
  fi
}

validate_inputs

INPUT_NODES=$1
INPUT_RELS=$2
DB_NAME=$3
DB_DIR="$NEO4J_HOME"/data/databases/$DB_NAME/

"$NEO4J_HOME"/bin/neo4j stop

if [ -d $DB_DIR ]; then
  rm -r $DB_DIR
fi

echo "------"

"$NEO4J_HOME"/bin/neo4j-admin import --nodes $INPUT_NODES --ignore-duplicate-nodes \
--relationships $INPUT_RELS --delimiter '|' --database $DB_NAME

echo "------"

cp "$NEO4J_HOME"/conf/neo4j.conf "$NEO4J_HOME"/conf/neo4j.conf.org
sed -i -e "s/#\?dbms.active_database=.*\$/dbms.active_database=$DB_NAME/" "$NEO4J_HOME"/conf/neo4j.conf

echo "------"

"$NEO4J_HOME"/bin/neo4j start

rm $INPUT_NODES $INPUT_RELS
