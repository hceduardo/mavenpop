#! /usr/bin/bash

set -e
NARGS=$#

function validate_inputs(){
  if [ -z "$NEO4J_HOME" ]; then
    echo "NEO4J_HOME env variable not set"
    exit 1
  fi

  if [ ! -f parser.jar ]; then
    echo "parser.jar file not found. compile neoDataParser.scala here"
    exit 1
  fi

  if [ $NARGS -ne 2 ]; then
    echo "usage: $0 INPUT_FILE DB_NAME"
    exit 1
  fi
}

validate_inputs

INPUT_FILE=$1
DB_NAME=$2
DB_DIR="$NEO4J_HOME"/data/databases/$DB_NAME/
TMP_GAV="pgav.csv"
TMP_DEP="pdep.csv"

scala parser.jar $INPUT_FILE $TMP_GAV $TMP_DEP

"$NEO4J_HOME"/bin/neo4j stop

if [ -d $DB_DIR ]; then
  rm -r $DB_DIR
fi

echo "------"

"$NEO4J_HOME"/bin/neo4j-admin import --nodes $TMP_GAV --ignore-duplicate-nodes \
--relationships $TMP_DEP --database $DB_NAME

echo "------"

cp "$NEO4J_HOME"/conf/neo4j.conf "$NEO4J_HOME"/conf/neo4j.conf.org
sed -i -e "s/dbms.active_database=.*\$/dbms.active_database=$DB_NAME/" "$NEO4J_HOME"/conf/neo4j.conf

echo "------"

"$NEO4J_HOME"/bin/neo4j start

rm $TMP_GAV $TMP_DEP
