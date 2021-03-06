#! /bin/bash

set -e
NARGS=$#

function validate_inputs(){
  if [ -z "$NEO4J_HOME" ]; then
    echo "NEO4J_HOME env variable not set"
    exit 1
  fi

  if [ $NARGS -ne 5 ]; then
    echo "usage: $0 INPUT_NODES INPUT_RELS DB_NAME BOLT_PORT TRANSACTION_TIMEOUT"
    exit 1
  fi
}

function overwrite_db(){

if [ -d $DB_DIR ]; then
  rm -r $DB_DIR
fi

"$NEO4J_HOME"/bin/neo4j-admin import --nodes $INPUT_NODES --ignore-duplicate-nodes \
--relationships ${INPUT_RELS} --delimiter '|' --database ${DB_NAME}

}

function configure_dbms(){
    cp "$NEO4J_HOME"/conf/neo4j.conf "$NEO4J_HOME"/conf/neo4j.conf.org
    sed -i -e "s/#\?dbms.active_database=.*\$/dbms.active_database=$DB_NAME/" "$NEO4J_HOME"/conf/neo4j.conf
    sed -i -e "s/#\?dbms.connector.bolt.enabled=.*\$/dbms.connector.bolt.enabled=true/" "$NEO4J_HOME"/conf/neo4j.conf
    sed -i -e "s/#\?dbms.connector.bolt.listen_address=.*\$/dbms.connector.bolt.listen_address=:$BOLT_PORT/" "$NEO4J_HOME"/conf/neo4j.conf
    sed -i -e '/^dbms.transaction.timeout=/d' "$NEO4J_HOME"/conf/neo4j.conf
    echo "dbms.transaction.timeout=$TX_TIMEOUT" >> "$NEO4J_HOME"/conf/neo4j.conf

    if [ ! -f "$NEO4J_HOME"/data/dbms/auth ]; then
        "$NEO4J_HOME"/bin/neo4j-admin set-initial-password ${SECRET}
    fi
}

function wait_neo_server_online(){
    local end="$((SECONDS+120))"
    while true; do
        nc -z -w 2 localhost ${BOLT_PORT} && break
        [[ "${SECONDS}" -ge "${end}" ]] && exit 1
        sleep 1
    done
}


function add_constraints_and_indexes(){
    "$NEO4J_HOME"/bin/cypher-shell \
        -a "bolt://localhost:$BOLT_PORT" \
        -u neo4j -p ${SECRET} \
        "CREATE CONSTRAINT ON (gav:GAV) ASSERT gav.id IS UNIQUE"
}

#### Main

validate_inputs

# Initialize variables

INPUT_NODES=$1
INPUT_RELS=$2
DB_NAME=$3
BOLT_PORT=$4
TX_TIMEOUT=$5
DB_DIR="$NEO4J_HOME"/data/databases/$DB_NAME/
SECRET="Neo03"


"$NEO4J_HOME"/bin/neo4j stop

echo "------ Overwriting DB"

overwrite_db

echo "------ Configuring DB"

configure_dbms


"$NEO4J_HOME"/bin/neo4j start

echo "------ Waiting Neo4j Server to be online"

wait_neo_server_online

echo "------ Adding indexes and constraints"

add_constraints_and_indexes