package com.redhat.mavenpop.UsageAnalyser

object CypherTestQueries {

  val LoadNodes: String =
"""LOAD CSV FROM $fileURL AS row FIELDTERMINATOR ' '
WITH SPLIT(row[1], ',') as dependencies, row[0] as gav_id
MERGE (:GAV { id:gav_id})
FOREACH (d IN dependencies | MERGE (:GAV {id:d}))"""

  val LoadRelationships: String =
"""LOAD CSV FROM $fileURL AS row FIELDTERMINATOR ' '
WITH SPLIT(row[1], ',') as dependencies, row[0] as gav_id
UNWIND dependencies as dep_id
MATCH (main_gav:GAV {id:gav_id}), (dep_gav:GAV {id:dep_id})
MERGE (main_gav)-[:DEPENDS_ON]->(dep_gav)"""

  val CountNodes: String = "MATCH (n) RETURN count(n) AS count"

  val CountRelationships: String = "MATCH (n)-[r]->() RETURN COUNT(r) AS count"
}
