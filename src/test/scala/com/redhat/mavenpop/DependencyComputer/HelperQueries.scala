package com.redhat.mavenpop.DependencyComputer

import com.redhat.mavenpop.MavenPopConfig

object HelperQueries {

  val conf: MavenPopConfig = new MavenPopConfig()

  val LoadNodes: String =
    """LOAD CSV FROM $fileURL AS row FIELDTERMINATOR ' ' """ +
      s"""WITH SPLIT(row[1], ',') as dependencies, row[0] as gav_id
MERGE (:${conf.gavLabel} { id:gav_id})
FOREACH (d IN dependencies | MERGE (:${conf.gavLabel} {id:d}))"""

  val LoadRelationships: String =
    """LOAD CSV FROM $fileURL AS row FIELDTERMINATOR ' ' """ +
      s"""WITH SPLIT(row[1], ',') as dependencies, row[0] as gav_id
UNWIND dependencies as dep_id
MATCH (main_gav:${conf.gavLabel} {id:gav_id}), (dep_gav:${conf.gavLabel} {id:dep_id})
MERGE (main_gav)-[:${conf.directDepLabel}]->(dep_gav)"""

  val CountNodes: String = "MATCH (n) RETURN count(n) AS count"

  val CountRelationships: String = "MATCH (n)-[r]->() RETURN COUNT(r) AS count"
}
