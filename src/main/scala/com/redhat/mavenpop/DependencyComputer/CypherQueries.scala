package com.redhat.mavenpop.DependencyComputer

object CypherQueries {

  val GetDependenciesFromList :String =
"""WITH $gavList as gavIds
MATCH p = (topLevel)-[*1..]->(dependency)
WHERE topLevel.id in gavIds AND dependency.id in gavIds AND
ANY (gavId in gavIds WHERE (topLevel:GAV)-[:DEPENDS_ON*1..]->(dependency:GAV{id:gavId}))
RETURN DISTINCT dependency.id AS dependencyId"""

}
