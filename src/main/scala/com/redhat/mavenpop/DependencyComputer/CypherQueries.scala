package com.redhat.mavenpop.DependencyComputer

object CypherQueries {

  def GetDependenciesFromList = _GetDependenciesFromListv2

  private val _GetDependenciesFromListv1: String =
    """WITH $gavList as gavIds
MATCH p = (topLevel)-[*1..]->(dependency)
WHERE topLevel.id in gavIds AND dependency.id in gavIds AND
ANY (gavId in gavIds WHERE (topLevel:GAV)-[:DEPENDS_ON*1..]->(dependency:GAV{id:gavId}))
RETURN DISTINCT dependency.id AS dependencyId"""

  private val _GetDependenciesFromListv2: String =
    """WITH $gavList as gavIds
MATCH p = (topLevel:GAV)-[:DEPENDS_ON*1..]->(dependency:GAV)
WHERE topLevel.id in gavIds AND dependency.id in gavIds AND
ANY (gavId in gavIds WHERE (topLevel:GAV)-[:DEPENDS_ON*1..]->(dependency:GAV{id:gavId}))
RETURN DISTINCT dependency.id AS dependencyId"""

}
