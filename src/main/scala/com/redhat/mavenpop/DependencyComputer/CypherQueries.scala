package com.redhat.mavenpop.DependencyComputer

import com.redhat.mavenpop.MavenPopConfig

object CypherQueries {

  private val conf: MavenPopConfig = new MavenPopConfig()
  private val (gavLabel, directDepLabel, transitiveDepLabel) =
    (conf.gavLabel, conf.directDepLabel, conf.transitiveDepLabel)

  def GetDependenciesFromListV3(depth: Int): String = {
    assert(depth >= 0)

    depth match {
      case 0 => {
        "WITH $gavList as gavIds " + s"""MATCH p = (topLevel:$gavLabel)-[:$directDepLabel*1..]->(dependency:$gavLabel)
                                        |WHERE topLevel.id in gavIds AND dependency.id in gavIds AND
                                        |ANY (gavId in gavIds WHERE (topLevel:$gavLabel)-[:$directDepLabel*1..]->(dependency:$gavLabel{id:gavId}))
                                        |RETURN DISTINCT dependency.id AS dependencyId""".stripMargin
      }
      case 1 => {
        "WITH $gavList as gavIds " + s"""MATCH (topLevel:$gavLabel)-[:$transitiveDepLabel]->(dependency:$gavLabel)
                                        |WHERE topLevel.id in gavIds AND dependency.id in gavIds
                                        |RETURN DISTINCT dependency.id AS dependencyId""".stripMargin
      }
      case _ => {
        "WITH $gavList as gavIds " + s"""MATCH (dependency:$gavLabel)
                                        |WHERE dependency.id in gavIds AND
                                        |ANY (gavId in gavIds WHERE (:$gavLabel{id:gavId})-[:$directDepLabel*1..$depth]->(dependency))
                                        |RETURN DISTINCT dependency.id AS dependencyId""".stripMargin
      }
    }
  }

  def GetDependenciesFromListV2(depth: Int): String = {
    assert(depth >= 0)

    depth match {
      case 0 => {
        "WITH $gavList as gavIds " + s"""MATCH p = (topLevel)-[*1..]->(dependency)
          |WHERE topLevel.id in gavIds AND dependency.id in gavIds AND
          |ANY (gavId in gavIds WHERE (topLevel:GAV)-[:D_DEPENDS_ON*1..]->(dependency:GAV{id:gavId}))
          |RETURN DISTINCT dependency.id AS dependencyId""".stripMargin
      }
      case 1 => {
        "WITH $gavList as gavIds " + s"""MATCH (dependency:$gavLabel)
          |WHERE dependency.id in gavIds AND
          |ANY(gavId in gavIds WHERE (:$gavLabel{id:gavId})-[:$transitiveDepLabel]->(dependency))
          |RETURN DISTINCT dependency.id AS dependencyId""".stripMargin
      }
      case _ => {
        "WITH $gavList as gavIds " + s"""MATCH (dependency:$gavLabel)
          |WHERE dependency.id in gavIds AND
          |ANY (gavId in gavIds WHERE (:$gavLabel{id:gavId})-[:$directDepLabel*1..$depth]->(dependency))
          |RETURN DISTINCT dependency.id AS dependencyId
        """.stripMargin
      }
    }
  }

  def GetDependenciesFromList(depth: Int): String = {
    assert(depth >= 0)

    depth match {
      case 0 => {
        """WITH $gavList as gavIds
           |MATCH p = (topLevel:GAV)-[:D_DEPENDS_ON*1..]->(dependency:GAV)
           |WHERE topLevel.id in gavIds AND dependency.id in gavIds AND
           |ANY (gavId in gavIds WHERE (topLevel:GAV)-[:D_DEPENDS_ON*1..]->(dependency:GAV{id:gavId}))
           |RETURN DISTINCT dependency.id AS dependencyId""".stripMargin
      }
      case 1 => {
        """WITH $gavList as gavIds
           |MATCH p = (topLevel:GAV)-[:DEPENDS_ON]->(dependency:GAV)
           |WHERE topLevel.id in gavIds AND dependency.id in gavIds
           |RETURN DISTINCT dependency.id AS dependencyId""".stripMargin
      }
      case _ => {
        "WITH $gavList as gavIds " + s"""MATCH
          | p = (topLevel:GAV)-[:DEPENDS_ON*1..$depth]->(dependency:GAV)
          |WHERE topLevel.id in gavIds AND dependency.id in gavIds AND
          |ANY (gavId in gavIds WHERE (topLevel:GAV)-[:DEPENDS_ON*1..$depth]->(dependency:GAV{id:gavId}))
          |RETURN DISTINCT dependency.id AS dependencyId""".stripMargin
      }
    }
  }

  def GetDependenciesFromList: String = {

    GetDependenciesFromListV2(40)
    //    _conf.profilerVersion match {
    //      case 1 => _getDependenciesFromList_v1
    //      case 2 => _getDependenciesFromList_v2
    //      case 3 => _getDependenciesFromList_v3
    //      case 4 => _getDependenciesFromList_v4
    //      case 5 => _getDependenciesFromList_v5
    //      case 20 => _getDependenciesFromList_v20
    //    }
  }

  def GetTraversalWork: String = _getTraversalWork_v2

  private val _getDependenciesFromList_v1: String =
    """WITH $gavList as gavIds
MATCH p = (topLevel)-[*1..]->(dependency)
WHERE topLevel.id in gavIds AND dependency.id in gavIds AND
ANY (gavId in gavIds WHERE (topLevel:GAV)-[:DEPENDS_ON*1..]->(dependency:GAV{id:gavId}))
RETURN DISTINCT dependency.id AS dependencyId"""

  private val _getDependenciesFromList_v2: String =
    """WITH $gavList as gavIds
MATCH p = (topLevel:GAV)-[:DEPENDS_ON*1..]->(dependency:GAV)
WHERE topLevel.id in gavIds AND dependency.id in gavIds AND
ANY (gavId in gavIds WHERE (topLevel:GAV)-[:DEPENDS_ON*1..]->(dependency:GAV{id:gavId}))
RETURN DISTINCT dependency.id AS dependencyId"""

  private val _getDependenciesFromList_v3: String =
    """WITH $gavList as gavIds
MATCH p = (topLevel:GAV)-[:DEPENDS_ON*1..1000]->(dependency:GAV)
WHERE topLevel.id in gavIds AND dependency.id in gavIds AND
ANY (gavId in gavIds WHERE (topLevel:GAV)-[:DEPENDS_ON*1..]->(dependency:GAV{id:gavId}))
RETURN DISTINCT dependency.id AS dependencyId"""

  private val _getDependenciesFromList_v4: String =
    """WITH $gavList as gavIds
MATCH p = (topLevel:GAV)-[:DEPENDS_ON*1..15]->(dependency:GAV)
WHERE topLevel.id in gavIds AND dependency.id in gavIds AND
ANY (gavId in gavIds WHERE (topLevel:GAV)-[:DEPENDS_ON*1..]->(dependency:GAV{id:gavId}))
RETURN DISTINCT dependency.id AS dependencyId"""

  private val _getDependenciesFromList_v5: String =
    """WITH $gavList as gavIds
MATCH p = (topLevel:GAV)-[:DEPENDS_ON*1..5]->(dependency:GAV)
WHERE topLevel.id in gavIds AND dependency.id in gavIds AND
ANY (gavId in gavIds WHERE (topLevel:GAV)-[:DEPENDS_ON*1..]->(dependency:GAV{id:gavId}))
RETURN DISTINCT dependency.id AS dependencyId"""

  private val _getDependenciesFromList_v20: String =
    """WITH $gavList as gavIds
MATCH p = (topLevel:GAV)-[:DEPENDS_ON]->(dependency:GAV)
WHERE topLevel.id in gavIds AND dependency.id in gavIds
RETURN DISTINCT dependency.id AS dependencyId"""

  private val _getTraversalWork_v1: String =
    """WITH $gavList as gavIds
MATCH p = (topLevel:GAV)-[:DEPENDS_ON*1..]->(dependency:GAV)
WHERE topLevel.id in gavIds AND dependency.id in gavIds
RETURN sum(length(p))"""

  private val _getTraversalWork_v2: String =
    """WITH $gavList as gavIds
MATCH p = (topLevel:GAV)-[:DEPENDS_ON*1..]->(dependency:GAV)
WHERE topLevel.id in gavIds
RETURN sum(length(p))"""

}
