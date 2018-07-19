package com.redhat.mavenpop.DependencyParser

import java.io.{PrintWriter, StringWriter}

import com.redhat.mavenpop.MavenPopConfig
import com.redhat.mavenpop.test.TestHelpers

import scala.io.Source
import org.scalatest._

class NeoDataParserTest extends FlatSpec with Matchers with BeforeAndAfterEach {

  private var outDepString: StringWriter = _
  private var outGavString: StringWriter = _
  private var source: Source = _
  private var outGav: PrintWriter = _
  private var outDep: PrintWriter = _

  private val conf: MavenPopConfig = new MavenPopConfig()

  override def beforeEach(): Unit = {
    super.beforeEach()
    outGavString = new StringWriter()
    outGav = new PrintWriter(outGavString)
    outDepString = new StringWriter()
    outDep = new PrintWriter(outDepString)

  }

  override def afterEach(): Unit = {
    super.afterEach()
    outGavString.close()
    outGav.close()
    outDepString.close()
    outDep.close()
    source.close()
  }

  "parseDependencies" should "write nodes and relationships files" in {

    val sourceStr =
      """1 p mavenpop:test:top1 mavenpop:test:dep1,mavenpop:test:dep2,mavenpop:test:dep3
1 p mavenpop:test:top2 mavenpop:test:dep4,mavenpop:test:dep1,mavenpop:test:dep3
1 p mavenpop:test:top3 UNKNOWN_DEPS
1 p mavenpop:test:top4 NO_DEPS"""

    val expectedNodeStr =
      """mavenpop:test:top1
mavenpop:test:dep1
mavenpop:test:dep2
mavenpop:test:dep3
mavenpop:test:top2
mavenpop:test:dep4
mavenpop:test:top3
mavenpop:test:top4"""

    val expectedRelStr =
      """mavenpop:test:top1,mavenpop:test:dep1
mavenpop:test:top1,mavenpop:test:dep2
mavenpop:test:top1,mavenpop:test:dep3
mavenpop:test:top2,mavenpop:test:dep4
mavenpop:test:top2,mavenpop:test:dep1
mavenpop:test:top2,mavenpop:test:dep3"""

    assertParseDependencies(sourceStr, expectedNodeStr, expectedRelStr, false)
  }

  it should "handle duplicate gavs and dependencies" in {

    val sourceStr =
      """1 p mavenpop:test:top1 mavenpop:test:dep1,mavenpop:test:dep2,mavenpop:test:dep3
1 p mavenpop:test:top2 mavenpop:test:dep4,mavenpop:test:dep1,mavenpop:test:dep3
1 p mavenpop:test:top3 UNKNOWN_DEPS
1 p mavenpop:test:top4 NO_DEPS
1 p mavenpop:test:top1 mavenpop:test:dep6"""

    val expectedNodeStr =
      """mavenpop:test:top1
mavenpop:test:dep1
mavenpop:test:dep2
mavenpop:test:dep3
mavenpop:test:top2
mavenpop:test:dep4
mavenpop:test:dep6
mavenpop:test:top3
mavenpop:test:top4"""

    val expectedRelStr =
      """mavenpop:test:top1,mavenpop:test:dep1
mavenpop:test:top1,mavenpop:test:dep2
mavenpop:test:top1,mavenpop:test:dep3
mavenpop:test:top2,mavenpop:test:dep4
mavenpop:test:top2,mavenpop:test:dep1
mavenpop:test:top2,mavenpop:test:dep3
mavenpop:test:top1,mavenpop:test:dep6"""

    assertParseDependencies(sourceStr, expectedNodeStr, expectedRelStr, false)
  }

  it should "write transitive dependencies given writeTransitiveAsDirect is true" in {

    val sourceStr =
      """1 p mavenpop:test:top1 mavenpop:test:dep1,mavenpop:test:dep2
1 p mavenpop:test:dep1 mavenpop:test:dep11
1 p mavenpop:test:dep11 mavenpop:test:dep111"""

    val expectedNodeStr =
      """mavenpop:test:top1
mavenpop:test:dep1
mavenpop:test:dep2
mavenpop:test:dep11
mavenpop:test:dep111"""

    val expectedRelStr =
      """mavenpop:test:top1,mavenpop:test:dep1
mavenpop:test:top1,mavenpop:test:dep2
mavenpop:test:dep1,mavenpop:test:dep11
mavenpop:test:dep1,mavenpop:test:dep111
mavenpop:test:dep11,mavenpop:test:dep111
mavenpop:test:top1,mavenpop:test:dep11
mavenpop:test:top1,mavenpop:test:dep111"""

    assertParseDependencies(sourceStr, expectedNodeStr, expectedRelStr, true)
  }


  private def assertParseDependencies(sourceStr: String, expectedNodeStr: String, expectedRelStr: String,
                                      writeTransitiveAsDirect: Boolean) = {
    source = Source.fromString(
      sourceStr.stripMargin)

    val relLabel = if (writeTransitiveAsDirect) conf.transitiveDepLabel else conf.directDepLabel

    val expectedGavArr = NeoDataParser.HEADER_NODE_GAV +:
      expectedNodeStr.stripMargin.split("\n").map(_ + NeoDataParser.DELIMITER + conf.gavLabel)

    val expectedDepArr = NeoDataParser.HEADER_REL_DEP +:
      expectedRelStr.stripMargin.replaceAll(",", NeoDataParser.DELIMITER).
      split("\n").map(_ + NeoDataParser.DELIMITER + relLabel)

    val parser = new NeoDataParser()
    parser.parseDependencies(source, outGav, outDep, writeTransitiveAsDirect)

    val outGavArr = outGavString.toString.split("\n")
    val outDepArr = outDepString.toString.split("\n")

    assertArrayEqual(outGavArr, expectedGavArr)
    assertArrayEqual(outDepArr, expectedDepArr)
  }

  private def assertArrayEqual[T <: Comparable[T]](a1: Array[T], a2: Array[T]): Unit = {
    val sameElements = a1.sorted.sameElements(a2.sorted)

    if (!sameElements) print(TestHelpers.generateMismatchMessage(a1, a2))

    sameElements should be(true)
  }
}
