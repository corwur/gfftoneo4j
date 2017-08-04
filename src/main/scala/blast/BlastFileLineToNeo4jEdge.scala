package blast

import java.io.File

import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.factory.GraphDatabaseFactory

import scala.collection.parallel.ParSeq

object BlastFileLineToNeo4jEdge {

  def insertIntoNeo4j(distinguishingAttributeLeft: String, distinguishingAttributeRight: String, blastFileLines: ParSeq[BlastFileLine]): Unit = {
    val databasePath: File = new File("mijndb.db")
    val db = new GraphDatabaseFactory().newEmbeddedDatabase(databasePath)

    blastFileLines.foreach { blastFileLine =>
      insertIntoNeo4j(db, distinguishingAttributeLeft, distinguishingAttributeRight, blastFileLine)
    }

    db.shutdown()
  }

  def insertIntoNeo4j(db: GraphDatabaseService, distinguishingAttributeLeft: String, distinguishingAttributeRight: String, blastFileLine: BlastFileLine): String = {
    val query =
      s"""MATCH
         |  (geneLeft:gene { ${distinguishingAttributeLeft}: "${blastFileLine.geneLeft}" }),
         |  (geneRight:gene { ${distinguishingAttributeRight}: "${blastFileLine.geneRight}" })
         |CREATE
         |  (geneLeft)-[:blast { identityScore: "${blastFileLine.identityScore}" }]->(geneRight)""".stripMargin
    val result = db.execute(query)
    println(result.resultAsString())
    "Henk"
  }

}
