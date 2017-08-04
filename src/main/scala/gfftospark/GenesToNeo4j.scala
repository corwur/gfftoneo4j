package gfftospark

import java.io.File

import org.neo4j.driver.v1._
import org.neo4j.graphdb.{Node, RelationshipType}

import scala.collection.JavaConversions._

object GenesToNeo4j {
  def insertInNeo4j(results: Array[Gene], dbPath: String): Unit = {
    val databasePath: File = new File(dbPath)

    val session = GraphDatabase.driver(dbPath, AuthTokens.basic("neo4j", "test")).session()


    // Link genes in order
    inTransaction(session) { implicit tx =>
      val geneNodes = results.map(insertGeneToNeo4J)
//      createOrderedRelationships(geneNodes, GffRelationshipTypes.order, tx)
    }

    // TODO shutdown hook..?
    //    new sun.misc.JavaLangAccess().registerShutdownHook()..registerShutdownHook(db)

    session.close()

//    db.shutdown()
  }

  def createNode(label: String, properties: Map[String, String])(implicit tx: Transaction): Long = {
    val props = properties.keys
      .map(key => s"$key: '{$key}'")
      .mkString(", ")

    val query = s"CREATE (n:$label { $props }) RETURN id(n)"

    val result = tx.run(query, mapAsJavaMap[String, Object](properties))
    result.single().get(0).asLong()
  }


  // TODO use a Scala Neo4J wrapper for nicer neo4j syntax
  def insertGeneToNeo4J(gene: Gene)(implicit tx: Transaction): Long = {

    println("Creating gene node for gene " + gene.id)
    val geneNodeId = createNode("gene", Map(
      "start" -> gene.start.toString,
      "end" -> gene.stop.toString,
      "geneID" -> gene.id
    ))
    // TODO sequence
    // TODO organism property

    gene.splicings.foreach(insertTranscript(_, geneNodeId, gene))

    geneNodeId
  }

  def insertTranscript(transcript: Splicing, geneNodeId: Any, gene: Gene)(implicit tx: Transaction): Unit = {
    // Create transcript node
    val transcriptNode = createNode("splicing", properties = Map(
      "start" -> transcript.start.toString,
      "end" -> transcript.stop.toString,
      "geneID" -> gene.id
    ))

    // Link gene to transcripts
    //    transcriptNode.createRelationshipTo(geneNode, GffRelationshipTypes.transcribes)

    // Create nodes for exons and introns. They are already in order
    val geneElementNodes = transcript.children.map { element =>
      val label = element match {
        case Exon(_, _) => "cds" // TODO should it be exon?
        case Intron(_, _) => "intron"
      }

      val nodeId = createNode(label, properties = Map(
        "start" -> element.start.toString,
        "end" -> element.stop.toString,
        "geneID" -> gene.id
      ))

      (element, nodeId)
    }

    // Exons and introns are linked
//    createOrderedRelationships(geneElementNodes.map(_._2), GffRelationshipTypes.links)
//
//    // Link exons as mRNA
//    val exonNodes = geneElementNodes.collect { case (Exon(_, _), node) => node }
//    val intronNodes = geneElementNodes.collect { case (Intron(_, _), node) => node }
//
//    createOrderedRelationships(exonNodes, GffRelationshipTypes.mRna)
//
//    // Exons code a transcript
//    exonNodes.foreach(_.createRelationshipTo(transcriptNode, GffRelationshipTypes.codes))
//
//    // Introns are 'in' a transcript
//    intronNodes.foreach(_.createRelationshipTo(transcriptNode, GffRelationshipTypes.in))
  }

  def createOrderedRelationships(elements: Seq[Node], relType: RelationshipType): Unit =
    createPairs(elements).foreach { case (nodeA, nodeB) =>
      nodeA.createRelationshipTo(nodeB, relType)
    }

  def createPairs[T](elements: Seq[T]): Seq[(T, T)] =
  // TODO what does it mean if they are empty?
    if (elements.nonEmpty) elements.zip(elements.tail) else Seq.empty

  def inTransaction[T](session: Session)(operation: Transaction => T): T = {
    session.writeTransaction(new TransactionWork[T] {
      override def execute(tx: Transaction): T = {
        operation(tx)
      }
    })
  }
}
