package gfftospark

import gfftospark.Neo4JUtils._
import org.neo4j.driver.v1._
import org.neo4j.graphdb.RelationshipType

import scala.collection.JavaConversions._

object GenesToNeo4j {
  def insertInNeo4j(genes: Array[Gene], dbPath: String): Unit = {
    withSession(dbPath, "neo4j", "test") { session =>
      val geneNodeIds = inTransaction(session) { implicit tx =>
        genes.zipWithIndex.map { case (gene, index) =>
          println(s"Creating node for gene ${index} of ${genes.size}")
          insertGeneToNeo4J(gene)
        }
      }

      println("Creating relationships between genes")
      inTransaction(session) { implicit tx =>
        // Link genes in order
        createOrderedRelationships(geneNodeIds, GffRelationshipTypes.order)
      }
    }
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

  def insertTranscript(transcript: Splicing, geneNodeId: NodeId, gene: Gene)(implicit tx: Transaction): Unit = {
    // Create transcript node
    val transcriptNode = createNode("splicing", properties = Map(
      "start" -> transcript.start.toString,
      "end" -> transcript.stop.toString,
      "geneID" -> gene.id
    ))

    // Link gene to transcripts
    createRelationship(transcriptNode, geneNodeId, GffRelationshipTypes.transcribes)

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
    createOrderedRelationships(geneElementNodes.map(_._2), GffRelationshipTypes.links)

    // Link exons as mRNA
    val exonNodes = geneElementNodes.collect { case (Exon(_, _), node) => node }
    val intronNodes = geneElementNodes.collect { case (Intron(_, _), node) => node }

    createOrderedRelationships(exonNodes, GffRelationshipTypes.mRna)

    // Exons code a transcript
    exonNodes.foreach(createRelationship(_, transcriptNode, GffRelationshipTypes.codes))

    // Introns are 'in' a transcript
    intronNodes.foreach(createRelationship(_, transcriptNode, GffRelationshipTypes.in))
  }
}

object Neo4JUtils {
  type NodeId = Long

  def withSession[T](url: String, username: String, password: String)(f: Session => T) = {
    val driver = GraphDatabase.driver(url, AuthTokens.basic(username, password))
    val session = driver.session()
    try {
      f(session)
    } finally {
      session.close()
      driver.close()
    }
  }

  def inTransaction[T](session: Session)(operation: Transaction => T): T = {
    session.writeTransaction(new TransactionWork[T] {
      override def execute(tx: Transaction): T = {
        operation(tx)
      }
    })
  }

  def createNode(label: String, properties: Map[String, String])(implicit tx: Transaction): NodeId = {
    val props = properties.keys
      .map(key => s"$key: {$key}")
      .mkString(", ")

    val query = s"CREATE (n:$label { $props }) RETURN id(n)"
    //    println(query)

    val javaParams = mapAsJavaMap[String, Object](properties)
    //    println(javaParams)
    val result = tx.run(new Statement(query, javaParams))
    result.single().get(0).asLong()
  }

  def createRelationship(nodeA: NodeId, nodeB: NodeId, relType: RelationshipType)(implicit tx: Transaction): Unit = {
    val query = s"MATCH (a),(b) WHERE id(a) = ${nodeA} AND id(b) = ${nodeB} create unique (a)-[r:${relType.name}]->(b) RETURN r"
    // TODO why doesn't it work with parameters?
    //      val params = Map("idA" -> nodeIdA.toString, "idB" -> nodeIdB.toString)
    //      val result = tx.run(query, mapAsJavaMap[String, Object](params))
    tx.run(query)
  }

  def createOrderedRelationships(nodeIds: Seq[NodeId], relType: RelationshipType)(implicit tx: Transaction): Unit =
    createPairs(nodeIds).foreach { case (nodeIdA, nodeIdB) =>
      //      println(s"Creating relationship between node $nodeIdA and $nodeIdB")
      createRelationship(nodeIdA, nodeIdB, relType)
    }

  def createPairs[T](elements: Seq[T]): Seq[(T, T)] =
  // TODO what does it mean if they are empty?
    if (elements.nonEmpty) elements.zip(elements.tail) else Seq.empty

}
