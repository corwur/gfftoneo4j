package gfftospark

import java.io.File

import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.graphdb.{GraphDatabaseService, Label, Node, RelationshipType}

object GenesToNeo4j {
  def insertInNeo4j(results: Array[Gene], dbPath: String): Unit = {
    // TODO can we connect to a server as well..?
    val databasePath: File = new File(dbPath)
    val db = new GraphDatabaseFactory().newEmbeddedDatabase(databasePath)

    val geneNodes = results.map(insertGeneToNeo4J(db, _))

    // Link genes in order
    inTransaction(db) {
      createOrderedRelationships(geneNodes, GffRelationshipTypes.order)
    }

    // TODO shutdown hook..?
    //    new sun.misc.JavaLangAccess().registerShutdownHook()..registerShutdownHook(db)

    db.shutdown()
  }

  // TODO use a Scala Neo4J wrapper for nicer neo4j syntax
  def insertGeneToNeo4J(db: GraphDatabaseService, gene: Gene): Node =
    inTransaction(db) {
      val geneNode = db.createNode(Label.label("gene"))
      println("Creating gene node for gene " + gene.id)
      // TODO sequence
      // TODO organism property
      geneNode.setProperty("start", gene.start)
      geneNode.setProperty("end", gene.stop)
      geneNode.setProperty("geneID", gene.id)

      gene.splicings.foreach(insertTranscript(_, geneNode, gene, db))

      geneNode
    }

  def insertTranscript(transcript: Splicing, geneNode: Node, gene: Gene, db: GraphDatabaseService): Unit = {
    // Create transcript node
    val transcriptNode = db.createNode(Label.label("splicing"))

    transcriptNode.setProperty("start", transcript.start)
    transcriptNode.setProperty("end", transcript.stop)
    transcriptNode.setProperty("geneID", gene.id)

    // Link gene to transcripts
    transcriptNode.createRelationshipTo(geneNode, GffRelationshipTypes.transcribes)

    // Create nodes for exons and introns. They are already in order
    val geneElementNodes = transcript.children.map { element =>
      val label = element match {
        case Exon(_, _) => "cds" // TODO should it be exon?
        case Intron(_, _) => "intron"
      }

      val node = db.createNode(Label.label(label))

      node.setProperty("start", element.start)
      node.setProperty("end", element.stop)
      node.setProperty("geneID", gene.id)

      (element, node)
    }

    // Exons and introns are linked
    createOrderedRelationships(geneElementNodes.map(_._2), GffRelationshipTypes.links)

    // Link exons as mRNA
    val exonNodes = geneElementNodes.collect { case (Exon(_, _), node) => node }
    val intronNodes = geneElementNodes.collect { case (Intron(_, _), node) => node }

    createOrderedRelationships(exonNodes, GffRelationshipTypes.mRna)

    // Exons code a transcript
    exonNodes.foreach(_.createRelationshipTo(transcriptNode, GffRelationshipTypes.codes))

    // Introns are 'in' a transcript
    intronNodes.foreach(_.createRelationshipTo(transcriptNode, GffRelationshipTypes.in))
  }

  def createOrderedRelationships(elements: Seq[Node], relType: RelationshipType): Unit =
    createPairs(elements).foreach { case (nodeA, nodeB) =>
      nodeA.createRelationshipTo(nodeB, relType)
    }

  def createPairs[T](elements: Seq[T]): Seq[(T, T)] =
    // TODO what does it mean if they are empty?
    if (elements.nonEmpty) elements.zip(elements.tail) else Seq.empty

  def inTransaction[T](db: GraphDatabaseService)(operation: => T): T = {
    val tx = db.beginTx
    try {
      val result = operation
      tx.success()
      result
    }
    finally {
      if (tx != null) tx.close()
    }
  }
}
