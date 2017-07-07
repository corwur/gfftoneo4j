package gfftospark

import java.io.File

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.neo4j.graphdb.{GraphDatabaseService, Label, Node, RelationshipType}
import org.neo4j.graphdb.factory.GraphDatabaseFactory

import scala.util.{Failure, Success, Try}

object GffToSpark {

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("GffToSpark")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  /** Main function */
  def main(args: Array[String]): Unit = {
    try {
      val databasePath: File = new File("mijndb.db")
      val db = new GraphDatabaseFactory().newEmbeddedDatabase(databasePath)

      // Read lines
      val lines: RDD[String] = sc.textFile(args(0))

      // Parse lines into a meaningful data structure (GffLine)
      val gffLines: RDD[GffLine] = lines.map { l =>
        Try {
          GffParser.parseLine(l)
        }.transform[GffLine](Success.apply, e => Failure(new IllegalArgumentException(s"Parsefout in regel '${l}'", e)))
          .get
      }

        // Filter out not used stuff TODO find out what to do with this
        .filter(l => l.feature != "similarity")

      // Group the data by gene
      val linesPerGene: RDD[(GeneId, Iterable[GffLine])] = gffLines.groupBy(GeneReader.getGeneId)

      // Convert the data into a Gene structure
      val genes: RDD[Gene] = linesPerGene.map((GeneReader.linesToGene _).tupled)

      // Collect results
      val results: Array[Gene] = genes.collect().take(100)

      println(results.map { gene =>
        s"Gene: ${gene.id}\n" + gene.transcripts.map(_.toString).map("\t" + _).mkString("\n")
      }.mkString("\n "))

      // TODO shutdown hook..?
      //    new sun.misc.JavaLangAccess().registerShutdownHook()..registerShutdownHook(db)

      results.foreach(insertGeneToNeo4J(db, _))
      db.shutdown()

      println(s"Number of genes: ${results.length}")
    }
    finally {
      sc.stop()
    }
  }

  // TODO use a Scala Neo4J wrapper for nicer neo4j syntax
  def insertGeneToNeo4J(db: GraphDatabaseService, gene: Gene): Unit = {
    def inTransaction(operation: => Unit): Unit = {
      val tx = db.beginTx
      try {
        operation
        tx.success
      }
      finally {
        if (tx != null) tx.close()
      }
    }

    inTransaction {
      val geneNode = db.createNode(Label.label("gene_" + "organism")) // TODO
      println("Creating gene node for gene " + gene.id)
      // TODO sequence
      geneNode.setProperty("start", gene.start)
      geneNode.setProperty("end", gene.stop)
      geneNode.setProperty("geneID", gene.id)

      // mRNA
      val geneElementNodes = gene.transcripts.head.children.map { element =>

        val label = element match {
          case CodingSequence(_, _) => "CDS"
          case Intron(_, _) => "intron"
        }

        val node = db.createNode(Label.label(label))

        node.setProperty("start", element.start)
        node.setProperty("end", element.stop)
        node.setProperty("geneID", gene.id)

        (element, node)
      }

      val cdsNodes = geneElementNodes.collect { case (CodingSequence(_, _), node) => node }
      createOrderedRelationships(cdsNodes, GffRelationshipTypes.mRna)

      // LINKS relationship
      createOrderedRelationships(geneElementNodes.map(_._2), GffRelationshipTypes.links)
    }
  }

  def createOrderedRelationships(elements: Seq[Node], relType: RelationshipType) =
    createPairs(elements).foreach { case (nodeA, nodeB) =>
      nodeA.createRelationshipTo(nodeB, relType)
    }

  def createPairs[T](elements: Seq[T]): Seq[(T, T)] =
    if (elements.nonEmpty) elements.zip(elements.tail) else Seq.empty
    // TODO what does it mean if they are empty?
}

object GffRelationshipTypes {
  val mRna = RelationshipType.withName("mRNA")
  val links = RelationshipType.withName("mRNA")
}
