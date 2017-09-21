package gfftospark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.neo4j.graphdb.RelationshipType

import scala.util.{Failure, Success, Try}
import scala.io.Source

object GffToSpark {

  @transient lazy val conf: SparkConf = new SparkConf()
    .setMaster("local")
    .setAppName("GffToSpark")
    .set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  /** Main function */
  def main(args: Array[String]): Unit = {

    val bufferedSource = Source.fromFile(args(0))
    val lines = bufferedSource.getLines().toSeq.par
    bufferedSource.close
    val gffLines = lines.map(GffParser.parseLineOrHeader).collect { case l @ GffLine(_, _, _, _, _, _, _, _, _) => l }
    val gffLineTree = FPoaeGffReader.buildTree(gffLines)
    val gffLineTreeNodesForGenes =
      if (gffLineTree.errors.isEmpty) {
        Right(gffLineTree.rootNodes.seq)
      } else {
        Left(gffLineTree.errors.seq)
      }

    val geneFromGffLineTreeNodeBuilder = new StandardGeneFromGffLineTreeNodeBuilder
    val res = for {
      gffLineTreeNodesForGenes <- gffLineTreeNodesForGenes.right
      genes <- geneFromGffLineTreeNodeBuilder.toGenes(gffLineTreeNodesForGenes).right
    } yield genes

    try {
      // Read lines
      val lines: RDD[String] = sc.textFile(args(0))
      val reader = GeneReaders.geneReadersById.get(args(1)).getOrElse(throw new IllegalArgumentException("No such rader found"))
      val dbPath = args(2)

      // Parse lines into a meaningful data structure (GffLine)
      val gffLines: RDD[GffLine] = lines
        .map { l =>
          Try {
            GffParser.parseLineOrHeader(l)
          }.transform[GffLineOrHeader](Success.apply, e => Failure(new IllegalArgumentException(s"Parsefout in regel '${l}'", e)))
            .get
        }
        .collect {
          // Discard headers:
          case l@GffLine(_, _, _, _, _, _, _, _, _) => l
        }

      val genes = reader(gffLines)
      println(s"Number of genes: ${genes.length}")

      val results: Array[Gene] = genes.sortBy(_.start) //.take(100) // Uncomment for testing
      println(results.map { gene =>
        s"Gene: ${gene.id}\n" + gene.splicings.map(_.toString).map("\t" + _).mkString("\n")
      }.mkString("\n "))

      GenesToNeo4j.insertInNeo4j(results, dbPath)

      println(s"Number of genes: ${results.length}")

    }
    finally {
      sc.stop()
    }
  }
}

object GffRelationshipTypes {
  val codes = RelationshipType.withName("codes")
  val in = RelationshipType.withName("in")
  val mRna = RelationshipType.withName("mRNA")
  val links = RelationshipType.withName("links")
  val transcribes = RelationshipType.withName("transcribes")
  val order = RelationshipType.withName("order")
}
