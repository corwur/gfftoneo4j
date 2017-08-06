package gfftospark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.neo4j.graphdb.RelationshipType

import scala.util.{Failure, Success, Try}

object GffToSpark {

  @transient lazy val conf: SparkConf = new SparkConf()
    .setMaster("local")
    .setAppName("GffToSpark")
    .set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  /** Main function */
  def main(args: Array[String]): Unit = {
    try {
      // Read lines
      val lines: RDD[String] = sc.textFile(args(0)) // .sample(false, 0.25)
      val reader = GeneReaders.geneReadersById.getOrElse(args(1), throw new IllegalArgumentException("No such rader found"))
      val dbUrl = args(2)

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

      val genesRdd = sc.makeRDD(genes)

      genesRdd.groupBy(_.sequenceName).foreach { case (sequenceName, genes) =>
        println(s"Processing sequence $sequenceName. Nr of genes: ${genes.size}")
        GenesToNeo4j.insertInNeo4j(genes.toArray, dbUrl)
      }
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
