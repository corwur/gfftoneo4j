package corwur

import corwur.CommandLineParser.CommandLineArgs
import corwur.genereader.{DnaSequence, GeneReaders}
import corwur.gffparser.{GffLine, GffLineOrHeader, GffParser}
import corwur.neo4j.GenesToNeo4j
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.neo4j.graphdb.RelationshipType

import scala.util.{Failure, Success}

/**
  * Main class
  *
  * Reads and parses a GFF file, construct Genes and insert them into a Neo4j database
  */
object Application {

  @transient lazy val conf: SparkConf = new SparkConf()
    .setMaster("local")
    .setAppName("gfftoneo4j")
    .set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  /** Main function */
  def main(args: Array[String]): Unit =
    CommandLineParser.parser
      .parse(args, init = CommandLineArgs(""))
      .foreach(importGffFile)

  private def importGffFile(params: CommandLineArgs) = {
    try {
      // Read lines
      val lines: RDD[String] = sc.textFile(params.file)
      val reader = GeneReaders.geneReadersById.get(params.format).getOrElse(throw new IllegalArgumentException("No such reader found"))
      val dbUrl = params.neo4jUrl

      // Parse lines into a meaningful data structure (GffLine)
      val gffLines: RDD[GffLineOrHeader] = lines
        .map { l =>
            GffParser.parseLineOrHeader(l)
          .fold(msg => Failure(new IllegalArgumentException(s"Parse error in line '${l}': $msg")), Success.apply)
            .get
        }

      // Discard headers and group by sequence
      val linesPerSequence = gffLines
        .flatMap {
          case g@GffLine(_, _, _, _, _, _, _, _, _) => Seq(g)
          case _ => Seq.empty
        }
        .groupBy { case GffLine(seqname, _, _, _, _, _, _, _, _) => seqname }

      val sequences = linesPerSequence.map { case (sequenceName, lines) =>
        val genes = reader(lines)

        DnaSequence(sequenceName, genes)
      }

      // Insert in database
      GenesToNeo4j.insertSequences(sequences.collect(), dbUrl)

      val nrGenes = sequences.map(_.genes.size).collect().sum
      println("Number of genes processed: " + nrGenes)
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
