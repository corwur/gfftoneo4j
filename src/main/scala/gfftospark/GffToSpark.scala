package gfftospark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.neo4j.graphdb.RelationshipType
import scopt.OptionParser

import scala.util.{Failure, Success, Try}

case class CommandLineParams(file: String, format: String = "fpoae", neo4jUrl: String = "bolt://127.0.0.1:7687", neo4jUsername: String = "neo4j", neo4jPassword: String = "test")

object GffToSpark {

  @transient lazy val conf: SparkConf = new SparkConf()
    .setMaster("local")
    .setAppName("GffToSpark")
    .set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  /** Main function */
  def main(args: Array[String]): Unit = {
    commandLineParser.parse(args, CommandLineParams("")) match {
      case Some(config) => importGff(config)
      case None => // command line arguments are bad, error message will have been displayed
    }
  }

  private def importGff(params: CommandLineParams) = {
    try {
      // Read lines
      val lines: RDD[String] = sc.textFile(params.file)
      val reader = GeneReaders.geneReadersById.get(params.format).getOrElse(throw new IllegalArgumentException("No such reader found"))
      val dbUrl = params.neo4jUrl

      // Parse lines into a meaningful data structure (GffLine)
      val gffLines: RDD[GffLineOrHeader] = lines
        .map { l =>
            GffParser.parseLineOrHeader(l)
          .fold(msg => Failure(new IllegalArgumentException(s"Parsefout in regel '${l}': $msg")), Success.apply)
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

  val commandLineParser = new OptionParser[CommandLineParams]("gfftoneo4j") {
    head("gfftoneo4j", "0.1")

    opt[String]('f', "file")
      .required()
      .valueName("<file>")
      .action((x, c) => c.copy(file = x))
      .text("Path to GFF file")

    opt[String]('t', "type")
      .required()
      .valueName("<type>")
      .action((x, c) => c.copy(format = x))
      .text(s"GFF file type. Possible values: ${GeneReaders.formats.mkString(",")}")

    opt[String]('u', "url")
      .valueName("<url>")
      .action((x, c) => c.copy(neo4jUrl = x))
      .text("Neo4J server URL")
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
