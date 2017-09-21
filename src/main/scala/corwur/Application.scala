package corwur

import corwur.CommandLineParser.CommandLineArgs
import corwur.genereader.{DnaSequence, Gene, GeneReaders}
import corwur.gffparser.{GffLine, GffParser}
import corwur.neo4j.{GenesToNeo4j, Neo4JUtils}

import scala.io.Source

/**
  * Main class
  *
  * Reads and parses a GFF file, construct Genes and insert them into a Neo4j database
  */
object Application {
  /** Main function */
  def main(args: Array[String]): Unit = {
    CommandLineParser.parser
      .parse(args, init = CommandLineArgs(""))
      .foreach { params =>
        implicit val reader = GeneReaders.geneReadersById.get(params.format)
          .getOrElse(throw new IllegalArgumentException("No such reader found"))

        importGffFile(params.file, params.neo4jUrl, params.neo4jUsername, params.neo4jPassword)
      }
  }

  type GeneReader = Iterable[GffLine] => Array[Gene]

  def importGffFile(file: String, neo4jUrl: String, neo4jUsername: String, neo4jPassword: String)(implicit linesToGenes: GeneReader): Unit =
    Neo4JUtils.withSession(neo4jUrl, neo4jUsername, neo4jPassword) { implicit session =>
      readLines(file)
        .flatMap(parseGffLine)
        .toSeq
        .groupBy(_.seqname)
        .mapValues(linesToGenes)
        .mapValues(_.toSeq)
        .map(genesToDnaSequence)
        .foreach(GenesToNeo4j.insertSequence)
    }

  def readLines(file: String): Iterator[String] =
    Source.fromFile(file).getLines()

  def parseGffLine(l: String): Option[GffLine] =
    GffParser.parseLineOrHeader(l) match {
      case Left(errorMessage) => throw new IllegalArgumentException(s"Parse error in line '${l}': $errorMessage")
      case Right(g@GffLine(_, _, _, _, _, _, _, _, _)) => Some(g)
      case _ => None // Ignore headers
    }

  val genesToDnaSequence = Function.tupled {
    (sequenceName: String, genes: Seq[Gene]) => DnaSequence(sequenceName, genes)
  }
}
