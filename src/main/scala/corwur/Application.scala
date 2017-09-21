package corwur

import corwur.CommandLineParser.CommandLineArgs
import corwur.genereader.{DnaSequence, GeneReaders}
import corwur.gffparser.{GffLine, GffLineOrHeader, GffParser}
import corwur.neo4j.GenesToNeo4j
import org.neo4j.graphdb.RelationshipType

import scala.io.Source
import scala.util.{Failure, Success}

/**
  * Main class
  *
  * Reads and parses a GFF file, construct Genes and insert them into a Neo4j database
  */
object Application {
  /** Main function */
  def main(args: Array[String]): Unit =
    CommandLineParser.parser
      .parse(args, init = CommandLineArgs(""))
      .foreach(importGffFile)

  private def importGffFile(params: CommandLineArgs) = {
    // Read lines
    val lines: Iterator[String] = Source.fromFile(params.file).getLines()
    val reader = GeneReaders.geneReadersById.get(params.format).getOrElse(throw new IllegalArgumentException("No such reader found"))
    val dbUrl = params.neo4jUrl

    // Parse lines into a meaningful data structure (GffLine)
    val gffLines: Iterator[GffLineOrHeader] = lines
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
      .toSeq
      .groupBy { case GffLine(seqname, _, _, _, _, _, _, _, _) => seqname }

    val sequences = linesPerSequence.map { case (sequenceName, lines) =>
      val genes = reader(lines)
      DnaSequence(sequenceName, genes)
    }

    // Insert in database
    GenesToNeo4j.insertSequences(sequences.toSeq, dbUrl)

    val nrGenes = sequences.map(_.genes.size).sum
    println("Number of genes processed: " + nrGenes)
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
