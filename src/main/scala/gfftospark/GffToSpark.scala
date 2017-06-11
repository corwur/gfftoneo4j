package gfftospark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.util.{Failure, Success, Try}

object GffToSpark extends GffToSpark {

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("GffToSpark")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  /** Main function */
  def main(args: Array[String]): Unit = {
    try {
      val lines: RDD[String] = sc.textFile(args(0)) //.sample(true, 0.05)

      // Parse lines into a meaningful data structure (GffLine)
      val gffLines: RDD[GffLine] = lines.map { l =>
        Try {
          parseLine(l)
        }.transform[GffLine](Success.apply, e => Failure(new IllegalArgumentException(s"Parsefout in regel '${l}'", e)))
          .get
      }

      // Filter out transcripts and other stuff
        .filter(l => l.feature != "transcript" && l.feature != "similarity")


      // Group the data by gene
      val linesPerGene = gffLines.groupBy(getKey)


      // Scan over the lines to collect genes


      val results: Array[(GeneId, Iterable[GffLine])] = linesPerGene.collect() //.take(2)

      println(results.map {
        case (geneId, lines) => s"GeneID: ${geneId}\n" + lines.map(_.toString).map("\t" + _).mkString("\n")
      }.mkString("\n "))
      println(s"Number of genes: ${results.length}")
    }
    finally {
      sc.stop()
    }
  }

  def getKey(l: GffLine): GeneId =
    (l.feature, l.attributes) match {
      case ("gene", Left(geneId)) => geneId
      case ("CDS" | "transcript" | "intron" | "start_codon" | "stop_codon", Right(attributes)) =>
        attributes.getOrElse("gene_id", throw new IllegalArgumentException("Parse error: gene has attribute-pairs instead of gene name"))

      case _ => throw new IllegalArgumentException(s"Parse error (${l.feature}, ${l.attributes}: gene has attribute-pairs instead of gene name")
    }
}

/** The parsing and kmeans methods */
class GffToSpark extends Serializable {
  // TODO: replace with parser combinator
  def parseLine(line: String): GffLine = {
    val fields = line.split("\t")
    require(fields.length == 9)

    val attributesFieldColumnIndex = 8
    val attributesField = fields(attributesFieldColumnIndex)

    // Try to parse a list of attributes (key value pairs) or if that fails, a single string
    // NOTE: lines with genes and transcripts seem to have a single string with the gene ID / transcript ID
    val attributes = Try(parseAttributesMap(attributesField)).map(Right(_))
      .getOrElse(Left(attributesField))

    GffLine(
      fields(0),
      fields(1),
      fields(2),
      fields(3).toLong,
      fields(4).toLong,
      0L, // TODO fields(5).toDouble,
      Strand.fromString(fields(6)),
      0L, // TODO fields(7).toLong,
      attributes
    )
  }

  def parseAttributesMap(a: String): Map[String, String] =
    a.split(";")
      .map(_.trim) // Remove spaces after the ;
      .filter(_.nonEmpty) // Each attribute ends with a ;, so we remove the last empty one
      .map(parseKeyValuePair)
      .toMap

  def parseKeyValuePair(kvp: String): (String, String) = {
    kvp.split(" ").toSeq match {
      case key +: tail =>
        val valueWithQuotes = tail.mkString(" ")
        //        println(s"Parsing key: ${key}, value: ${valueWithQuotes}")
        val value = valueWithQuotes.substring(1, valueWithQuotes.length - 1)

        (key, value)
      case _ =>
        throw new IllegalArgumentException("Ongeldig attribute: kvp")
    }
  }
}







