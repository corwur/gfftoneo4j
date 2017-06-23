package gfftospark

import scala.util.Try

/**
  * Functions for parsing lines of a GFF file
  */
object GffParser {
  /**
    * Parses a single line of a GFF file
    *
    * @param line
    */
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

case class GffLine(
                    seqname: String,
                    source: String,
                    feature: String,
                    start: Long,
                    stop: Long,
                    score: Double,
                    strand: Strand,
                    frame: Long,
                    attributes: Either[String, Map[String, String]] // Either a single string or list of attributes
                  ) extends Serializable
