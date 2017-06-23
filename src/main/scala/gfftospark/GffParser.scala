package gfftospark

import scala.util.Try
import scala.util.parsing.combinator.JavaTokenParsers

/**
  * Functions for parsing lines of a GFF file
  */
object GffParser extends JavaTokenParsers {

  val attributeKeyValueSeparator = ";"

  def notDoubleQuoteRegex = "[^\"]*".r

  def oneOrMoreNonWhitespaceRegex = "[^\\s]+".r

  def orPeriod[A](p: Parser[A]): Parser[Option[A]] = {

    def periodAsNone: Parser[Option[Nothing]] = literal(".") map (_ => None)

    def wrapInSome[A](p: Parser[A]): Parser[Option[A]] = p map (a => Some(a))

    wrapInSome(p) | periodAsNone
  }

  def long: Parser[Long] = wholeNumber ^^ (_.toLong)

  def double: Parser[Double] = floatingPointNumber ^^ (_.toDouble)

  def seqname: Parser[String] =
    oneOrMoreNonWhitespaceRegex withFailureMessage "Expected a `seqname`"

  def source: Parser[String] =
    oneOrMoreNonWhitespaceRegex withFailureMessage "Expected a `source`"

  def feature: Parser[String] =
    oneOrMoreNonWhitespaceRegex withFailureMessage "Expected a `feature`"

  def start: Parser[Long] =
    long withFailureMessage "Expected a `start`"

  def end: Parser[Long] =
    long withFailureMessage "Expected an `end`"

  def score: Parser[Option[Double]] =
    orPeriod(double) withFailureMessage "Expected a `score`"

  def strand: Parser[Option[Strand]] = {
    val plusParser = "+" ^^ (_ => Forward)
    val minusParser = "-" ^^ (_ => Reverse)
    orPeriod(plusParser | minusParser) withFailureMessage "Expected a `strand`"
  }

  def frame: Parser[Option[Long]] =
    orPeriod(("0" | "1" | "2").map(_.toLong)) withFailureMessage "Expected a `frame`"

  def attributeValueWithQuotes: Parser[String] =
    "\"" ~> notDoubleQuoteRegex <~ "\"" withFailureMessage "Expected an attribute value, surrounded by quotes"

  def attributeValueWithoutQuotes: Parser[String] =
    """[^";\s]+""".r withFailureMessage "Expected an attribute value"

  def attributeValue = attributeValueWithQuotes | attributeValueWithoutQuotes

  def attributeKey: Parser[String] =
    """[^";\s=]+""".r withFailureMessage "Expected an attribute key"

  def attributeKeyValue: Parser[(String, String)] =
    (attributeKey <~ opt("=")) ~ attributeValue map {
      case key ~ value => (key, value)
    }

  def attributeKeyValues: Parser[Map[String, String]] =
    rep1sep(attributeKeyValue, attributeKeyValueSeparator) <~ opt(
      attributeKeyValueSeparator) map (_.toMap)

  def attributes: Parser[Either[String, Map[String, String]]] = {
    def asLeft[A](p: Parser[A]) = p map (Left(_))
    def asRight[A](p: Parser[A]) = p map (Right(_))

    asRight(phrase(attributeKeyValues)) | asLeft(".*".r)
  }

  def line =
    seqname ~
      source ~
      feature ~
      start ~
      end ~
      score ~
      strand ~
      frame ~
      attributes map {
      case seqname ~ source ~ feature ~ start ~ end ~ score ~ strand ~ frame ~ attributes =>
        GffLine(seqname,
                source,
                feature,
                start,
                end,
                score,
                strand,
                frame,
                attributes)
    }

  def parseLine(s: String): GffLine =
    parseAll(line, s).getOrElse(throw new IllegalArgumentException)
}

object App {
  def main(args: Array[String]): Unit = {

    val toParse =
      """
        |3 transcribed_unprocessed_pseudogene  gene        11869 14409 . - . henk devries; gene_id "ENSG00000223972"; henk de.vries; Note "Clone Y74C9A; Genbank AC024206"; gene_name "DDX11L1"; gene_source "havana"; gene_biotype "transcribed_unprocessed_pseudogene";
        |1 processed_transcript                transcript  11869 14409 . + . roborovski a 1
        |Chr3   giemsa heterochromatin  4500000 6000000 . . .   Band 3q12.1
        |Chr3 giemsa heterochromatin 4500000 6000000 . . . Band 3q12.1 ; Note "Marfan's syndrome"
        |Chr1        assembly Link   10922906 11177731 . . . Target Sequence:LINK_H06O01 1 254826
        |LINK_H06O01 assembly Cosmid 32386    64122    . . . Target Sequence:F49B2       6 31742
        |X	Ensembl	Repeat	2419108	2419128	42	.	.	hid=trf; hstart=1; hend=21
        |X	Ensembl	Repeat	2419108	2419410	2502	-	.	hid=AluSx; hstart=1; hend=303
        |X	Ensembl	Repeat	2419108	2419128	0	.	.	hid=dust; hstart=2419108; hend=2419128
        |X	Ensembl	Pred.trans.	2416676	2418760	450.19	-	2	genscan=GENSCAN00000019335
        |X	Ensembl	Variation	2413425	2413425	.	+	.
        |X	Ensembl	Variation	2413805	2413805	.	+	.
        """.stripMargin

    val lines = toParse.split("\n") map (_.trim) filter (_.nonEmpty)

    lines map GffParser.parseLine foreach println
  }
}

case class GffLine(
                    seqname: String,
                    source: String,
                    feature: String,
                    start: Long,
                    stop: Long,
                    score: Option[Double],
                    strand: Option[Strand],
                    frame: Option[Long],
                    attributes: Either[String, Map[String, String]] // Either a single string or list of attributes
                  ) extends Serializable

