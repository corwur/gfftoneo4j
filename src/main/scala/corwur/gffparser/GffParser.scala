package corwur.gffparser

//REVIEW: Part of GffLine , either move the classes to this file or move GffLineOrHeader, GffLine and Header
import corwur.genereader.{Forward, Reverse, Strand}

import scala.util.parsing.combinator.JavaTokenParsers

/**
  * Functions for parsing lines of a GFF file
  */
object GffParser extends JavaTokenParsers {
  /**
    * Parse a line in a GFF file
    *
    * @param s The line to parse
    * @return Either an error message or a successful result: a [[GffLine]] or [[Header]]
    */
    //REVIEW: A Try[GffLineOrHeader] expresses the function better than an Either.
  def parseLineOrHeader(s: String): Either[String, GffLineOrHeader] =
    parseAll(lineOrHeader, s) match {
      case Success(result, _) => Right(result)
      case Failure(msg, _) =>  Left(msg)
      case Error(msg, _) =>  Left(msg)
    }

  def lineOrHeader: Parser[GffLineOrHeader] =
    line | header

  val attributeKeyValueSeparator = ";"

  def header = phrase("#" ~> ".*".r) map (Header(_))

  def notDoubleQuoteRegex = "[^\"]*".r

  def oneOrMoreNonWhitespaceRegex = "[^\\s]+".r

  //REVIEW: Can be shorter: def orPeriod[A](p: Parser[A]): Parser[Option[A]] =  literal(".") ^^ (_ => None ) | p  ^^ (a => Some(a))
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

  //REVIEW: Different 'style' than elsewhere
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
    """[^";]+""".r map (_.trim) withFailureMessage "Expected an attribute value"

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

  //REVIEW: phrase(attributeKeyValues) ^^ (Right(_)) | ".*".r ^^ (Left(_))
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

}

sealed trait GffLineOrHeader

final case class Header(value: String) extends GffLineOrHeader

//REVIEW: GffParser should not reference model (Strand, (Forward and Reverse)).
//Strand should be in a spearte file or part of GffParser.
final case class GffLine(
                    seqname: String,
                    source: String,
                    feature: String,
                    start: Long,
                    stop: Long,
                    score: Option[Double],
                    strand: Option[Strand],
                    frame: Option[Long],
                    attributes: Either[String, Map[String, String]] // Either a single string or list of attributes
                  ) extends Serializable with GffLineOrHeader {
  def getAttribute(key: String): Option[String] =
    attributes.right.toOption.flatMap(_.get(key))
}

