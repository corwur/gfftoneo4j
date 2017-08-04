package blast

import scala.util.Try
import scala.util.parsing.combinator.{JavaTokenParsers, RegexParsers}

object BlastFileParser extends JavaTokenParsers {

  def oneOrMoreNonWhitespaceRegex = "[^\\s]+".r
  def oneOrMoreWhitespaceRegex = "\\s+".r
  def anyContentRegex = ".*".r

  def blastFileLine: Parser[BlastFileLine] =
    oneOrMoreNonWhitespaceRegex ~ oneOrMoreNonWhitespaceRegex ~ floatingPointNumber <~ phrase(anyContentRegex) map {
      case leftGene ~ rightGene ~ identityScore => BlastFileLine(leftGene, rightGene, identityScore.toDouble)
    }

  def parseLine(str: String): BlastFileLine = {
    parseAll(blastFileLine, str).getOrElse(throw new IllegalStateException)
  }

}
