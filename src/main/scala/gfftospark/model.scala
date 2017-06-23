package gfftospark

/**
  * This file contains data structures (models) that represent the genetic data in a GFF file
  */

trait HasPositionOnDna {
  val start: Long
  val stop: Long
}

sealed trait TranscriptElement extends HasPositionOnDna

case class DnaSequence(genes: Seq[Gene])

case class Gene(id: String, start: Long, stop: Long, transcripts: Seq[Transcript]) extends HasPositionOnDna

case class CodingSequence(start: Long, stop: Long) extends TranscriptElement

case class Intron(start: Long, stop: Long) extends TranscriptElement

case class Transcript(id: String, children: Seq[TranscriptElement]) {
  val mRNA: Seq[CodingSequence] = children.collect { case cds @ CodingSequence(_, _) => cds }
}

sealed trait Strand extends Serializable

case object Forward extends Strand

case object Reverse extends Strand

object Strand {
  def fromString(s: String): Strand = {
    s match {
      case "+" => Forward
      case "-" => Reverse
      case _ => throw new IllegalArgumentException("Ongeldige strand, verwachtte + of -")
    }
  }
}
