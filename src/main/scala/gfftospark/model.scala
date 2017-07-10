package gfftospark

/**
  * This file contains data structures (models) that represent the genetic data in a GFF file
  */

trait HasPositionOnDna {
  val start: Long
  val stop: Long
}

sealed trait TranscriptElement extends HasPositionOnDna {
  def gffLine: GffLine
}

case class DnaSequence(genes: Seq[Gene])

case class Gene(id: String, start: Long, stop: Long, splicings: Seq[Splicing]) extends HasPositionOnDna

case class Exon(start: Long, stop: Long, gffLine: GffLine) extends TranscriptElement

case class Intron(start: Long, stop: Long, gffLine: GffLine) extends TranscriptElement

case class Splicing(id: String, children: Seq[TranscriptElement]) {
  val mRNA: Seq[Exon] = children.collect { case cds @ Exon(_, _, _) => cds }
}

sealed trait Strand extends Serializable

case object Forward extends Strand

case object Reverse extends Strand
