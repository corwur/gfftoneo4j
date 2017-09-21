package corwur.genereader

/**
  * This file contains data structures (models) that represent the genetic data in a GFF file
  */

trait HasPositionOnDna {
  val start: Long
  val stop: Long
}

sealed trait TranscriptElement extends HasPositionOnDna {
  def attributes: Map[String, String]
}

case class DnaSequence(name: String, genes: Seq[Gene])

case class Gene(sequenceName: String, id: String, start: Long, stop: Long, splicings: Seq[Splicing], attributes: Map[String, String]) extends HasPositionOnDna

case class Exon(start: Long, stop: Long, attributes: Map[String, String]) extends TranscriptElement

case class Intron(start: Long, stop: Long) extends TranscriptElement {
  override def attributes = Map.empty
}

case class Splicing(id: String, start: Long, stop: Long, children: Seq[TranscriptElement], attributes: Map[String, String]) {
  val mRNA: Seq[Exon] = children.collect { case cds @ Exon(_, _, _) => cds }
}

sealed trait Strand extends Serializable

case object Forward extends Strand

case object Reverse extends Strand
