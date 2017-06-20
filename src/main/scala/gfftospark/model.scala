package gfftospark

trait DnaThingy {
  val start: Long
  val stop: Long
}

case class DnaSequence(genes: Seq[Gene])

case class Gene(id: String, start: Long, stop: Long, transcripts: Seq[Transcript]) extends DnaThingy

case class CodingSequence(start: Long, stop: Long) extends DnaThingy

case class Intron(start: Long, stop: Long) extends DnaThingy

sealed trait Transcript {
  val mRNA: Seq[CodingSequence]
}

final case class TerminalCodingSequence(cds: CodingSequence) extends Transcript {
  override val mRNA: Seq[CodingSequence] = Seq(cds)
}

final case class Cons(cds: CodingSequence, intron: Intron, tail: Transcript) extends Transcript {
  override val mRNA: Seq[CodingSequence] = cds +: tail.mRNA
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
