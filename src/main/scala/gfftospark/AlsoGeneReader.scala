package gfftospark

/**
  * Functions for reading Gene objects and their Transcripts from a series of GFFLines
  */
object AlsoGeneReader {

  val GENE = "gene_id"
  val EXON = "CDS"
  val INTRON = "intron"
  val START_CODON = "start_codon"
  val STOP_CODON = "stop_codon"
  val SPLICING = "transcript"
  val GENE_ID_KEY = "gene_id"

  /**
    * Get the ID of the Gene this GFFLine belongs to
    *
    * @param line
    */
  def getGeneId(line: GffLine): GeneId = {
    (line.feature, line.attributes) match {
      case (GENE, Left(geneId)) => geneId
      case (SPLICING, Left(splicingId)) => splicingId.split('.').head
      case (EXON | SPLICING | INTRON | START_CODON | STOP_CODON, Right(attributes)) =>
        attributes.getOrElse(GENE_ID_KEY, throw new IllegalArgumentException("Parse error: gene has attribute-pairs instead of gene name"))

      case _ => throw new IllegalArgumentException(s"Parse error (${line.feature}, ${line.attributes}: gene has attribute-pairs instead of gene name")
    }
  }

  def linesToGene(geneId: GeneId, lines: Iterable[GffLine]): Gene = {
    val exons = lines
      .filter(_.feature == EXON)
      .map(line => Exon(line.start, line.stop))
      .toSeq

    val introns = lines
      .filter(_.feature == INTRON)
      .map(line => Intron(line.start, line.stop))
      .toSeq

    val geneData = lines
      .find(_.feature == GENE)
      .getOrElse(throw new IllegalArgumentException("Parse error: no gene data found"))

    val splicings = lines
      .filter(_.feature == SPLICING)
      .map(line => toSplicing(line, exons, introns))
      .toSeq

    Gene(geneId, geneData.start, geneData.stop, splicings)
  }

  def toSplicing(line: GffLine, exons: Seq[Exon], introns: Seq[Intron]): Splicing = {
    val children = (exons ++ introns).sortBy(_.start)

    val splicingId = line.attributes match {
      case Left(id) => id
      case _ => throw new IllegalArgumentException("Parse error: unable to parse splicing ID")
    }

    Splicing(splicingId, children)
  }


}
