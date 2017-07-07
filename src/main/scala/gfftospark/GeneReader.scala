package gfftospark

/**
  * Functions for reading Gene objects and their Transcripts from a series of GFFLines
  */
object GeneReader {
  /**
    * Get the ID of the Gene this GFFLine belongs to
    *
    * @param line
    */
  def getGeneId(line: GffLine): GeneId = {
    (line.feature, line.attributes) match {
      case ("gene", Left(geneId)) => geneId
      case ("transcript", Left(transcriptId)) => transcriptId.split('.').head
      case ("CDS" | "transcript" | "intron" | "start_codon" | "stop_codon", Right(attributes)) =>
        attributes.getOrElse("gene_id", throw new IllegalArgumentException("Parse error: gene has attribute-pairs instead of gene name"))

      case _ => throw new IllegalArgumentException(s"Parse error (${line.feature}, ${line.attributes}: gene has attribute-pairs instead of gene name")
    }

  }

  def linesToGene(geneId: GeneId, lines: Iterable[GffLine]): Gene = {
    val codingSequences = lines
      .filter(_.feature == "CDS")
      .map(line => Exon(line.start, line.stop))
      .toSeq

    val introns = lines
      .filter(_.feature == "intron")
      .map(line => Intron(line.start, line.stop))
      .toSeq

    val geneData = lines
      .find(_.feature == "gene")
      .getOrElse(throw new IllegalArgumentException("Parse error: no gene data found"))

    val transcripts = lines
      .filter(_.feature == "transcript")
      .map(line => toTranscript(line, codingSequences, introns))
      .toSeq

    Gene(geneId, geneData.start, geneData.stop, transcripts)
  }

  def toTranscript(line: GffLine, codingSequences: Seq[Exon], introns: Seq[Intron]): Splicing = {
    val children = (codingSequences ++ introns).sortBy(_.start)

    val transcriptId = line.attributes match {
      case Left(id) => id
      case _ => throw new IllegalArgumentException("Parse error: unable to parse transcript ID")
    }

    Splicing(transcriptId, children)
  }


}
