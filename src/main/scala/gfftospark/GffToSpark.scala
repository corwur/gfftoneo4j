package gfftospark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.util.{Failure, Success, Try}

object GffToSpark extends GffToSpark {

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("GffToSpark")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  /** Main function */
  def main(args: Array[String]): Unit = {
    try {
      val lines: RDD[String] = sc.textFile(args(0))

      // Parse lines into a meaningful data structure (GffLine)
      val gffLines: RDD[GffLine] = lines.map { l =>
        Try {
          GffParser.parseLine(l)
        }.transform[GffLine](Success.apply, e => Failure(new IllegalArgumentException(s"Parsefout in regel '${l}'", e)))
          .get
      }

      // Filter out transcripts and other stuff TODO find out what to do with this
        .filter(l => l.feature != "similarity")

      // Group the data by gene
      val linesPerGene: RDD[(GeneId, Iterable[GffLine])] = gffLines.groupBy(getKey)

      // Convert the data into a Gene structure
      val genes = linesPerGene.map((toGene _).tupled)

      // Collect results
      val results: Array[Gene] = genes.collect() //.take(2)

      println(results.map { gene =>
        s"Gene: ${gene.id}\n" + gene.transcripts.map(_.toString).map("\t" + _).mkString("\n")
      }.mkString("\n "))
      println(s"Number of genes: ${results.length}")
    }
    finally {
      sc.stop()
    }
  }

  def getKey(l: GffLine): GeneId =
    (l.feature, l.attributes) match {
      case ("gene", Left(geneId)) => geneId
      case ("transcript", Left(transcriptId)) => transcriptId.split('.').head
      case ("CDS" | "transcript" | "intron" | "start_codon" | "stop_codon", Right(attributes)) =>
        attributes.getOrElse("gene_id", throw new IllegalArgumentException("Parse error: gene has attribute-pairs instead of gene name"))

      case _ => throw new IllegalArgumentException(s"Parse error (${l.feature}, ${l.attributes}: gene has attribute-pairs instead of gene name")
    }

  def toGene(geneId: GeneId, lines: Iterable[GffLine]): Gene = {
    val codingSequences = lines
      .filter(_.feature == "CDS")
      .map(line => CodingSequence(line.start, line.stop))
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

  def toTranscript(line: GffLine, codingSequences: Seq[CodingSequence], introns: Seq[Intron]): Transcript = {
    val children = (codingSequences ++ introns).sortBy(_.start)

    val transcriptId = line.attributes match {
      case Left(id) => id
      case _ => throw new IllegalArgumentException("Parse error: unable to parse transcript ID")
    }

    Transcript(transcriptId, children)
  }
}

/** The parsing and kmeans methods */
class GffToSpark extends Serializable {

}







