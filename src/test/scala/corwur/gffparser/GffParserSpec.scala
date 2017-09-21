package corwur.gffparser

import org.scalatest._

abstract class UnitSpec extends FlatSpec with Matchers with
  OptionValues with Inside with Inspectors

class GffParserSpec extends UnitSpec {
  "The GFF line parser" should "be able to parse a line with dots for empty fields" in {
    val line = "Chr3   giemsa heterochromatin  4500000 6000000 . . .   Band 3q12.1"

    parsedLineShouldMatch(line) { l =>
      l.seqname shouldBe "Chr3"
      l.source shouldBe "giemsa"
      l.feature shouldBe "heterochromatin"
      l.start shouldBe 4500000L
      l.stop shouldBe 6000000L
      l.score shouldBe empty
      l.strand shouldBe empty
      l.frame shouldBe empty

      inside(l.attributes) {
        case Right(attributes) =>
          attributes should contain ("Band" -> "3q12.1")
      }
    }
  }

  it should "be able to parse a line with semicolon-separated attributes and with or without quotes" in {
    val line = s"""|3 transcribed_unprocessed_pseudogene  gene        11869 14409 . - . klaas devries; gene_id "ENSG00000223972"; henk de.vries; Note "Clone Y74C9A; Genbank AC024206"; gene_name "DDX11L1"; gene_source "havana"; gene_biotype "transcribed_unprocessed_pseudogene";"""

    parsedLineShouldMatch(line) { l =>
      inside(l.attributes) {
        case Right(attributes) =>
          attributes should contain ("klaas" -> "devries")
          attributes should contain ("henk" -> "de.vries")
          attributes should contain ("gene_id" -> "ENSG00000223972")
          attributes should contain ("Note" -> "Clone Y74C9A; Genbank AC024206")
      }
    }
  }

  it should "be able to parse a diverse range of GFF-compatible lines" in {
    val linesToParse =
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
        |NC_026474.1	RefSeq	tRNA	571898	572014	.	+	.	ID=rna231;Parent=gene231;Dbxref=GeneID:23560927;gbkey=tRNA;product=tRNA-Gly
        |NC_026474.1	RefSeq	region	1	11697295	.	+	.	ID=id0;Dbxref=taxon:229533;Name=1;chromosome=1;gb-synonym=Gibberella zeae PH-1;gbkey=Src;genome=chromosome;mol_type=genomic DNA;old-name=Gibberella zeae PH-1;strain=PH-1%3B NRRL 31084
      """.stripMargin

    val lines = linesToParse.split("\n") map (_.trim) filter (_.nonEmpty)

    val parsedLines = lines map GffParser.parseLineOrHeader

    forAll(parsedLines) { l =>
      l should matchPattern { case Right(_ : GffLine) => }
    }
  }

  it should "give an error when there are not enough fields" in {
    val parsedLine = GffParser.parseLineOrHeader("X Ensembl")

    inside(parsedLine) {
      case Left(msg) => msg should include ("feature")
    }
  }

  private def parsedLineShouldMatch(line: String)(f: GffLine => Unit): Unit = {
    val parsedLine = GffParser.parseLineOrHeader(line)

    inside(parsedLine) { case Right(l : GffLine) => f(l) }
  }
}
