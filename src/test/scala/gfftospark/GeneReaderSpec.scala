package gfftospark

import org.scalatest.LoneElement

class GeneReaderSpec extends UnitSpec with LoneElement {
  "FpoaeGeneReader" should "be able to parse a GFF file with two genes" in {
    val file =
      """Chr1	AUGUSTUS	intron	1	144	0.99	-	.	transcript_id "FPOA_00001.t1"; gene_id "FPOA_00001";
        |Chr1	AUGUSTUS	gene	1	2803	0.8	-	.	FPOA_00001
        |Chr1	AUGUSTUS	transcript	1	2803	0.8	-	.	FPOA_00001.t1
        |Chr1	AUGUSTUS	CDS	145	2803	0.8	-	0	transcript_id "FPOA_00001.t1"; gene_id "FPOA_00001";
        |Chr1	AUGUSTUS	start_codon	2801	2803	.	-	0	transcript_id "FPOA_00001.t1"; gene_id "FPOA_00001";
        |Chr1	AUGUSTUS	gene	6783	8255	0.52	+	.	FPOA_00002
        |Chr1	AUGUSTUS	transcript	6783	8255	0.52	+	.	FPOA_00002.t1
        |Chr1	AUGUSTUS	start_codon	6783	6785	.	+	0	transcript_id "FPOA_00002.t1"; gene_id "FPOA_00002";
        |Chr1	AUGUSTUS	CDS	6783	6906	0.79	+	0	transcript_id "FPOA_00002.t1"; gene_id "FPOA_00002";
        |Chr1	AUGUSTUS	intron	6907	6999	0.79	+	.	transcript_id "FPOA_00002.t1"; gene_id "FPOA_00002";
        |Chr1	AUGUSTUS	CDS	7000	7236	0.52	+	2	transcript_id "FPOA_00002.t1"; gene_id "FPOA_00002";
        |Chr1	AUGUSTUS	intron	7237	7395	0.63	+	.	transcript_id "FPOA_00002.t1"; gene_id "FPOA_00002";
        |Chr1	AUGUSTUS	CDS	7396	8255	0.64	+	2	transcript_id "FPOA_00002.t1"; gene_id "FPOA_00002";
        |Chr1	AUGUSTUS	stop_codon	8253	8255	.	+	0	transcript_id "FPOA_00002.t1"; gene_id "FPOA_00002";
        |Chr1	AUGUSTUS	stop_codon	8492	8494	.	-	0	transcript_id "FPOA_00003.t1"; gene_id "FPOA_00003";
        |"""".stripMargin.lines.toSeq

    val lines = file.map(GffParser.parseLineOrHeader).collect { case Right(g: GffLine) => g }

    val gffLineTreeNodeWriters = FPoaeGeneReader.getGenes(lines, FPoaeGeneReader.toGffLines(lines))

    val genes = gffLineTreeNodeWriters.flatMap { gffLineTreeNodeWriter =>
      gffLineTreeNodeWriter.value.map(_.domainObject).toSeq
    }.toSeq

    forExactly(1, genes) { gene =>
      gene.id shouldBe "FPOA_00001"
      gene.splicings.loneElement.mRNA.size shouldBe 1
    }

    forExactly(1, genes) { gene =>
      gene.id shouldBe "FPOA_00002"

      gene.splicings.loneElement.mRNA.size shouldBe 3
      // TODO why are there no exons being read..?
      gene.splicings.loneElement.children.size shouldBe 5
    }

    genes.size shouldBe 2
  }
}
