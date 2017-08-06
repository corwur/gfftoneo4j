package gfftospark

import org.scalatest.EitherValues

class FeatureIdReaderSpec extends UnitSpec with EitherValues {
  "FeatureIdReader" should "read the whole attribute column as feature ID" in {
    val l = parseLine("Chr1\tAUGUSTUS\tgene\t1\t2803\t0.8\t-\t.\tFPOA_00001")

    val reader = FeatureIdReader.singleAttribute
    val result = reader.apply(l)
    result should contain("FPOA_00001")
  }

  it should "not be able to read the feature ID when the attribute column contains key-value pairs" in {
    val l = parseLine("Chr1\tAUGUSTUS\tstart_codon\t2801\t2803\t.\t-\t0\ttranscript_id \"FPOA_00001.t1\"; gene_id \"FPOA_00001\"")

    val reader = FeatureIdReader.singleAttribute
    val result = reader.apply(l)
    result shouldBe empty
  }

  it should "be able to read an attribute by key" in {
    val l = parseLine("Chr1\tAUGUSTUS\tstart_codon\t2801\t2803\t.\t-\t0\ttranscript_id \"FPOA_00001.t1\"; gene_id \"FPOA_00001\"")

    val reader = FeatureIdReader.attributeWithKey("gene_id")
    val result = reader.apply(l)
    result should contain("FPOA_00001")
  }

  it should "be able to read an attribute from a list of key names" in {
    val l = parseLine("Chr1\tAUGUSTUS\tstart_codon\t2801\t2803\t.\t-\t0\ttranscript_id \"FPOA_00001.t1\"; gene_id \"FPOA_00001\"")

    val reader = FeatureIdReader.attributesFromList("transcript_id", "gene_id")
    val result = reader.apply(l)
    result should contain("FPOA_00001.t1")
  }

  it should "be able to read an attribute differently depending on the feature name" in {
    val l1 = parseLine("Chr1\tAUGUSTUS\tgene\t1\t2803\t0.8\t-\t.\tFPOA_00001")
    val l2 = parseLine("Chr1\tAUGUSTUS\tstart_codon\t2801\t2803\t.\t-\t0\ttranscript_id \"FPOA_00001.t1\"; gene_id \"FPOA_00001\"")

    val reader = FeatureIdReader.byFeatureType {
      case "gene" => FeatureIdReader.singleAttribute
      case _ => FeatureIdReader.attributeWithKey("transcript_id")
    }

    val results = Seq(l1, l2).flatMap(reader.apply)
    results shouldBe Seq("FPOA_00001", "FPOA_00001.t1")
  }

  def parseLine(l: String): GffLine = {
    val parsedLineOrHeader = GffParser.parseLineOrHeader(l).right.value

    parsedLineOrHeader match {
      case gffLine: GffLine => gffLine
      case _ => fail("Input was a header, not a line")
    }
  }
}
