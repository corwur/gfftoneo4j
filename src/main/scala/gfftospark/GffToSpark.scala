package gfftospark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.util.{Failure, Success, Try}

object GffToSpark {

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("GffToSpark")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  /** Main function */
  def main(args: Array[String]): Unit = {
    try {
      // Read lines
      val lines: RDD[String] = sc.textFile(args(0))

      // Parse lines into a meaningful data structure (GffLine)
      val gffLines: RDD[GffLine] = lines
        .map { l =>
          Try {
            GffParser.parseLineOrHeader(l)
          }.transform[GffLineOrHeader](Success.apply, e => Failure(new IllegalArgumentException(s"Parsefout in regel '${l}'", e)))
            .get
        }
        .collect {
          // Discard headers:
          case l@GffLine(_, _, _, _, _, _, _, _, _) => l
        }

//    val gffLineTreeNodeWriters = FPoaeGeneReader.getGenes(gffLines, FPoaeGeneReader.toGffLines(gffLines)).collect()
      val gffLineTreeNodeWriters = GcfGeneReader.getGenes(gffLines, GcfGeneReader.toGffLines(gffLines)).collect()

      val genes = gffLineTreeNodeWriters.flatMap { gffLineTreeNodeWriter =>
        gffLineTreeNodeWriter.value.map(_.domainObject).toSeq
      }

      // Collect results
      println(genes.map { gene =>
        s"Gene: ${gene.id}\n" + gene.splicings.map(_.toString).map("\t" + _).mkString("\n")
      }.mkString("\n "))

      println(s"Number of genes: ${genes.length}")
    }
    finally {
      sc.stop()
    }
  }

}

