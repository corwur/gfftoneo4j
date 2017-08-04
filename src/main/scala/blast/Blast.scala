package blast

import scala.io.Source

object Blast {

  def main(args: Array[String]): Unit = {
    val bufferedSource = Source.fromFile("/Users/wgr21717/Sandboxes/gffimport/fuji_single.csv")
    val lines = bufferedSource.getLines().toSeq.par
    bufferedSource.close

    val blastFileLines = lines.map(BlastFileParser.parseLine)
    BlastFileLineToNeo4jEdge.insertIntoNeo4j("geneID", "Name_attribute", blastFileLines)
  }

}
