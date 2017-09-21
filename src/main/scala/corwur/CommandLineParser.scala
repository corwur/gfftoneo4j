package corwur

import corwur.genereader.GeneReaders
import scopt.OptionParser

object CommandLineParser {

  val parser = new OptionParser[CommandLineArgs]("gfftoneo4j") {
    head("gfftoneo4j", "0.1")

    opt[String]('f', "file")
      .required()
      .valueName("<file>")
      .action((x, c) => c.copy(file = x))
      .text("Path to GFF file")

    opt[String]('t', "type")
      .required()
      .valueName("<type>")
      .action((x, c) => c.copy(format = x))
      .text(s"GFF file type. Possible values: ${GeneReaders.formats.mkString(",")}")

    opt[String]('u', "url")
      .valueName("<url>")
      .action((x, c) => c.copy(neo4jUrl = x))
      .text("Neo4J server URL")
  }

  case class CommandLineArgs(
                              file: String,
                              format: String = "fpoae",
                              neo4jUrl: String = "bolt://127.0.0.1:7687",
                              neo4jUsername: String = "neo4j",
                              neo4jPassword: String = "test"
                            )

}
