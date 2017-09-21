name := "gfftoneo4j"

scalaVersion := "2.11.8"

scalacOptions ++= Seq("-deprecation")

resolvers += Resolver.sonatypeRepo("releases")

// grading libraries
libraryDependencies += "junit" % "junit" % "4.10" % "test"
libraryDependencies += "org.neo4j" % "neo4j" % "3.3.0-alpha02"
// https://mvnrepository.com/artifact/org.neo4j/neo4j-lucene-index
libraryDependencies += "org.neo4j" % "neo4j-lucene-index" % "3.3.0-alpha02"
libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.6"
libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.1"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"
libraryDependencies += "com.github.scopt" %% "scopt" % "3.5.0"
libraryDependencies += "org.neo4j.driver" % "neo4j-java-driver" % "1.4.0"

mainClass in assembly := Some("corwur.Application")

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "services", "org.apache.hadoop.fs.FileSystem") => MergeStrategy.filterDistinctLines
  case PathList("META-INF", "services", "org.neo4j.kernel.extension.KernelExtensionFactory") => MergeStrategy.filterDistinctLines
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case PathList("META-INF", ps @ _*) => MergeStrategy.first
  case x => MergeStrategy.first
}
