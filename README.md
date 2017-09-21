# GFF Import to Neo4J

## About
Utility to read GFF files into a Neo4J graph database.

The parser supports various GFF file formats and is very robust to variations, thanks to the use of parser combinator technology.

Creates the following graph database entities:

* Nodes of type `gene`, `splicing`, `exon`
* Relationships:
  * `order` between genes of a sequence in order of start position
  * `transcribes` between gene and its splicings
  * `links` between exons and introns in order of start position
  * `mRNA` between exons in a splicing
  * `codes` between exons and their splicing
  * `in` between introns and their splicing

Written in Scala

## Dependencies
* SBT
* Neo4J

## Development
### Building
* Run `sbt assembly`
* This produces `gfftoneo4j-assembly-0.1-SNAPSHOT.jar`

### Set up Neo4J
* Start Neo4j at some folder
* Browse to the shown Neo4J URL
* Use the default password 'neo4j' and change it to 'test'


## Running
```
scala target/scala-2.11/gfftoneo4j-assembly-0.1-SNAPSHOT.jar 
	-f pathToFile 
	-t gffType 
	-u neo4jUrl
```

* Path to GFF file
* GFF file format (`fpoae` | `gcf`)
* Neo4J server address, eg `bolt://127.0.0.1:7687`
