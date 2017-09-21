package corwur.neo4j

import corwur.genereader._
import corwur.neo4j.Neo4JUtils._
import org.neo4j.driver.v1._

object GenesToNeo4j {
  def insertSequence(sequence: DnaSequence)(implicit session: Session) = {
    println(s"Processing sequence ${sequence.name} with nr of genes ${sequence.genes.size}")

    val genesWithNodeId = inTransaction(session) { implicit tx =>
      sequence.genes.zipWithIndex.map { case (gene, index) =>
        println(s"Creating node for gene ${index} of ${sequence.genes.size}")
        val geneNodeId = insertGene(gene, sequence.name)
        (gene, geneNodeId)
      }
    }

    println("Creating relationships between genes")
    inTransaction(session) { implicit tx =>
      // Link genes in order
      val sortedGenesWithNodeId = genesWithNodeId.sortBy(_._1.start)
      val nodeIds = sortedGenesWithNodeId.map(_._2)
      createOrderedRelationships(nodeIds, GffRelationshipTypes.order)
    }
  }

  def insertGene(gene: Gene, sequenceName: String)(implicit tx: Transaction): NodeId = {
    val geneNodeId = createNode(NodeLabels.gene, Map(
      "sequence" -> sequenceName,
      "start" -> gene.start.toString,
      "end" -> gene.stop.toString,
      "geneID" -> gene.id
    ))
    // TODO organism property

    gene.splicings.foreach(insertTranscript(_, geneNodeId, gene))

    geneNodeId
  }

  def insertTranscript(transcript: Splicing, geneNodeId: NodeId, gene: Gene)(implicit tx: Transaction): Unit = {
    // Create transcript node
    val transcriptNode = createNode(NodeLabels.splicing, properties = Map(
      "start" -> transcript.start.toString,
      "end" -> transcript.stop.toString,
      "geneID" -> gene.id
    ))

    // Link gene to transcripts
    createRelationship(transcriptNode, geneNodeId, GffRelationshipTypes.transcribes)

    // Create nodes for exons and introns. They are already in order
    val transcriptElementNodes = transcript.children.map { element =>
      val label = element match {
        case Exon(_, _) => NodeLabels.cds // TODO should it be exon?
        case Intron(_, _) => NodeLabels.intron
      }

      val nodeId = createNode(label, properties = Map(
        "start" -> element.start.toString,
        "end" -> element.stop.toString,
        "geneID" -> gene.id
      ))

      (element, nodeId)
    }

    // Exons and introns are linked
    createOrderedRelationships(transcriptElementNodes.map(_._2), GffRelationshipTypes.links)

    // Link exons as mRNA
    val exonNodes = transcriptElementNodes.collect { case (Exon(_, _), node) => node }
    val intronNodes = transcriptElementNodes.collect { case (Intron(_, _), node) => node }

    createOrderedRelationships(exonNodes, GffRelationshipTypes.mRna)

    // Exons code a transcript
    exonNodes.foreach(createRelationship(_, transcriptNode, GffRelationshipTypes.codes))

    // Introns are 'in' a transcript
    intronNodes.foreach(createRelationship(_, transcriptNode, GffRelationshipTypes.in))
  }
}

object NodeLabels
{
  val gene = "gene"
  val splicing = "splicing"
  val cds = "cds"
  val intron = "intron"
}
