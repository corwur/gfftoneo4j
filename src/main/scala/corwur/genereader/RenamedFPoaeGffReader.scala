package corwur.genereader

import corwur.gffparser.GffLine

object RenamedFPoaeGffReader extends GenericGffReader {

  val GENE = "gene"
  val EXON = "CDS"
  val INTRON = "intron"
  val START_CODON = "start_codon"
  val STOP_CODON = "stop_codon"
  val SPLICING = "transcript"
  val SPLICING_ID_KEY = "transcript_id"
  val GENE_ID_KEY = "gene_id"
  val ID_KEYS_BY_FEATURE = Map(GENE -> GENE_ID_KEY, SPLICING -> SPLICING_ID_KEY)

  override def getParentInfo(gffLineTreeNode: GffLineTreeNode,
                             gffLinesRepository: GffLinesRepository) = {
    gffLineTreeNode.gffLine.feature match {
      case EXON =>
        getParentSplicing(gffLineTreeNode, gffLinesRepository)
      case SPLICING =>
        getParentGene(gffLineTreeNode, gffLinesRepository)
      case GENE =>
        HasNoParent
      case _ =>
        ParentInfoNotFound(s"Unexpected feature ${gffLineTreeNode.gffLine.feature} while looking for a parent")
    }
  }

  private def getParentSplicing(gffLineTreeNode: GffLineTreeNode,
                                gffLinesRepository: GffLinesRepository) =
    getParentId(gffLineTreeNode.gffLine, SPLICING_ID_KEY) match {
      case Some(splicingId) =>
        gffLinesRepository.gffLinesById.get(splicingId) match {
          case Some(parent) => ParentFound(splicingId, parent)
          case None =>
            ParentInfoNotFound(
              s"Splicing with id ${splicingId} could not be found")
        }
      case None =>
        ParentInfoNotFound(
          s"Exon ${gffLineTreeNode.gffLine} does not contain attribute ${SPLICING_ID_KEY}")
    }

  private def getParentGene(gffLineTreeNode: GffLineTreeNode,
                            gffLinesRepository: GffLinesRepository) = {

    def getGeneIdFromExonGffLine(
        exonGffLineTreeNode: GffLineTreeNode): Option[String] =
      getParentId(exonGffLineTreeNode.gffLine, GENE_ID_KEY)

    val geneIds =
      gffLineTreeNode.children.map(getGeneIdFromExonGffLine).distinct
    geneIds match {
      case Seq(Some(geneId)) =>
        gffLinesRepository.gffLinesById.get(geneId) match {
          case Some(parent) => ParentFound(geneId, parent)
          case None =>
            ParentInfoNotFound(s"Gene with id ${geneId} could not be found")
        }
      case _ =>
        ParentInfoNotFound(
          s"Found more/less than a single gene id for a single splicing: ${geneIds}")

    }
  }

  private def getParentId(line: GffLine, parentIdKey: String): Option[String] =
    line.attributes match {
      case Left(_) => None
      case Right(attributes) =>
        attributes.get(parentIdKey)
    }

  override def isExon(gffLine: GffLine) = gffLine.feature == EXON

  override def getId(gffLine: GffLine) =
    gffLine.attributes match {
      case Left(id) => Some(id)
      case Right(attributes) =>
        for {
          idKey <- ID_KEYS_BY_FEATURE.get(gffLine.feature)
          id <- attributes.get(idKey)
        } yield id
    }
}
