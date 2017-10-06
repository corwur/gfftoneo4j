package corwur.genereader

import corwur.gffparser.GffLine

object GcaFPoaeGffReader extends GenericGffReader {

  val GENE = "gene"
  val EXON = "exon"
  val INTRON = "intron"
  val SPLICING = "mRNA"
  val ID_KEY = "ID"
  val PARENT_ID_KEY = "Parent"

  override def getParentInfo(gffLineTreeNode: GffLineTreeNode, gffLinesRepository: GffLinesRepository) =
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

  override def isExon(gffLine: GffLine) = gffLine.feature == EXON

  override def getId(gffLine: GffLine) =
    gffLine.attributes match {
      case Left(id) => Some(id)
      case Right(attributes) => attributes.get(ID_KEY)
    }

  private def getParentSplicing(gffLineTreeNode: GffLineTreeNode, gffLinesRepository: GffLinesRepository) = {
    getParentInfo(gffLineTreeNode.gffLine, Seq(SPLICING), gffLinesRepository)
  }

  private def getParentGene(gffLineTreeNode: GffLineTreeNode, gffLinesRepository: GffLinesRepository) =
    getParentInfo(gffLineTreeNode.gffLine, Seq(GENE), gffLinesRepository)

  def getParentInfoOpt(gffLine: GffLine, gffLinesRepository: GffLinesRepository): Option[ParentFound] =
    for {
      parentId <- getParentId(gffLine)
      parent <- gffLinesRepository.gffLinesById.get(parentId)
    } yield ParentFound(parentId, parent)

  private def getParentInfo(gffLine: GffLine, parentFeatures: Seq[String], gffLinesRepository: GffLinesRepository): ParentInfo =
    getParentInfoOpt(gffLine, gffLinesRepository) match {
      case Some(parentInfo) =>
        if (parentFeatures contains parentInfo.parent.feature) {
          parentInfo
        } else {
          getParentInfo(parentInfo.parent, parentFeatures, gffLinesRepository)
        }
      case None =>
        ParentInfoNotFound(s"Could not find parent of ${gffLine} while looking for a parent with features ${parentFeatures.mkString(", ")}.")
    }

  def getParentId(line: GffLine): Option[String] =
    line.getAttribute(PARENT_ID_KEY)
}
