package corwur.genereader
import corwur.gffparser.GffLine

object GcaFujiGffReader extends GenericGffReader {

  val GENE = "gene"
  val EXON = "exon"
  val SPLICINGS = Seq("tRNA", "mRNA", "rRNA")
  val ID_KEY = "ID"
  val PARENT_ID_KEY = "Parent"

  override def getParentInfo(gffLineTreeNode: GffLineTreeNode, gffLinesRepository: GffLinesRepository) =
    gffLineTreeNode.gffLine.feature match {
      case EXON =>
        getParent(gffLineTreeNode, gffLinesRepository)
      case f if SPLICINGS.contains(f) =>
        getParent(gffLineTreeNode, gffLinesRepository)
      case GENE =>
        HasNoParent
      case _ =>
        ParentInfoNotFound(s"Unexpected feature ${gffLineTreeNode.gffLine.feature} while looking for a parent of ${gffLineTreeNode.gffLine}")
    }

  override def isExon(gffLine: GffLine) = gffLine.feature == EXON

  override def getId(gffLine: GffLine) =
    gffLine.attributes match {
      case Left(id) => Some(id)
      case Right(attributes) => attributes.get(ID_KEY)
    }

  private def getParent(gffLineTreeNode: GffLineTreeNode, gffLinesRepository: GffLinesRepository) = {
    val parentFoundOpt = for {
      parentId <- getParentId(gffLineTreeNode.gffLine)
      parent <- gffLinesRepository.gffLinesById.get(parentId)
    } yield ParentFound(parentId, parent)
    parentFoundOpt.getOrElse(ParentInfoNotFound(s"Could not find parent of ${gffLineTreeNode.gffLine}"))
  }

  def getParentId(line: GffLine): Option[String] =
    line.getAttribute(PARENT_ID_KEY)
}
