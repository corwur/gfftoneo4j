package corwur.genereader

import corwur.gffparser.GffLine

import scala.collection.parallel.{ParIterable, ParMap, ParSeq}

sealed trait ParentInfo extends Serializable

final case class ParentFound(parentId: String, parent: GffLine) extends ParentInfo {
  override def hashCode() = parentId.hashCode

  override def equals(obj: Any) =
    if (obj.isInstanceOf[ParentFound]) {
      parentId equals obj.asInstanceOf[ParentFound].parentId
    } else {
      false
    }
}

final case class ParentInfoNotFound(message: String) extends ParentInfo

final case object HasNoParent extends ParentInfo

case class GffLineTreeNode(id: Option[String], gffLine: GffLine, children: Seq[GffLineTreeNode]) extends Serializable

final case class GffLinesRepository(gffLinesById: Map[String, GffLine], gffLinesWithoutId: Seq[GffLine]) {
  def +(that: GffLinesRepository): GffLinesRepository =
    GffLinesRepository(this.gffLinesById ++ that.gffLinesById, this.gffLinesWithoutId ++ that.gffLinesWithoutId)

  def add(idOpt: Option[String], gffLine: GffLine): GffLinesRepository =
    idOpt match {
      case Some(id) => GffLinesRepository(gffLinesById + (id -> gffLine), gffLinesWithoutId)
      case None => GffLinesRepository(gffLinesById, gffLine +: gffLinesWithoutId)
    }
}

final object GffLinesRepository {
  def empty: GffLinesRepository = GffLinesRepository(Map.empty, Seq.empty)
}

final case class GffLineTree(rootNodes: ParSeq[GffLineTreeNode], errors: ParIterable[String])

trait GenericGffReader {

  def getParentInfo(gffLineTreeNode: GffLineTreeNode, gffLinesRepository: GffLinesRepository): ParentInfo

  def isExon(gffLine: GffLine): Boolean

  def getId(gffLine: GffLine): Option[String]

  def buildTree(gffLines: ParSeq[GffLine]): GffLineTree = {
    val gffLinesRepository = createGffLinesRepository(gffLines)
    val exonTreeNodes = getAllExonTreeNodes(gffLines)
    val exonTreeNodesByParents = groupByParents(exonTreeNodes, gffLinesRepository)
    buildTree(exonTreeNodesByParents, gffLinesRepository)
  }

  private def createGffLinesRepository(gffLines: ParSeq[GffLine]): GffLinesRepository =
    gffLines.foldLeft(GffLinesRepository.empty) { (acc, curr) =>
      acc.add(getId(curr), curr)
    }

  private def getAllExonTreeNodes(gffLines: ParSeq[GffLine]): ParSeq[GffLineTreeNode] =
    gffLines.filter(isExon) map { gffLine =>
      GffLineTreeNode(None, gffLine, Seq.empty)
    }

  private def groupByParents(gffLineTreeNodes: ParSeq[GffLineTreeNode], gffLinesRepository: GffLinesRepository): ParMap[ParentInfo, ParSeq[GffLineTreeNode]] = {
    gffLineTreeNodes.groupBy(n => getParentInfo(n, gffLinesRepository))
  }

  private def buildTree(gffLineTreeNodesByParents: ParMap[ParentInfo, ParSeq[GffLineTreeNode]], gffLinesRepository: GffLinesRepository): GffLineTree = {

    lazy val nodesWithoutParent = gffLineTreeNodesByParents.getOrElse(HasNoParent, ParSeq.empty)

    lazy val nodesWithErrors = gffLineTreeNodesByParents.collect {
      case (key @ ParentInfoNotFound(_), value) => (key, value)
    }

    lazy val nodesThatRequireNoFurtherWork = nodesWithErrors ++ ParMap(HasNoParent -> nodesWithoutParent)

    val nodesThatRequireFurtherWork = gffLineTreeNodesByParents.toSeq.collect {
      case (ParentFound(parentId, parent), children) => GffLineTreeNode(Some(parentId), parent, children.seq)
    }

    if (nodesThatRequireFurtherWork.isEmpty) {
      // We're done!
      GffLineTree(nodesWithoutParent, nodesWithErrors.keys.map(_.message))
    } else {
      // We need to recurse again...
      val gffLineTreeNodesByParentsForNextIteration = nodesThatRequireNoFurtherWork ++ groupByParents(nodesThatRequireFurtherWork, gffLinesRepository)
      buildTree(gffLineTreeNodesByParentsForNextIteration, gffLinesRepository)
    }
  }

}
