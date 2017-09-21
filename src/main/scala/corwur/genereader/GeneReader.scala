package corwur.genereader

import corwur.gffparser.GffLine
import FeatureIdReader.{FeatureIdReader, attributeWithKey, singleAttribute}

/**
  * Functions for reading Gene objects and their Transcripts from a series of GFFLines
  */
trait GeneReader {

  def isExon(gffLine: GffLine): Boolean

  val getId: FeatureIdReader

  def getParentSplicing(gffLineTreeNode: GffLineTreeNode[Exon], gffLinesRepository: GffLinesRepository): ParentInfo

  def getParentGene(gffLineTreeNode: GffLineTreeNode[Splicing], gffLinesRepository: GffLinesRepository): ParentInfo

  sealed trait ParentInfo extends Serializable

  final case class ParentInfoFound(parentId: String, parent: GffLine) extends ParentInfo {
    override def hashCode() = parentId.hashCode

    override def equals(obj: Any) =
      if (obj.isInstanceOf[ParentInfoFound]) {
        parentId equals obj.asInstanceOf[ParentInfoFound].parentId
      } else {
        false
      }
  }

  final case class ParentInfoNotFound(message: String) extends ParentInfo {
    override def hashCode(): Int = 0

    override def equals(obj: scala.Any): Boolean =
      obj.isInstanceOf[ParentInfoNotFound]
  }

  case class GffLineAndDomainObject[A](gffLine: GffLine, domainObject: A) extends Serializable

  case class GffLineTreeNode[A](gffLine: GffLine, domainObject: A, children: Seq[GffLine]) extends Serializable

  final case class GffLinesRepository(gffLinesById: Map[String, GffLine], gffLinesWithoutId: Seq[GffLine]) {
    def +(that: GffLinesRepository): GffLinesRepository =
      GffLinesRepository(this.gffLinesById ++ that.gffLinesById, this.gffLinesWithoutId ++ that.gffLinesWithoutId)

    def add(gffLine: GffLine): GffLinesRepository =
      getId(gffLine) match {
        case Some(id) => GffLinesRepository(gffLinesById + (id -> gffLine), gffLinesWithoutId)
        case None => GffLinesRepository(gffLinesById, gffLine +: gffLinesWithoutId)
      }
  }

  final object GffLinesRepository {
    def empty: GffLinesRepository = GffLinesRepository(Map.empty, Seq.empty)
  }

  def toGffLines(gffLines: Iterable[GffLine]): GffLinesRepository = {
    gffLines.aggregate(GffLinesRepository.empty)(
      (acc: GffLinesRepository, curr: GffLine) => acc add curr,
      (left: GffLinesRepository, right: GffLinesRepository) => left + right
    )
  }

  def getExons(gffLines: Iterable[GffLine]): Iterable[Writer[Option[GffLineTreeNode[Exon]]]] = {
    gffLines
      .filter(isExon)
      .map(gffLine => {
        GffLineTreeNode(gffLine, Exon(gffLine.start, gffLine.stop), Seq.empty)
      })
      .map(Option.apply)
      .map(Writer(_))
  }

  def groupByParents[A, B](
                            gffLineTreeNodes: Iterable[Writer[Option[GffLineTreeNode[A]]]],
                            getParentInfo: (GffLineTreeNode[A], GffLinesRepository) => ParentInfo,
                            toParentTreeNode: (ParentInfoFound, Seq[GffLineTreeNode[A]]) => GffLineTreeNode[B],
                            gffLinesRepository: GffLinesRepository): Iterable[Writer[Option[GffLineTreeNode[B]]]] = {

    type T = (ParentInfo, Iterable[Writer[Option[GffLineTreeNode[A]]]])

    def toParentTreeNodeWriter(t: T): Writer[Option[GffLineTreeNode[B]]] =
      t match {
        case (ParentInfoNotFound(message), gffLineTreeNodeWriters) =>
          val gffLineTreeNodeWritersSeq = gffLineTreeNodeWriters.toSeq
          val summary = s"Ignoring ${gffLineTreeNodeWriters.size} rows. Reason: ${message}"
          val otherMessages = gffLineTreeNodeWritersSeq.flatMap(_.logs)
          val allMessages = summary +: otherMessages
          Writer(allMessages, None)
        case (parentInfoFound@ParentInfoFound(_, _), gffLineTreeNodeWriters) =>
          val gffLineTreeNodeWritersSeq = gffLineTreeNodeWriters.toSeq
          val gffLineTreeNodes = gffLineTreeNodeWritersSeq.flatMap(_.value)
          val messages = gffLineTreeNodeWritersSeq.flatMap(_.logs)
          Writer(messages, Some(toParentTreeNode(parentInfoFound, gffLineTreeNodes)))
      }

    def getParentInfoOpt(w: Writer[Option[GffLineTreeNode[A]]]) =
      w.value.map(getParentInfo(_, gffLinesRepository)).getOrElse(ParentInfoNotFound("Could not find parent info, as something at a lower level went wrong..."))

    gffLineTreeNodes
      .groupBy(getParentInfoOpt)
      .map(toParentTreeNodeWriter)
  }

  def getSplicings(gffLines: Iterable[GffLine], gffLinesAlt: GffLinesRepository): Iterable[Writer[Option[GffLineTreeNode[Splicing]]]] =
    groupByParents[Exon, Splicing](
      getExons(gffLines),
      getParentSplicing,
      (parentInfo: ParentInfoFound, exonTreeNodes: Iterable[GffLineTreeNode[Exon]]) => {
        val exons = exonTreeNodes.map(_.domainObject).toSeq
        val exonGffLines = exonTreeNodes.map(_.gffLine).toSeq
        val splicing = Splicing(parentInfo.parentId, parentInfo.parent.start, parentInfo.parent.stop, exons)
        GffLineTreeNode(parentInfo.parent, splicing, exonGffLines)
      },
      gffLinesAlt)

  def getGenes(gffLines: Iterable[GffLine], gffLinesAlt: GffLinesRepository): Iterable[Writer[Option[GffLineTreeNode[Gene]]]] = {
    val splicings = getSplicings(gffLines, gffLinesAlt)
    groupByParents[Splicing, Gene](
      splicings,
      getParentGene,
      (parentInfo: ParentInfoFound, splicingTreeNodes: Iterable[GffLineTreeNode[Splicing]]) => {
        val splicings = splicingTreeNodes.map(_.domainObject).toSeq
        val splicingGffLines = splicingTreeNodes.map(_.gffLine).toSeq
        val gene = Gene(
          parentInfo.parent.seqname,
          parentInfo.parentId,
          parentInfo.parent.start,
          parentInfo.parent.stop,
          splicings)
        GffLineTreeNode(parentInfo.parent, gene, splicingGffLines)
      },
      gffLinesAlt)
  }
}

object GcfGeneReader extends GeneReader with Serializable {

  val GENE = "gene"
  val EXON = "exon"
  val INTRON = "intron"
  val START_CODON = "start_codon"
  val STOP_CODON = "stop_codon"
  val SPLICING = Seq("mRNA", "tRNA", "rRNA")
  val ID_KEY = "ID"
  val PARENT_ID_KEY = "Parent"

  override def isExon(gffLine: GffLine): Boolean = gffLine.feature == EXON

  override def getParentSplicing(gffLineTreeNode: GffLineTreeNode[Exon], gffLinesRepository: GffLinesRepository) =
    getParentInfo(gffLineTreeNode.gffLine, SPLICING, gffLinesRepository)

  override def getParentGene(gffLineTreeNode: GffLineTreeNode[Splicing], gffLinesRepository: GffLinesRepository) =
    getParentInfo(gffLineTreeNode.gffLine, Seq(GENE), gffLinesRepository)

  override val getId = singleAttribute orElse attributeWithKey(ID_KEY)

  def getParentInfoOpt(gffLine: GffLine, gffLinesRepository: GffLinesRepository): Option[ParentInfoFound] =
    for {
      parentId <- getParentId(gffLine)
      parent <- gffLinesRepository.gffLinesById.get(parentId)
    } yield ParentInfoFound(parentId, parent)

  def getParentInfo(gffLine: GffLine, parentFeatures: Seq[String], gffLinesRepository: GffLinesRepository): ParentInfo =
    getParentInfoOpt(gffLine, gffLinesRepository) match {
      case Some(parentInfo) =>
        if (parentFeatures contains parentInfo.parent.feature) {
          parentInfo
        } else {
          getParentInfo(parentInfo.parent, parentFeatures, gffLinesRepository)
        }
      case None =>
        ParentInfoNotFound(s"Could not find parent of ${gffLine} while looking for a parent.")
    }

  def getParentId(line: GffLine): Option[String] =
    line.getAttribute(PARENT_ID_KEY)
}

object FPoaeGeneReader extends GeneReader with Serializable {

  val GENE = "gene"
  val EXON = "CDS"
  val INTRON = "intron"
  val START_CODON = "start_codon"
  val STOP_CODON = "stop_codon"
  val SPLICING = "transcript"
  val SPLICING_ID_KEY = "transcript_id"
  val GENE_ID_KEY = "gene_id"
  val ID_KEYS_BY_FEATURE = Map(GENE -> GENE_ID_KEY, SPLICING -> SPLICING_ID_KEY)

  override def isExon(gffLine: GffLine): Boolean = gffLine.feature == EXON

  override def getParentSplicing(gffLineTreeNode: GffLineTreeNode[Exon], gffLinesRepository: GffLinesRepository) =
    getParentId(gffLineTreeNode.gffLine, SPLICING_ID_KEY) match {
      case Some(splicingId) =>
        gffLinesRepository.gffLinesById.get(splicingId) match {
          case Some(parent) => ParentInfoFound(splicingId, parent)
          case None => ParentInfoNotFound(s"Splicing with id ${splicingId} could not be found")
        }
      case None => ParentInfoNotFound(s"Exon ${gffLineTreeNode.gffLine} does not contain attribute ${SPLICING_ID_KEY}")
    }

  override def getParentGene(gffLineTreeNode: GffLineTreeNode[Splicing], gffLinesRepository: GffLinesRepository) = {

    def getGeneIdFromExonGffLine(exonGffLine: GffLine): Option[String] =
      getParentId(exonGffLine, GENE_ID_KEY)

    val geneIds = gffLineTreeNode.children.map(getGeneIdFromExonGffLine).distinct
    geneIds match {
      case Seq(Some(geneId)) =>
        gffLinesRepository.gffLinesById.get(geneId) match {
          case Some(parent) => ParentInfoFound(geneId, parent)
          case None => ParentInfoNotFound(s"Gene with id ${geneId} could not be found")
        }
      case _ => ParentInfoNotFound(s"Found more/less than a single gene id for a single splicing: ${geneIds}")

    }
  }

  // Both genes and transcripts have their ID as single attribute
  override val getId = singleAttribute

  def getParentId(line: GffLine, parentIdKey: String): Option[String] =
    line.getAttribute(parentIdKey)
}

object GeneReaders {
  val geneReadersById: Map[String, Iterable[GffLine] => Array[Gene]] = Map(
    "gcf" -> (gffLines => {
      val gffLineTreeNodeWriters = GcfGeneReader.getGenes(gffLines, GcfGeneReader.toGffLines(gffLines))

      val genes = gffLineTreeNodeWriters.flatMap { gffLineTreeNodeWriter =>
        gffLineTreeNodeWriter.value.map(_.domainObject).toSeq
      }

      genes.toArray
    }),
    "fpoae" -> (gffLines => {
      val gffLineTreeNodeWriters = FPoaeGeneReader.getGenes(gffLines, FPoaeGeneReader.toGffLines(gffLines))

      val genes = gffLineTreeNodeWriters.flatMap { gffLineTreeNodeWriter =>
        gffLineTreeNodeWriter.value.map(_.domainObject).toSeq
      }

      genes.toArray
    })
  )

  val formats = geneReadersById.keys
}