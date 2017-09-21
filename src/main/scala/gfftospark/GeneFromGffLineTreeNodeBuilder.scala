package gfftospark

trait GeneFromGffLineTreeNodeBuilder {

  def toGene(gffLineTreeNode: GffLineTreeNode): Either[Iterable[String], Gene]

  def toGenes(gffLineTreeNodes: Seq[GffLineTreeNode])
    : Either[Iterable[String], Seq[Gene]] =
    sequence(gffLineTreeNodes.map(toGene))

  protected def sequence[B](
      s: Seq[Either[Iterable[String], B]]): Either[Iterable[String], Seq[B]] =
    s.foldRight(Right(Nil): Either[Iterable[String], List[B]]) { (e, acc) =>
      (e, acc) match {
        case (Left(leftErrors), Left(rightErrors)) =>
          Left(leftErrors ++ rightErrors)
        case (Left(leftErrors), Right(_))  => Left(leftErrors)
        case (Right(_), Left(rightErrors)) => Left(rightErrors)
        case (Right(left), Right(right))   => Right(left +: right)
      }
    }

}

class StandardGeneFromGffLineTreeNodeBuilder
    extends GeneFromGffLineTreeNodeBuilder {

  override def toGene(gffLineTreeNode: GffLineTreeNode) = {
    val idAsEither: Either[Iterable[String], String] =
      gffLineTreeNode.id match {
        case Some(id) => Right(id)
        case None =>
          Left(Seq(s"Gene ${gffLineTreeNode.gffLine} should have an id!"))
      }

    val splicingsAsEither: Either[Iterable[String], Seq[Splicing]] =
      sequence(gffLineTreeNode.children.map(toSplicing))

    idAsEither.right.flatMap(id => {
      splicingsAsEither.right.map(splicings => {
        Gene(id,
             gffLineTreeNode.gffLine.start,
             gffLineTreeNode.gffLine.stop,
             splicings,
             gffLineTreeNode.gffLine.attributesAsMap)
      })
    })
  }

  def toSplicing(
      gffLineTreeNode: GffLineTreeNode): Either[Iterable[String], Splicing] =
    gffLineTreeNode.id match {
      case Some(id) =>
        Right(
          Splicing(id,
                   gffLineTreeNode.gffLine.start,
                   gffLineTreeNode.gffLine.stop,
                   gffLineTreeNode.children.map(toExon),
                   gffLineTreeNode.gffLine.attributesAsMap))
      case None =>
        Left(Seq(s"Splicing ${gffLineTreeNode.gffLine} should have an id!"))
    }

  def toExon(gffLineTreeNode: GffLineTreeNode): Exon =
    Exon(gffLineTreeNode.gffLine.start,
         gffLineTreeNode.gffLine.stop,
         gffLineTreeNode.gffLine.attributesAsMap)
}
