package stackoverflow

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import annotation.tailrec
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

trait DnaThingy {
  val start: Long
  val end: Long
}

case class DnaSequence(genes: Seq[Gene])

case class Gene(id: String, start: Long, end: Long, transcripts: Seq[Transcript]) extends DnaThingy

case class CodingSequence(start: Long, end: Long) extends DnaThingy

case class Intron(start: Long, end: Long) extends DnaThingy

sealed trait Transcript {
  val mRNA: Seq[CodingSequence]
}

final case class TerminalCodingSequence(cds: CodingSequence) extends Transcript {
  override val mRNA: Seq[CodingSequence] = Seq(cds)
}

final case class Cons(cds: CodingSequence, intron: Intron, tail: Transcript) extends Transcript {
  override val mRNA: Seq[CodingSequence] = cds +: tail.mRNA
}

object GffToSpark extends GffToSpark {

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("StackOverflow")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  /** Main function */
  def main(args: Array[String]): Unit = {
    try {


      val lines: RDD[String] = sc.textFile("/Users/svr21640/projects/wageningen/corwur/f_poae_renamed.gff") //.sample(true, 0.05)

      val gffLines: RDD[GffToSpark.GffLine] = lines.map { l =>
        Try {
          parseLine(l)
        }.transform[GffLine](Success.apply, e => Failure(new IllegalArgumentException(s"Parsefout in regel '${l}'", e)))
          .get
      }

      println(gffLines.collect().take(10).mkString("\n "))

      //
      //
      //
      //      val raw = rawPostings(lines)
      //      val grouped = groupedPostings(raw)
      //      //    assert(grouped.count() == 2121822, "Incorrect number of vectors: " + grouped.count())
      //      println("Grouped: " + grouped.count())
      //      val scored = scoredPostings(grouped)
      //      println("Scored: " + scored.count())
      //      val vectors = vectorPostings(scored)
      //      println("Vectors: " + vectors.count())
      //      println(vectors.collect().take(5).mkString(","))
      //      //    assert(vectors.count() == 2121822, "Incorrect number of vectors: " + vectors.count())
      //
      //      val means = kmeans(sampleVectors(vectors), vectors, debug = true)
      //      val results = clusterResults(means, vectors)
      //      printResults(results)
    }
    finally {
      sc.stop()
    }
  }
}

/** The parsing and kmeans methods */
class GffToSpark extends Serializable {

  sealed trait Strand

  case object Forward extends Strand

  case object Reverse extends Strand

  object Strand {
    def fromString(s: String): Strand = {
      s match {
        case "+" => Forward
        case "-" => Reverse
        case _ => throw new IllegalArgumentException("Ongeldige strand, verwachtte + of -")
      }
    }
  }

  case class GffLine(seqname: String, source: String, feature: String, start: Long, stop: Long, score: Double, strand: Strand, frame: Long, attributes: Map[String, String])

  // TODO: replace with parser combinator
  def parseLine(line: String): GffLine = {
    val fields = line.split("\t")
    require(fields.length == 9)

    val attributesFieldColumnIndex = 8

    val attributePairs: Array[String] = fields(attributesFieldColumnIndex)
      .split(";")
      .map(_.trim) // Remove spaces after the ;
      .filter(_.nonEmpty) // Each attribute ends with a ;, so we remove the last empty one

    GffLine(
      fields(0),
      fields(1),
      fields(2),
      fields(3).toLong,
      fields(4).toLong,
      0L, // TODO fields(5).toDouble,
      Strand.fromString(fields(6)),
      0L, // TODO fields(7).toLong,
      attributePairs.map(parseKeyValuePair).toMap
    )
  }

  def parseKeyValuePair(kvp: String): (String, String) = {
    kvp.split(" ").toSeq match {
      case key +: tail =>
        val valueWithQuotes = tail.mkString(" ")
        println(s"Parsing key: ${key}, value: ${valueWithQuotes}")
        val value = valueWithQuotes.substring(1, valueWithQuotes.length - 1)

        (key, value)
      case _ =>
        throw new IllegalArgumentException("Ongeldig attribute: kvp")
    }
  }

  //
  //
  //  /** Group the questions and answers together */
  //  def groupedPostings(postings: RDD[Posting]): RDD[(Int, Iterable[(Posting, Posting)])] = {
  //    val questions = postings.filter(_.postingType == 1).map(p => (p.id, p))
  //    val answers = postings.flatMap {
  //      case p if p.postingType == 2 => p.parentId.map((_, p))
  //      case _ => None
  //    }
  //
  //    questions.join(answers).groupByKey()
  //  }
  //
  //
  //  /** Compute the maximum score for each posting */
  //  def scoredPostings(grouped: RDD[(Int, Iterable[(Posting, Posting)])]): RDD[(Posting, Int)] = {
  //
  //    def answerHighScore(as: Array[Posting]): Int = {
  //      var highScore = 0
  //      var i = 0
  //      while (i < as.length) {
  //        val score = as(i).score
  //        if (score > highScore)
  //          highScore = score
  //        i += 1
  //      }
  //      highScore
  //    }
  //
  //    // NOTE: dit kan shuffling doen vanwege de groupByKey
  //    //    grouped.flatMap(_._2).groupByKey().mapValues(_.toArray).mapValues(answerHighScore)
  //
  //    //    grouped.mapValues(_.maxBy(_._2.score))
  //
  //    grouped.mapValues { a =>
  //      a.groupBy(_._1)
  //      val answers = a.map(_._2)
  //      val highScore = answerHighScore(answers.toArray)
  //
  //      val question = a.map(_._1).head
  //
  //      (question, highScore)
  //    }.values
  //  }
  //
  //  /** Compute the vectors for the kmeans */
  //  def vectorPostings(scored: RDD[(Posting, Int)]): RDD[(Int, Int)] = {
  //    /** Return optional index of first language that occurs in `tags`. */
  //    def firstLangInTag(tag: Option[String], ls: List[String]): Option[Int] = {
  //      if (tag.isEmpty) None
  //      else if (ls.isEmpty) None
  //      else if (tag.get == ls.head) Some(0) // index: 0
  //      else {
  //        val tmp = firstLangInTag(tag, ls.tail)
  //        tmp match {
  //          case None => None
  //          case Some(i) => Some(i + 1) // index i in ls.tail => index i+1
  //        }
  //      }
  //    }
  //
  //    scored.map { case (q, score) => (firstLangInTag(q.tags, langs) map (_ * langSpread) getOrElse 0, score) }.cache
  //  }
  //
  //
  //  /** Sample the vectors */
  //  def sampleVectors(vectors: RDD[(Int, Int)]): Array[(Int, Int)] = {
  //
  //    assert(kmeansKernels % langs.length == 0, "kmeansKernels should be a multiple of the number of languages studied.")
  //    val perLang = kmeansKernels / langs.length
  //
  //    // http://en.wikipedia.org/wiki/Reservoir_sampling
  //    def reservoirSampling(lang: Int, iter: Iterator[Int], size: Int): Array[Int] = {
  //      val res = new Array[Int](size)
  //      val rnd = new util.Random(lang)
  //
  //      for (i <- 0 until size) {
  //        assert(iter.hasNext, s"iterator must have at least $size elements")
  //        res(i) = iter.next
  //      }
  //
  //      var i = size.toLong
  //      while (iter.hasNext) {
  //        val elt = iter.next
  //        val j = math.abs(rnd.nextLong) % i
  //        if (j < size)
  //          res(j.toInt) = elt
  //        i += 1
  //      }
  //
  //      res
  //    }
  //
  //    val res =
  //      if (langSpread < 500)
  //      // sample the space regardless of the language
  //        vectors.takeSample(false, kmeansKernels, 42)
  //      else
  //      // sample the space uniformly from each language partition
  //        vectors.groupByKey.flatMap({
  //          case (lang, vectors) => reservoirSampling(lang, vectors.toIterator, perLang).map((lang, _))
  //        }).collect()
  //
  //    assert(res.length == kmeansKernels, res.length)
  //    res
  //  }
  //
  //
  //  //
  //  //
  //  //  Kmeans method:
  //  //
  //  //
  //
  //  /** Main kmeans computation */
  //  @tailrec final def kmeans(means: Array[(Int, Int)], vectors: RDD[(Int, Int)], iter: Int = 1, debug: Boolean = false): Array[(Int, Int)] = {
  //    //    val meansWithIndex: Array[(Int, (Int, Int))] = means.zipWithIndex.map { case (mean, index) => (index, mean)}
  //
  //    //    val newMeans = means.clone()
  //
  //    val assigned: RDD[(Int, (Int, Int))] = vectors.map(p => (findClosest(p, means), p))
  //
  //    val newMeansRdd: RDD[(Int, (Int, Int))] = assigned
  //      .groupByKey()
  //      .mapValues(averageVectors)
  //      .sortByKey()
  //
  //    val newMeansCollected = newMeansRdd.collect()
  //
  //
  //
  //    //    val meansWithIndex = means.zipWithIndex.map { case (a, b) => (b, a)}
  //
  //    val newMeans = means.zipWithIndex.map { case (mean, index) =>
  //      newMeansCollected.find(_._1 == index).map(_._2).getOrElse(mean)
  //    }
  //    //    newMeansRdd.collect().foreach { case (cluster, newMean) => newMeans.update(cluster, newMean)}
  //
  //    //    val meansRdd = StackOverflow.sc.parallelize(means.zipWithIndex.map { case (a, b) => (b, a)})
  //
  //    //    val newMeansAll = meansRdd
  //    //      .leftOuterJoin(newMeansRdd)
  //    //      .mapValues { case (oldMean, newMeanOpt) => newMeanOpt.getOrElse(oldMean)}
  //    //
  //    //    val newMeans = newMeansAll.sortByKey().values.collect()
  //    //
  //    //      newMeansRdd
  //    //      .foreach { case (cluster, average) => {
  //    //        println("Updating average for cluster" + cluster)
  //    //        newMeans.update(cluster, average)
  //    //      }
  //    //      }
  //
  //    println(means.mkString(","))
  //    println(means.size)
  //    println(newMeans.mkString(","))
  //    println(newMeans.size)
  //
  //    val distance = euclideanDistance(means, newMeans)
  //
  //    if (debug) {
  //      println(
  //        s"""Iteration: $iter
  //           |  * current distance: $distance
  //           |  * desired distance: $kmeansEta
  //           |  * means:""".stripMargin)
  //      for (idx <- 0 until kmeansKernels)
  //        println(f"   ${means(idx).toString}%20s ==> ${newMeans(idx).toString}%20s  " +
  //          f"  distance: ${euclideanDistance(means(idx), newMeans(idx))}%8.0f")
  //    }
  //
  //    if (converged(distance))
  //      newMeans
  //    else if (iter < kmeansMaxIterations)
  //      kmeans(newMeans, vectors, iter + 1, debug)
  //    else {
  //      println("Reached max iterations!")
  //      newMeans
  //    }
  //  }
  //
  //  //
  //  //
  //  //  Kmeans utilities:
  //  //
  //  //
  //
  //  /** Decide whether the kmeans clustering converged */
  //  def converged(distance: Double) =
  //    distance < kmeansEta
  //
  //
  //  /** Return the euclidean distance between two points */
  //  def euclideanDistance(v1: (Int, Int), v2: (Int, Int)): Double = {
  //    val part1 = (v1._1 - v2._1).toDouble * (v1._1 - v2._1)
  //    val part2 = (v1._2 - v2._2).toDouble * (v1._2 - v2._2)
  //    part1 + part2
  //  }
  //
  //  /** Return the euclidean distance between two points */
  //  def euclideanDistance(a1: Array[(Int, Int)], a2: Array[(Int, Int)]): Double = {
  //    assert(a1.length == a2.length)
  //    var sum = 0d
  //    var idx = 0
  //    while (idx < a1.length) {
  //      sum += euclideanDistance(a1(idx), a2(idx))
  //      idx += 1
  //    }
  //    sum
  //  }
  //
  //  /** Return the closest point */
  //  def findClosest(p: (Int, Int), centers: Array[(Int, Int)]): Int = {
  //    var bestIndex = 0
  //    var closest = Double.PositiveInfinity
  //    for (i <- 0 until centers.length) {
  //      val tempDist = euclideanDistance(p, centers(i))
  //      if (tempDist < closest) {
  //        closest = tempDist
  //        bestIndex = i
  //      }
  //    }
  //    bestIndex
  //  }
  //
  //
  //  /** Average the vectors */
  //  def averageVectors(ps: Iterable[(Int, Int)]): (Int, Int) = {
  //    val iter = ps.iterator
  //    var count = 0
  //    var comp1: Long = 0
  //    var comp2: Long = 0
  //    while (iter.hasNext) {
  //      val item = iter.next
  //      comp1 += item._1
  //      comp2 += item._2
  //      count += 1
  //    }
  //    ((comp1 / count).toInt, (comp2 / count).toInt)
  //  }
  //
  //
  //  //
  //  //
  //  //  Displaying results:
  //  //
  //  //
  //  def clusterResults(means: Array[(Int, Int)], vectors: RDD[(Int, Int)]): Array[(String, Double, Int, Int)] = {
  //    val closest = vectors.map(p => (findClosest(p, means), p))
  //    val closestGrouped = closest.groupByKey()
  //
  //    val median = closestGrouped.mapValues { vs =>
  //      // vs is a list of (langIndex * langSpread, highScore)
  //      val langSizes = vs.groupBy(_._1).mapValues(_.size)
  //      val mostCommonLangAndNr = langSizes.maxBy(_._2)
  //      val mostCommonLangIndex = mostCommonLangAndNr._1 / langSpread
  //      val mostCommonLangNr = mostCommonLangAndNr._2
  //
  //      val langLabel: String = langs(mostCommonLangIndex)
  //
  //      // most common language in the cluster
  //      val langPercent: Double = mostCommonLangNr * 100.0 / vs.size
  //
  //      // percent of the questions in the most common language
  //      val clusterSize: Int = mostCommonLangNr
  //      val medianScore: Int = calcMedian(vs.map(_._2).toSeq).toInt
  //
  //      (langLabel, langPercent, clusterSize, medianScore)
  //    }
  //
  //    median.collect().map(_._2).sortBy(_._4)
  //  }
  //
  //  def calcMedian(s: Seq[Int])  =
  //  {
  //    val (lower, upper) = s.sortWith(_<_).splitAt(s.size / 2)
  //    if (s.size % 2 == 0) (lower.last + upper.head) / 2.0 else upper.head * 1.0
  //  }
  //
  //  //  def calcMedian(xs: List[Int]) = {
  //  //    val sorted = xs.sorted
  //  //    val size = sorted.size
  //  //    if (size % 2 == 0) {
  //  //      val middleIndex = size / 2
  //  //      (xs(middleIndex) + xs(middleIndex + 2)) / 2
  //  //    } else {
  //  //      val middleIndex = size / 2 + 1
  //  //      xs(middleIndex)
  //  //    }
  //  //  }
  //
  //  def printResults(results: Array[(String, Double, Int, Int)]): Unit = {
  //    println("Resulting clusters:")
  //    println("  Score  Dominant language (%percent)  Questions")
  //    println("================================================")
  //    for ((lang, percent, size, score) <- results)
  //      println(f"${score}%7d  ${lang}%-17s (${percent}%-5.1f%%)      ${size}%7d")
  //  }
}







