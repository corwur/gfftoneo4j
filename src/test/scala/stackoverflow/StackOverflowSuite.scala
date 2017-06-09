package stackoverflow

import org.scalatest.{FunSuite, BeforeAndAfterAll}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import java.io.File
import StackOverflow._

@RunWith(classOf[JUnitRunner])
class StackOverflowSuite extends FunSuite with BeforeAndAfterAll {


  lazy val testObject = new GffToSpark {
    override val langs =
      List(
        "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
        "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")
    override def langSpread = 50000
    override def kmeansKernels = 45
    override def kmeansEta: Double = 20.0D
    override def kmeansMaxIterations = 120
  }

  test("testObject can be instantiated") {
    val instantiatable = try {
      testObject
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiatable, "Can't instantiate a StackOverflow object")
  }

  val q1 = Posting(1, 1001, None, None, 0, Some("Ruby")) // idx 6
  val q2 = Posting(1, 1002, None, None, 0, Some("MATLAB")) // idx 12

  val a1 = Posting(2, 2001, None, Some(q1.id), 10, None)
  val a2 = Posting(2, 2002, None, Some(q1.id), 20, None)
  val a3 = Posting(2, 2003, None, Some(q2.id), 15, None)

  val posts = sc.parallelize(Seq(q1, q2, a1, a2, a3))

  test("'groupedPostings' should group together questions and answers correctly") {
    val grouped: List[(Int, List[(Posting, Posting)])] = groupedPostings(posts).collect.toList
        .map(a => (a._1, a._2.toList))

    assert(grouped.size == 2)

    val answersQ1: Option[List[(Posting, Posting)]] = grouped.find(_._1 == q1.id).map(_._2)

    assert(answersQ1.exists(_.size == 2))
    assert(answersQ1.exists(_.contains(q1, a1)))
    assert(answersQ1.exists(_.contains(q1, a2)))

    val answersQ2: Option[List[(Posting, Posting)]] = grouped.find(_._1 == q2.id).map(_._2)

    assert(answersQ2.exists(_.size == 1))
    assert(answersQ2.exists(_.contains((q2, a3))))
  }

  test("'scoredPostings' should calculate scores correctly") {
    val grouped = groupedPostings(posts)
    val scoredPosts = scoredPostings(grouped)
      .collect.toList

    assert(scoredPosts.sortBy(_._1.id) == List((q1, 20), (q2, 15)))
  }

  test("'vectorPostings'") {
    val grouped = groupedPostings(posts)
    val scored = scoredPostings(grouped)

    val vectors = vectorPostings(scored).collect.toList

    assert(vectors.sortBy(_._1) == List((6 * langSpread, 20), (12 * langSpread, 15)))

  }
}
