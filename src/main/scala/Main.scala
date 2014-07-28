import java.io.FileInputStream
import java.util.Properties

import core._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection._
import scala.util.control.Breaks._

object Main {
  final val NUM_DATA_FIELDS = 9

  val conf = new SparkConf()
    .setAppName("Explanation Table")
    .setMaster("spark://freddie-Lenovo-Ideapad-Flex-15:7077")
    .set("spark.executor.memory", "3g")
    .set("spark.executor.extraJavaOptions", "-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps")
  val sc = new SparkContext(conf)

//  var filePath: String = null
  var workingDirectory: String = null
  var inputDataFileName: String = null
  var inputDataSize: Long = 0
  var summaryTableSize: Int = 0
  var sampleTableSize: Long = 0
  var diffThreshold: Double = 0
  var summaryTable: mutable.Map[Int, SummaryTuple] = mutable.Map()

  var inputDataRDD: RDD[DataTuple] = null
  var summaryRDD: RDD[SummaryTuple] = null
  var estimateRDD: RDD[EstimateTuple] = null
  var richSummaryRDD: RDD[RichSummaryTuple] = null
  var sampleRDD: RDD[DataTuple] = null
  var LCARDD: RDD[LCATuple] = null
  var aggregatedRDD: RDD[LCATuple] = null
  var correctedPatternRDD: RDD[CorrectedTuple] = null

  def prepareData() {
    val sourceData = sc.textFile(workingDirectory + inputDataFileName, 4).cache()
    inputDataRDD = sourceData.map(line => parse(line))
  }

  def prepareSummary() {
    val topPattern = SummaryTuple(0, NUM_DATA_FIELDS)
    for (i <- 0 until NUM_DATA_FIELDS) {
      topPattern.pattern(i) = "*"
    }
    summaryTable(0) = topPattern
    val data: Array[SummaryTuple] = summaryTable.map(_._2).toArray
    summaryRDD = sc.parallelize(data)
  }

  def computeEstimate() {
    //    println("inputDataRDD.count():" + inputDataRDD.count())
    //    println("summaryRDD.count():" + summaryRDD.count())
    val product = inputDataRDD.cartesian(summaryRDD)
    estimateRDD = product
      .filter(pair => matchPattern(pair._1.attributes, pair._2.pattern))
      .map(pair => (pair._1.flatten, pair._2.multiplier))
      .reduceByKey((left, right) => left + right)
      .map(
        pair => {
          val power2 = Math.pow(2, pair._2)
          EstimateTuple(DataTuple(pair._1), (power2 / (power2 + 1)).toFloat)
        }
      )
  }

  // TODO: Simplify this function
  def computeRichSummary() {
    case class ReductionValue(pattern: Array[String], multiplier: Double, var p: Float, var q: Float, var count: Long = 1)

    val bDiffThreshold = sc.broadcast(Array(diffThreshold))
    val product = estimateRDD.cartesian(summaryRDD)
    richSummaryRDD = product
      .filter(pair => matchPattern(pair._1.dataTuple.attributes, pair._2.pattern))
      .map(
        pair =>
          // TODO: Check if multiplier and id are required
          (pair._2.id, ReductionValue(pair._2.pattern, pair._2.multiplier, pair._1.dataTuple.p, pair._1.q, 1))
      )
      .reduceByKey((left, right) => ReductionValue(left.pattern, left.multiplier, left.p + right.p, right.q + left.q, left.count + right.count))
      .map(
        pair => {
          pair._2.p /= pair._2.count // Divide p by count to compute avg(p)
          pair._2.q /= pair._2.count // Divide q by count to compute avg(q)
          val diff = calculateDiff(pair._2.p, pair._2.q, pair._2.count)
          (diff, RichSummaryTuple(pair._2.pattern, pair._1, pair._2.multiplier, pair._2.p, pair._2.q, pair._2.count, diff))
        }
      )
      .filter(pair => {pair._1 > bDiffThreshold.value(0)})
      .sortByKey(false, 4)
      .map(pair => pair._2)
  }

  def computeLCA() {
    val product = estimateRDD.cartesian(sampleRDD)
    LCARDD = product
      .map(
        pair => {
          val pattern = generatePattern(pair._1.dataTuple.attributes, pair._2.attributes)
          (pattern.mkString("-"), Array(1.0, pair._1.dataTuple.p, pair._1.q))
//          (Pattern(pattern), Array(1.0, pair._1.dataTuple.p, pair._1.q))
        }
      )
      .reduceByKey(
        (left, right) => {
          (left, right).zipped map (_ + _)
        }
      )
      .map(
        pair => {
          LCATuple(pair._1.split("-"), pair._2(0).toLong, pair._2(1), pair._2(2))
        }
      )
  }

  def computeHierarchy() {
    aggregatedRDD =
      LCARDD
        .flatMap(
          tuple => {
            generateAncestors(tuple.pattern, 0).toList.map(key => (key.content.mkString("-"), Array(tuple.count, tuple.p, tuple.q)))
          }
        )
        .reduceByKey(
          (left, right) => {
            (left, right).zipped map (_ + _)
          }
        )
        .map(
          pair => {
            LCATuple(pair._1.split("-"), pair._2(0).toLong, pair._2(1), pair._2(2))
          }
        )
  }

  def computeCorrectedStats() {
    //    case class ReductionKey(pattern: Array[String], count: Long, p: Double, q: Double) extends Serializable

    val product = aggregatedRDD.cartesian(sampleRDD)
    correctedPatternRDD = product
      .filter(
        pair => {
          matchPattern(pair._2.attributes, pair._1.pattern)
        }
      )
      //    println("product.count():" + product.count())
      //    aggregatedRDD.take(50).foreach(x => printTuples(x.pattern))
      .map(
        pair => {
          // TODO: Verify that getting rid of count, p and q as part of the key is fine
          (pair._1.pattern.mkString("-"), Array(pair._1.p, pair._1.q, pair._1.count, 1))
        }
      )
//    partial.collect().foreach(pair => {
//      val p = pair._2(0)
//      val q = pair._2(1)
//      val count = pair._2(2).toLong
//      val newP = p / count
//      if (newP.isNaN) {
//        println(p, q, count, pair._2(2), pair._1)
//      }
//    })
        .reduceByKey(
        (left, right) => {
          left(3) += right(3)
          left
        }
      )
        .map(
          pair => {
            val p = pair._2(0)
            val q = pair._2(1)
            val count = pair._2(2).toLong
            val numSampleMatch = pair._2(3).toInt
            val gain = calculateGain(p, q, count, numSampleMatch)
            val t = CorrectedTuple(
              pair._1.split("-"),
              count / numSampleMatch,
              p / count,
              q / count,
              gain,
              calculateMultiplier(p, q),
              numSampleMatch
            )
            (gain, t)
            //          (p, t)
          }
        )
        .sortByKey(false, 4)
        .map(pair => pair._2)
  }

  def parse(line: String): DataTuple = {
    val fields = line.split(",")
    assert(fields.length == 2 + NUM_DATA_FIELDS)
    val ret = DataTuple(fields(0).toLong, fields(1).toShort, NUM_DATA_FIELDS)
    for (i <- 2 until fields.length) {
      ret.attributes(i - 2) = fields(i)
    }
    ret
  }

  def main(args: Array[String]) {

    //    val test_string: Array[String] = Array("0", "2", null, null, "001", null, "999", null, null)
    //    val test_string: Array[String] = Array("0", "2", "", "", "001", "", "999", "", "")
    //    val test_pattern: Array[String] = Array(null, null, null, null, null, null, null, null, null)
    //
    ////    val test_output = generateAncestors(test_string, 0)
    ////    test_output.foreach(pattern => {
    ////      pattern.content.foreach(field => print(field + ","))
    ////      println()
    ////    })
    //    println(matchPattern(test_string, test_pattern))
    //    return

    //TODO: Read it from config file or some sort
    val properties = new Properties()
    properties.load(getClass.getResourceAsStream("config"))
    summaryTableSize = properties.getProperty("summaryTableSize").toInt
    sampleTableSize = properties.getProperty("sampleTableSize").toInt
    workingDirectory = properties.getProperty("workingDirectory")
    if (!(workingDirectory.takeRight(1) equals "/"))
      workingDirectory += "/"
    inputDataFileName = properties.getProperty("inputDataFileName")

    prepareData()
    inputDataSize = inputDataRDD.count()
    diffThreshold = inputDataSize / 5000 + 1
    println("diffThreshold:" + diffThreshold)

    prepareSummary()

    for (i <- 0 until summaryTableSize) {
      breakable {
        while (true) {
          computeEstimate()
          //          println("estimateRDD.count() " + estimateRDD.count())
          computeRichSummary()

          //          println("richSummaryRDD.count() " + richSummaryRDD.count())

          if (richSummaryRDD.count() == 0) {
            break
          } else {
            val topPattern = richSummaryRDD.first()
            val multiplier = scaleMultiplier(topPattern.p, topPattern.q, topPattern.multiplier)
            println(topPattern.flatten, multiplier)

            summaryTable.get(topPattern.id) match {
              case Some(pair) => {
                pair.multiplier = multiplier
                summaryTable.updated(topPattern.id, multiplier)
                val newSummaries: Array[SummaryTuple] = summaryTable.map(_._2).toArray
                summaryRDD = sc.parallelize(newSummaries)
              }
              case None => {
                Console.err.println("Summary ID not found!")
              }
            }
          }
        }
      }

      println("Convergence Loop Done")

      //      val sourceData = sc.textFile("hdfs://localhost:9000/explanation_table/sample.csv", 4).cache()
      //      sampleRDD = sourceData.map(line => parse(line))

      sampleRDD = inputDataRDD.sample(false, sampleTableSize.toDouble / inputDataSize, 99)

      // Cache the two RDD for an expensive join
      sampleRDD.cache()
      estimateRDD.cache()
//      println("sampleRDD.count():" + sampleRDD.count() + " estimateRDD.count():" + estimateRDD.count())

      computeLCA()
      //      LCARDD.map(lca => {
      //        lca.flatten
      //      }).saveAsTextFile("hdfs://localhost:9000/explanation_table/LCARDD.csv")
      println("LCARDD.count():" + LCARDD.count())

      computeHierarchy()
      //      aggregatedRDD.map(lca => {
      //        lca.flatten
      //      }).saveAsTextFile("hdfs://localhost:9000/explanation_table/aggregatedRDD.csv")
      println("aggregatedRDD.count():" + aggregatedRDD.count())

      computeCorrectedStats()
      //      val pattern = correctedPatternRDD.collect()
      val topPattern = correctedPatternRDD.first()
//      correctedPatternRDD.map(t => t.flatten).saveAsTextFile("hdfs://localhost:9000/explanation_table/correctedPatternRDD.csv")
      val newEntry =
        SummaryTuple(
          summaryTable.size,
          NUM_DATA_FIELDS,
          topPattern.count,
          topPattern.q,
          topPattern.multiplier
        )
      for (i <- 0 until newEntry.pattern.size) {
        newEntry.pattern(i) = topPattern.pattern(i)
      }
      println("newEntry.flatten:" + newEntry.flatten)
      summaryTable(summaryTable.size) = newEntry

      //TODO: Refactor the following
      val data: Array[SummaryTuple] = summaryTable.map(_._2).toArray
      summaryRDD = sc.parallelize(data)

      //    val top = correctedPatternRDD.collect()(0)
      //    val pattern = top.pattern.mkString("-")
      //    println("correctedPatternRDD.first():" + pattern + " " + top.gain
      //      + " " + top.p + " " + top.q + " " + top.count + " " + top.numMatch)
      //    println("correctedPatternRDD.count():" + correctedPatternRDD.count())

    }

    summaryRDD.map(t => t.flatten).saveAsTextFile("hdfs://localhost:9000/explanation_table/summaryRDD.csv")
  }
}
