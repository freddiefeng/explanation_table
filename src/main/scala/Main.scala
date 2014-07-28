import java.util.Properties

import core._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection._
import scala.util.control.Breaks._

object Main {
  var numDataFields = 9
  var workingDirectory: String = null
  var inputDataFileName: String = null
  var sparkMaster: String = null

  var conf: SparkConf = null
  var sc: SparkContext = null

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
    val sourceData = sc.textFile(workingDirectory + inputDataFileName)
    inputDataRDD = sourceData.map(line => parseInputLine(line, numDataFields)).cache()
  }

  def prepareSummary() {
    val topPattern = SummaryTuple(0, numDataFields)
    for (i <- 0 until numDataFields) {
      topPattern.pattern(i) = "*"
    }
    summaryTable(0) = topPattern
    //    val data: Array[SummaryTuple] = summaryTable.map(_._2).toArray
    var data = Array.empty[SummaryTuple]
    summaryTable.foreach(pair => data :+= pair._2)
    summaryRDD = sc.parallelize(data, 8)
  }

  def computeEstimate() {
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
//      .coalesce(512)
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
      .filter(
        pair => {
          pair._1 > bDiffThreshold.value(0)
        }
      )
      .sortByKey(false)
      .map(pair => pair._2)
  }

  def computeLCA() {
    val product = estimateRDD.cartesian(sampleRDD)
    LCARDD = product
      .map(
        pair => {
          val pattern = generatePattern(pair._1.dataTuple.attributes, pair._2.attributes)
          (pattern.mkString("-"), Array(1.0, pair._1.dataTuple.p, pair._1.q))
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
    val product = aggregatedRDD.cartesian(sampleRDD)
    correctedPatternRDD = product
      .filter(
        pair => {
          matchPattern(pair._2.attributes, pair._1.pattern)
        }
      )
      .map(
        pair => {
          // TODO: Verify that getting rid of count, p and q as part of the key is fine
          (pair._1.pattern.mkString("-"), Array(pair._1.p, pair._1.q, pair._1.count, 1))
        }
      )
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

  def loadConfig() {
    val properties = new Properties()
    properties.load(getClass.getResourceAsStream("config"))
    summaryTableSize = properties.getProperty("summaryTableSize").toInt
    sampleTableSize = properties.getProperty("sampleTableSize").toInt
    workingDirectory = properties.getProperty("workingDirectory")
    if (!(workingDirectory.takeRight(1) equals "/"))
      workingDirectory += "/"
    inputDataFileName = properties.getProperty("inputDataFileName")
    sparkMaster = properties.getProperty("sparkMaster")
    numDataFields = properties.getProperty("numDataFields").toInt
  }

  def main(args: Array[String]) {
    var start_time: Long = 0
    var end_time: Long = 0
    loadConfig()

    conf = new SparkConf()
      .setAppName("Explanation Table")
      .setMaster(sparkMaster)
      //      .set("spark.executor.memory", "3g")
      .set("spark.executor.extraJavaOptions", "-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps")
      .set("spark.eventLog.enabled", "true")
      .set("spark.eventLog.dir", workingDirectory + "/sparkevents")
    sc = new SparkContext(conf)

    prepareData()
    inputDataSize = inputDataRDD.count()
    diffThreshold = inputDataSize / 5000 + 1
    println("diffThreshold:" + diffThreshold)

    prepareSummary()

    for (i <- 0 until summaryTableSize) {
      start_time = System.currentTimeMillis()

      breakable {
        while (true) {
          computeEstimate()
          estimateRDD.cache()
          computeRichSummary()

          if (richSummaryRDD.count() == 0) {
            break
          } else {
            val topPattern = richSummaryRDD.first()
            val multiplier = scaleMultiplier(topPattern.p, topPattern.q, topPattern.multiplier)
            println(topPattern.flatten, multiplier)

            summaryTable.get(topPattern.id) match {
              case Some(tuple) => {
                tuple.multiplier = multiplier
                summaryTable.update(topPattern.id, tuple)
                //                val newSummaries: Array[SummaryTuple] = summaryTable.map(_._2).toArray
                var newSummaries = Array.empty[SummaryTuple]
                summaryTable.foreach(pair => newSummaries :+= pair._2)
                summaryRDD = sc.parallelize(newSummaries, 8)
              }
              case None => {
                Console.err.println("Summary ID not found!")
              }
            }

            estimateRDD.unpersist()
          }
        }
      }

      end_time = System.currentTimeMillis()

      println("Step 1: Convergence loop done. Time taken:" + (end_time - start_time))

      start_time = System.currentTimeMillis()
      sampleRDD = inputDataRDD.sample(false, sampleTableSize.toDouble / inputDataSize, 99)

      // Cache the two RDD for an expensive join
      sampleRDD.cache()

      end_time = System.currentTimeMillis()
      println("Step 2: Sampling done. Time taken:" + (end_time - start_time))

      start_time = System.currentTimeMillis()
      computeLCA()
      //      println("LCARDD.count():" + LCARDD.count())

      computeHierarchy()
      //      println("aggregatedRDD.count():" + aggregatedRDD.count())

      computeCorrectedStats()
      val topPattern = correctedPatternRDD.first()
      val newEntry =
        SummaryTuple(
          summaryTable.size,
          numDataFields,
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
      //      val data: Array[SummaryTuple] = summaryTable.map(_._2).toArray
      var newSummaries = Array.empty[SummaryTuple]
      summaryTable.foreach(pair => newSummaries :+= pair._2)
      summaryRDD = sc.parallelize(newSummaries, 8)

      end_time = System.currentTimeMillis()
      println("Step 3: Generate new rules done. Time taken:" + (end_time - start_time))
    }

    //    summaryRDD.map(t => t.flatten).saveAsTextFile(workingDirectory + "summaryRDD.csv")

    sc.stop()
  }
}
