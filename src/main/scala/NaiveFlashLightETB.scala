import java.io.{PrintWriter, OutputStreamWriter, BufferedWriter}
import java.net.URI

import core._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import scala.collection._

class NaiveFlashLightETB extends ExplanationTableBuilder {
  var inputDataSize: Long = 0
  var diffThreshold: Double = 0

  var summaryRDD: RDD[SummaryTuple] = null
  var estimateRDD: RDD[EstimateTuple] = null
  var richSummaryRDD: RDD[RichSummaryTuple] = null
  var sampleRDD: RDD[DataTuple] = null
  var LCARDD: RDD[LCATuple] = null
  var aggregatedRDD: RDD[LCATuple] = null
  var correctedPatternRDD: RDD[CorrectedTuple] = null

  def prepareSummary() {
    val topPattern = SummaryTuple(0, numDataFields)
    for (i <- 0 until numDataFields) {
      topPattern.pattern(i) = "*"
    }
    summaryTable(0) = topPattern
//    val data: Array[SummaryTuple] = summaryTable.map(_._2).toArray
    var data = Array.empty[SummaryTuple]
    summaryTable.foreach(pair => data :+= pair._2)
//    summaryRDD = sc.parallelize(data, 8)
    summaryRDD = sc.parallelize(data)
  }

  def computeEstimate() {
    val product = inputDataRDD.cartesian(summaryRDD)
    estimateRDD = product
      .filter(pair => matchPattern(pair._1.attributes, pair._2.pattern))
//      .coalesce(8)
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
//      .coalesce(1024)
      .map(
        pair =>
          // TODO: Check if multiplier and id are required
          (pair._2.id, ReductionValue(pair._2.pattern, pair._2.multiplier, pair._1.dataTuple.p, pair._1.q, 1))
      )
      .reduceByKey(
        (left, right) => ReductionValue(left.pattern, left.multiplier, left.p + right.p, right.q + left.q, left.count + right.count)
      )
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
//      .coalesce(8)
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
//      .coalesce(8)
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
//      .sortByKey(false, 4)
      .sortByKey(false)
      .map(pair => pair._2)
  }

  def iterativeScaling() {
    while (true) {
      computeEstimate()
      estimateRDD.cache()
      computeRichSummary()

      if (richSummaryRDD.count() == 0) {
        return
      } else {
        val topPattern = richSummaryRDD.first()
        val multiplier = scaleMultiplier(topPattern.p, topPattern.q, topPattern.multiplier)
        println(topPattern.flatten, multiplier)

        summaryTable.get(topPattern.id) match {
          case Some(tuple) => {
            tuple.multiplier = multiplier
            summaryTable.update(topPattern.id, tuple)
//            val newSummaries: Array[SummaryTuple] = summaryTable.map(_._2).toArray
            var newSummaries = Array.empty[SummaryTuple]
            summaryTable.foreach(pair => newSummaries :+= pair._2)
//            summaryRDD = sc.parallelize(newSummaries, 8)
            summaryRDD = sc.parallelize(newSummaries)
          }
          case None => {
            Console.err.println("Summary ID not found!")
          }
        }

        estimateRDD.unpersist()
      }

//      statOutput ++= "iterative scaling\n"
      reportRunning()
    }
  }

  def measureKL(): Double = {
    iterativeScaling()
    estimateRDD.map(t => computeKL(t)).reduce(_ + _)
  }

//  override def prepareData() {
//    val bNumDataFields = sc.broadcast(numDataFields)
//    val sourceData = sc.textFile(hostAddress + workingDirectory + inputDataFileName)
//    val sourceArray = sourceData.collect()
//    inputDataRDD = sc.parallelize(sourceArray, 4)
//      .map(
//        line => {
//          val numDataFields = bNumDataFields.value
//          parseInputLine(line, numDataFields)
//        }
//      )
//    inputDataRDD.cache()
//  }

  def reportRunning(): Unit = {
//    hdfs = FileSystem.get(new URI(hostAddress), new Configuration())
//    val path = new Path(hostAddress + workingDirectory + "/running.txt")
//    if (hdfs.exists(path))
//      hdfs.delete(path, true)
//    val bufferedWriter = new BufferedWriter(new OutputStreamWriter(hdfs.create(path, true)))
//    bufferedWriter.write(statOutput.toString())
//    bufferedWriter.close()
  }

  def buildTable() {
    var start_time: Long = 0
    var end_time: Long = 0
    loadConfig()

    prepareData()
    inputDataSize = inputDataRDD.count()
    statOutput ++= "Data size: " + inputDataSize + " rows" + "\n"
    diffThreshold = inputDataSize / 5000 + 1
    //    println("diffThreshold:" + diffThreshold)
    statOutput ++= "diffThreshold:" + diffThreshold + "\n"

    prepareSummary()

    for (i <- 0 until summaryTableSize) {
      start_time = System.currentTimeMillis()

      iterativeScaling()

      end_time = System.currentTimeMillis()

      println("Step 1: Convergence loop done. Time taken:" + (end_time - start_time))
      statOutput ++= "Step 1: Convergence loop done. Time taken:" + (end_time - start_time) + "\n"

      reportRunning()

      start_time = System.currentTimeMillis()
//      sampleRDD = inputDataRDD
//        .sample(false, sampleTableSize.toDouble / inputDataSize, rand.nextInt(50000))
//        .repartition(8)
      sampleRDD = inputDataRDD
        .sample(false, sampleTableSize.toDouble / inputDataSize, rand.nextInt(50000))

      // Cache the two RDD for an expensive join
      sampleRDD.cache()
      end_time = System.currentTimeMillis()
      println("Step 2: Sampling done. Time taken:" + (end_time - start_time))
      statOutput ++= "Step 2: Sampling done. Time taken:" + (end_time - start_time) + "\n"

      reportRunning()

      start_time = System.currentTimeMillis()
      computeLCA()

      computeHierarchy()

      computeCorrectedStats()
      val topPattern = correctedPatternRDD.first()
      val newEntry =
        SummaryTuple(
          summaryTable.size,
          numDataFields,
          topPattern.count,
          topPattern.q,
          topPattern.multiplier,
          topPattern.gain
        )
      for (i <- 0 until newEntry.pattern.size) {
        newEntry.pattern(i) = topPattern.pattern(i)
      }
      summaryTable(summaryTable.size) = newEntry

      //TODO: Refactor the following
//      val data: Array[SummaryTuple] = summaryTable.map(_._2).toArray
      var newSummaries = Array.empty[SummaryTuple]
      summaryTable.foreach(pair => newSummaries :+= pair._2)
//      summaryRDD = sc.parallelize(newSummaries, 8)
      summaryRDD = sc.parallelize(newSummaries)

      sampleRDD.unpersist()
      estimateRDD.unpersist()

      end_time = System.currentTimeMillis()
      println("Step 3: Generate new rules done. Time taken:" + (end_time - start_time))
      statOutput ++= "Step 3: Generate new rules done. Time taken:" + (end_time - start_time) + "\n"

      reportRunning()
    }

//        summaryRDD.map(t => t.flatten).saveAsTextFile(workingDirectory + "summaryRDD.csv")

    KL = measureKL()

    sc.stop()

    postProcess()
  }
}