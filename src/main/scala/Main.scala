import java.io.{BufferedWriter, OutputStreamWriter}
import java.net.URI
import java.util.Properties

import core._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection._
import scala.collection.mutable.ListBuffer
import scala.util.Random
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

  var estimateRDD: RDD[EstimateTuple] = null
  var richSummaryRDD: RDD[RichSummaryTuple] = null

  var sampleTable: Array[DataTuple] = null
  var bSampleTable: Broadcast[Array[DataTuple]] = null

  var LCARDD: RDD[LCATuple] = null
  var aggregatedRDD: RDD[LCATuple] = null
  var correctedPatternRDD: RDD[CorrectedTuple] = null

  val rand = new Random(System.currentTimeMillis())

  def prepareData() {
    val sourceData = sc.textFile(workingDirectory + inputDataFileName, 4).cache()
    inputDataRDD = sourceData.map(line => parseInputLine(line, numDataFields))
  }

  def prepareSummary() {
    val topPattern = SummaryTuple(0, numDataFields)
    for (i <- 0 until numDataFields) {
      topPattern.pattern(i) = "*"
    }
    summaryTable(0) = topPattern
  }

  def computeEstimate() {
    val bSummaryTable = sc.broadcast(summaryTable.map(_._2).toArray)
    estimateRDD =
      inputDataRDD
        .flatMap(
          t => {
            val summaryTable = bSummaryTable.value
            val ret = ListBuffer.empty[(String, Double)]
            summaryTable.foreach(
              summary => {
                if (matchPattern(t.attributes, summary.pattern)) {
                  val pair = (t.flatten, summary.multiplier)
                  ret += pair
                }
              }
            )
            ret.toList
          }
        )
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
    val bSummaryTable = sc.broadcast(summaryTable.map(_._2).toArray)
    richSummaryRDD = estimateRDD
      .flatMap(
        t => {
          val summaryTable = bSummaryTable.value
          val ret = ListBuffer.empty[(Int, ReductionValue)]
          summaryTable.foreach(
            summary => {
              if (matchPattern(t.dataTuple.attributes, summary.pattern)) {
                val pair = (summary.id, ReductionValue(summary.pattern, summary.multiplier, t.dataTuple.p, t.q, 1))
                ret += pair
              }
            }
          )
          ret.toList
        }
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
      .sortByKey(false, 4)
      .map(pair => pair._2)
  }

  def computeLCA() {
    val product = estimateRDD
    val bSampleTable = sc.broadcast(sampleTable)
    LCARDD = product
      .flatMap(
        t => {
          val sampleDataTable = bSampleTable.value
          val ret = ListBuffer.empty[(String, Array[Double])]
          sampleDataTable.foreach(
            sample => {
              val pattern = generatePattern(t.dataTuple.attributes, sample.attributes)
              val pair = (pattern.mkString("-"), Array(1.0, t.dataTuple.p, t.q))
              ret += pair
            }
          )
          ret.toList
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
    bSampleTable.unpersist()
  }

  def computeHierarchy() {
    aggregatedRDD = LCARDD
  }

  def computeCorrectedStats() {
    val bSampleTable = sc.broadcast(sampleTable)
    val product = aggregatedRDD
    correctedPatternRDD = product
      .flatMap(
        t => {
          val sampleDataTable = bSampleTable.value
          val ret = ListBuffer.empty[(String, Array[Double])]
          sampleDataTable.foreach(sample => {
            if (matchPattern(sample.attributes, t.pattern)) {
              val pair = (t.pattern.mkString("-"), Array(t.p, t.q, t.count, 1))
              ret += pair
            }
          })
          ret.toList
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
        }
      )
      .sortByKey(false, 4)
      .map(pair => pair._2)
    bSampleTable.unpersist()
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
      .set("spark.executor.memory", "3g")
      .set("spark.executor.extraJavaOptions", "-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps")
    sc = new SparkContext(conf)

    prepareData()
    inputDataSize = inputDataRDD.count()
    println("Data size: " + inputDataSize + " rows")
    diffThreshold = inputDataSize / 5000 + 1
    println("diffThreshold:" + diffThreshold)

    prepareSummary()

    for (i <- 0 until summaryTableSize) {
      start_time = System.currentTimeMillis()

      breakable {
        while (true) {
          computeEstimate()
          computeRichSummary()

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
              }
              case None => {
                Console.err.println("Summary ID not found!")
              }
            }
          }
        }
      }

      end_time = System.currentTimeMillis()

      println("Step 1: Convergence loop done. Time taken:" + (end_time - start_time))

      start_time = System.currentTimeMillis()
      sampleTable = inputDataRDD.sample(false, sampleTableSize.toDouble / inputDataSize, rand.nextInt(50000)).collect()

      estimateRDD.cache()

      end_time = System.currentTimeMillis()
      println("Step 2: Sampling done. Time taken:" + (end_time - start_time))

      start_time = System.currentTimeMillis()
      computeLCA()
      println("LCARDD.count():" + LCARDD.count())

      computeHierarchy()
      println("aggregatedRDD.count():" + aggregatedRDD.count())

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

      end_time = System.currentTimeMillis()
      println("Step 3: Generate new rules done. Time taken:" + (end_time - start_time))
    }

    val configuration = new Configuration()
    //TODO: Change the hdfs host
    val hdfs = FileSystem.get(new URI("hdfs://localhost:9000"), configuration)
    val path = new Path(workingDirectory + "summaryRDD.csv")
    if (hdfs.exists(path))
      hdfs.delete(path, true)
    val bufferedWriter = new BufferedWriter(new OutputStreamWriter(hdfs.create(path, true)))
    summaryTable.foreach(pair => bufferedWriter.write(pair._2.flatten + "\n"))
    bufferedWriter.close()
    println(summaryTable.size, workingDirectory + "summaryRDD.csv")
  }
}
