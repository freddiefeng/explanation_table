import java.io.{BufferedWriter, OutputStreamWriter}
import java.net.URI
import java.util.concurrent.{LinkedBlockingDeque}

import core._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection._
import scala.collection.mutable.ListBuffer

class AsyncBroadcastFlashLightETB extends ExplanationTableBuilder {
  class SampleProducer(
                        inputDataRDD: RDD[DataTuple],
                        sampleDataQueue: LinkedBlockingDeque[Array[DataTuple]]
                        ) extends Runnable {
    var finished: Boolean = false
    def terminate() {
      finished = true
    }

    var log: String = ""

    override def run(): Unit = {
      while(!finished) {
        log += "one pass " + System.currentTimeMillis() + "\n"
        val sampleRDD = inputDataRDD
          .sample(false, sampleTableSize.toDouble / inputDataSize, rand.nextInt(50000))
          .collect()
        sampleDataQueue.put(sampleRDD)
      }
    }
  }

  val QUEUE_CAPACITY = 5
  val sampleDataQueue = new LinkedBlockingDeque[Array[DataTuple]](QUEUE_CAPACITY)

  var inputDataSize: Long = 0
  var diffThreshold: Double = 0
  var summaryTable: mutable.Map[Int, SummaryTuple] = mutable.Map()

  var estimateRDD: RDD[EstimateTuple] = null
  var richSummaryRDD: RDD[RichSummaryTuple] = null

  var sampleTable: Array[DataTuple] = null
  var bSampleTable: Broadcast[Array[DataTuple]] = null

  var LCARDD: RDD[LCATuple] = null
  var aggregatedRDD: RDD[LCATuple] = null
  var correctedPatternRDD: RDD[CorrectedTuple] = null

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
    case class ReductionValue(
                               pattern: Array[String],
                               multiplier: Double,
                               var p: Float,
                               var q: Float,
                               var count:
                               Long = 1)

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
      .reduceByKey(
        (left, right) =>
          ReductionValue(left.pattern, left.multiplier, left.p + right.p, right.q + left.q, left.count + right.count)
      )
      .map(
        pair => {
          pair._2.p /= pair._2.count // Divide p by count to compute avg(p)
          pair._2.q /= pair._2.count // Divide q by count to compute avg(q)
          val diff = calculateDiff(pair._2.p, pair._2.q, pair._2.count)
          (diff,
            RichSummaryTuple(pair._2.pattern, pair._1, pair._2.multiplier, pair._2.p, pair._2.q, pair._2.count, diff))
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
    val bSampleTable = sc.broadcast(sampleTable)
    LCARDD = estimateRDD
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
    aggregatedRDD =
      LCARDD
        .flatMap(
          tuple => {
            generateAncestors(tuple.pattern, 0)
              .toList
              .map(key => (key.content.mkString("-"), Array(tuple.count, tuple.p, tuple.q)))
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

  def iterativeScaling() {
    while (true) {
      computeEstimate()
      computeRichSummary()

      if (richSummaryRDD.count() == 0) {
        return
      } else {
        val topPattern = richSummaryRDD.first()
        val multiplier = scaleMultiplier(topPattern.p, topPattern.q, topPattern.multiplier)

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

  def measureKL(): Double =  {
    iterativeScaling()
    estimateRDD.map(t => computeKL(t)).reduce(_+_)
  }

  def postProcess() {
    hdfs = FileSystem.get(new URI(hostAddress), new Configuration())
    val path = new Path(hostAddress + workingDirectory + "/summary.txt")
    if (hdfs.exists(path))
      hdfs.delete(path, true)
    val bufferedWriter = new BufferedWriter(new OutputStreamWriter(hdfs.create(path, true)))
    summaryTable.foreach(pair => bufferedWriter.write(pair._2.flatten + "\n"))
    bufferedWriter.write("Execution Time by Steps:" + "\n")
    bufferedWriter.write(statOutput.toString())
    bufferedWriter.write("Kullback Leibler divergence:" + "\n")
    bufferedWriter.write(KL.toString + "\n")

    bufferedWriter.close()
  }

  def buildTable() {
    var start_time: Long = 0
    var end_time: Long = 0
    loadConfig()

    prepareData()
    inputDataSize = inputDataRDD.count()
    //    println("Data size: " + inputDataSize + " rows")
    statOutput ++= "Data size: " + inputDataSize + " rows" + "\n"
    diffThreshold = inputDataSize / 5000 + 1
    //    println("diffThreshold:" + diffThreshold)
    statOutput ++= "diffThreshold:" + diffThreshold + "\n"

    val sampleDataProducer = new SampleProducer(inputDataRDD, sampleDataQueue)
    val producerThread = new Thread(sampleDataProducer)
    producerThread.start()

    prepareSummary()

    for (i <- 0 until summaryTableSize) {
      start_time = System.currentTimeMillis()

      iterativeScaling()

      end_time = System.currentTimeMillis()

      println("Step 1: Convergence loop done. Time taken:" + (end_time - start_time))
      statOutput ++= "Step 1: Convergence loop done. Time taken:" + (end_time - start_time) + "\n"

      start_time = System.currentTimeMillis()
      //      sampleTable = inputDataRDD.sample(false, sampleTableSize.toDouble / inputDataSize, rand.nextInt(50000)).collect()
      sampleTable = sampleDataQueue.take()


      end_time = System.currentTimeMillis()
      println("Step 2: Sampling done. Time taken:" + (end_time - start_time))
      statOutput ++= "Step 2: Sampling done. Time taken:" + (end_time - start_time) + "\n"
      estimateRDD.cache()

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
          topPattern.multiplier,
          topPattern.gain
        )
      for (i <- 0 until newEntry.pattern.size) {
        newEntry.pattern(i) = topPattern.pattern(i)
      }
      //      println("newEntry.flatten:" + newEntry.flatten)
      summaryTable(summaryTable.size) = newEntry

      end_time = System.currentTimeMillis()
      println("Step 3: Generate new rules done. Time taken:" + (end_time - start_time))
      statOutput ++= "Step 3: Generate new rules done. Time taken:" + (end_time - start_time) + "\n"
    }

    KL = measureKL()

    statOutput ++= "Log From sampling thread:"
    statOutput ++= sampleDataProducer.log

    sampleDataProducer.terminate()
    while(!sampleDataQueue.isEmpty) {
      sampleDataQueue.clear()
    }

    sc.stop()

    postProcess()
  }
}
