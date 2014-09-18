import java.io.{BufferedWriter, OutputStreamWriter}
import java.net.URI

import core._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import scala.collection._
import scala.collection.mutable.ListBuffer

class DataCubeETB extends ExplanationTableBuilder {
  var inputDataSize: Long = 0
  var diffThreshold: Double = 0

  var estimateRDD: RDD[EstimateTuple] = null
  var richSummaryRDD: RDD[RichSummaryTuple] = null

  //  var sampleTable: Array[DataTuple] = null
  //  var bSampleTable: Broadcast[Array[DataTuple]] = null

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
      .sortByKey(false)
      .map(pair => pair._2)
  }

  def computeLCA() {
    correctedPatternRDD = estimateRDD
      .flatMap(
        t => {
          val ret = ListBuffer.empty[(String, Array[Double])]
          val pair = (t.dataTuple.attributes.mkString("-"), Array(1.0, t.dataTuple.p, t.q))
          ret += pair
          generateAncestors(t.dataTuple.attributes, 0)
            .toList
            .foreach(
              ancestor => {
                val pair = (ancestor.content.mkString("-"), Array(1.0, t.dataTuple.p, t.q))
                ret += pair
              }
            )
          ret.toList
        }
      )
//      .mapPartitions(
//        iter => {
//          val lookup = mutable.Map.empty[String, Array[Double]]
//          while(iter.hasNext) {
//            val t = iter.next()
//            val ancestors = generateAncestors(t.dataTuple.attributes, 0)
//            ancestors.foreach(
//              pattern => {
//                val key = pattern.content.mkString("-")
//                lookup.get(key) match {
//                  case Some(aggregates) => {
//                    aggregates(0) += 1.0
//                    aggregates(1) += t.dataTuple.p
//                    aggregates(2) += t.q
//                    lookup.update(key, aggregates)
//                  }
//                  case None => {
//                    lookup(key) = Array(1.0, t.dataTuple.p, t.q)
//                  }
//                }
//              }
//            )
//          }
//          lookup.view.toList.iterator
//        }
//      )
      .reduceByKey(
        (left, right) => {
          (left, right).zipped map (_ + _)
        }, 256
      )
      .map(
        pair => {
          val count = pair._2(0).toLong
          val p = pair._2(1)
          val q = pair._2(2)
          val numSampleMatch = 1
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
      .sortByKey(false)
      .map(pair => pair._2)
  }

  def computeHierarchy() {
  }

  def computeCorrectedStats() {
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

        summaryTable.get(topPattern.id) match {
          case Some(tuple) => {
            tuple.multiplier = multiplier
            summaryTable.update(topPattern.id, tuple)
          }
          case None => {
            Console.err.println("Summary ID not found!")
          }
        }

        estimateRDD.unpersist()
      }
    }
  }

  def measureKL(): Double = {
    iterativeScaling()
    estimateRDD.map(t => computeKL(t)).reduce(_ + _)
  }

  def buildTable() {
    val entry_time = System.currentTimeMillis()
    var start_time: Long = 0
    var end_time: Long = 0
    loadConfig()

    prepareData(128)
    inputDataSize = inputDataRDD.count()
    //    println("Data size: " + inputDataSize + " rows")
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

      val currentKL = estimateRDD.map(t => computeKL(t)).reduce(_+_)
      println("The current KL value:" + currentKL)
      statOutput ++= "The current KL value:" + currentKL + "\n"

      start_time = System.currentTimeMillis()
      //      sampleTable = inputDataRDD.sample(false, sampleTableSize.toDouble / inputDataSize, rand.nextInt(50000)).collect()

//      estimateRDD.cache()

      end_time = System.currentTimeMillis()
      println("Step 2: Sampling done. Time taken:" + (end_time - start_time))
      statOutput ++= "Step 2: Sampling done. Time taken:" + (end_time - start_time) + "\n"

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

      estimateRDD.unpersist()

      end_time = System.currentTimeMillis()
      println("Step 3: Generate new rules done. Time taken:" + (end_time - start_time))
      statOutput ++= "Step 3: Generate new rules done. Time taken:" + (end_time - start_time) + "\n"
    }

    statOutput ++= "Time spent on explanation table construction: " +
      (System.currentTimeMillis() - entry_time).toString + "\n"

    KL = measureKL()

    sc.stop()

    postProcess()
  }
}