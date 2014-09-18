import java.io.{OutputStreamWriter, BufferedWriter}
import java.net.URI

import core._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SchemaRDD, SQLContext}
import scala.util.control.Breaks._

class SparkSQLETB extends ExplanationTableBuilder {

  case class Input(id: Long,
                   p: Short,
                   a1: String,
                   a2: String,
                   a3: String,
                   a4: String,
                   a5: String,
                   a6: String,
                   a7: String,
                   a8: String,
                   a9: String)

  case class Summary(id: Long,
                     a1: String,
                     a2: String,
                     a3: String,
                     a4: String,
                     a5: String,
                     a6: String,
                     a7: String,
                     a8: String,
                     a9: String,
                     support: Double = 0,
                     observation: Double = 0,
                     var multiplier: Double = 0,
                     gain: Double = 0,
                     kl: Double = 1.0 / 0.0)

  case class Estimate(id: Long,
                      a1: String,
                      a2: String,
                      a3: String,
                      a4: String,
                      a5: String,
                      a6: String,
                      a7: String,
                      a8: String,
                      a9: String,
                      q: Float)

  var inputDataSize: Long = 0
  var diffThreshold: Double = 0

  var summaryRDD: RDD[Summary] = null
  var estimateRDD: SchemaRDD = null
  var richSummaryRDD: SchemaRDD = null

  override def prepareData(numPartitions: Int = 0) = {
    val bNumDataFields = sc.broadcast(numDataFields)
    val sourceData =
      if (numPartitions == 0)
        sc.textFile(hostAddress + workingDirectory + inputDataFileName)
      else
        sc.textFile(hostAddress + workingDirectory + inputDataFileName, numPartitions)
    inputDataRDD = sourceData
      .map(
        line => {
          val numDataFields = bNumDataFields.value
          parseInputLine(line, numDataFields)
        }
      )
    //    inputDataRDD.cache()
  }

  def prepareSummary() {
//    val topPattern = SummaryTuple(0, numDataFields)
//    for (i <- 0 until numDataFields) {
//      topPattern.pattern(i) = "*"
//    }
//    summaryTable(0) = topPattern
//    val topPattern = Summary(0, "*", "*", "*", "*", "*", "*", "*", "*", "*")
    val topPattern = Summary(0, null, null, null, null, null, null, null, null, null)
    summaryRDD = sc.parallelize(Array(topPattern))
  }

  def buildTable() {
    loadConfig()
    val sqlContext = new SQLContext(sc)

    import sqlContext._

    prepareData()
    inputDataRDD.registerAsTable("input")
    inputDataSize = inputDataRDD.count()
    //    println("Data size: " + inputDataSize + " rows")
    statOutput ++= "Data size: " + inputDataSize + " rows" + "\n"
    diffThreshold = inputDataSize / 5000 + 1
    //    println("diffThreshold:" + diffThreshold)
    statOutput ++= "diffThreshold:" + diffThreshold + "\n"

    prepareSummary()
    summaryRDD.registerAsTable("summary")

    for (i <- 0 until summaryTableSize) {

      summaryRDD.count()

      breakable {
        estimateRDD = sql(
          "SELECT input.id, input.p, input.a1, input.a2, input.a3, input.a4, input.a5, input.a6, input.a7, input.a8," +
            "input.a9, (2^(sum(multiplier)))/(2^(sum(multiplier))+1) as q " +
            "FROM input, summary " +
            "WHERE (summary.a1 IS NULL OR summary.a1 = input.a1) AND " +
            "(summary.a2 IS NULL OR summary.a2 = input.a2) AND " +
            "(summary.a3 IS NULL OR summary.a3 = input.a3) AND " +
            "(summary.a4 IS NULL OR summary.a4 = input.a4) AND " +
            "(summary.a5 IS NULL OR summary.a5 = input.a5) AND " +
            "(summary.a6 IS NULL OR summary.a6 = input.a6) AND " +
            "(summary.a7 IS NULL OR summary.a7 = input.a7) AND " +
            "(summary.a8 IS NULL OR summary.a8 = input.a8) AND " +
            "(summary.a9 IS NULL OR summary.a9 = input.a9) " +
            "GROUP BY input.id,input.p,input.a1,input.a2,input.a3,input.a4,input.a5,input.a6,input.a7,input.a8,input.a9"
        )

        estimateRDD.registerAsTable("estimate")

        statOutput ++= "estimateRDD.count(): " + estimateRDD.count() + "\n"

//        richSummaryRDD =
        richSummaryRDD = sql(
          "SELECT summ.a1,summ.a2,summ.a3,summ.a4,summ.a5,summ.a6,summ.a7,summ.a8,summ.a9,summ.id,summ.multiplier," +
            "avg(p) AS p, avg(q) AS q, count(*) AS ct," +
            "CASE " +
            "WHEN avg(p) = avg(q) THEN 0.0 " +
            "WHEN (avg(p) = 0.0 and avg(q) = 1.0) THEN count(*) " +
            "WHEN avg(p) = 0.0 THEN (count(*)) * log(2, 1.0/(1.0-avg(q))) " +
            "WHEN (avg(p) = 1.0 AND avg(q) = 0.0) THEN (count(*)) " +
            "WHEN avg(p) = 1.0 THEN (count(*)) * log(2, 1.0/avg(q)) " +
            "WHEN avg(q) = 0.0 THEN (count(*))*(avg(p))* 9 + (count(*))*(1-avg(p))*log(2, (1-avg(p))) " +
            "WHEN avg(q) = 1.0 THEN (count(*))*(avg(p))*log(2, avg(p)) + (count(*))*(1-avg(p)) * 9 " +
            "ELSE (count(*))*(avg(p))*log(2, avg(p)/avg(q)) + (count(*))*(1-avg(p))*log(2, (1-avg(p))/(1-avg(q))) " +
            "END " +
            "AS diff " +
            "FROM estimate AS est, summary AS summ " +
            "WHERE (summ.a1 IS NULL OR summ.a1 = est.a1) AND " +
            "(summ.a2 IS NULL OR summ.a2 = est.a2) AND " +
            "(summ.a3 IS NULL OR summ.a3 = est.a3) AND " +
            "(summ.a4 IS NULL OR summ.a4 = est.a4) AND " +
            "(summ.a5 IS NULL OR summ.a5 = est.a5) AND " +
            "(summ.a6 IS NULL OR summ.a6 = est.a6) AND " +
            "(summ.a7 IS NULL OR summ.a7 = est.a7) AND " +
            "(summ.a8 IS NULL OR summ.a8 = est.a8) AND " +
            "(summ.a9 IS NULL OR summ.a9 = est.a9) " +
            "GROUP BY summ.a1,summ.a2,summ.a3,summ.a4,summ.a5,summ.a6,summ.a7,summ.a8,summ.a9,summ.multiplier,summ.id"
        )

        statOutput ++= "richSummaryRDD.count" + richSummaryRDD.count() + "\n"

      }

    }

    sc.stop()

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
}
