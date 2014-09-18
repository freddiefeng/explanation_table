import java.io.{OutputStreamWriter, BufferedWriter}
import java.net.URI
import java.util.Properties

import core._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable
import scala.util.Random

trait ExplanationTableBuilder {
  var numDataFields: Int = 0
  var workingDirectory: String = null
  var metricsConfFile: String = null
  var inputDataFileName: String = null
  var sparkMaster: String = null
  var hostAddress: String = null

  val statOutput = new StringBuilder
  var KL: Double = -1

  var conf: SparkConf = null
  var sc: SparkContext = null
  var hdfs: FileSystem = null

  var summaryTableSize: Int = 0
  var sampleTableSize: Long = 0

  var inputDataRDD: RDD[DataTuple] = null

  var summaryTable: mutable.Map[Int, SummaryTuple] = mutable.Map()

  val rand = new Random(System.currentTimeMillis())

  def loadConfig() {
    val appProperties = new Properties()
    appProperties.load(getClass.getResourceAsStream("config.properties"))
    summaryTableSize = appProperties.getProperty("summaryTableSize").toInt
    sampleTableSize = appProperties.getProperty("sampleTableSize").toInt
    hostAddress = appProperties.getProperty("hostAddress")
    workingDirectory = appProperties.getProperty("workingDirectory")
    if (!(workingDirectory.takeRight(1) equals "/"))
      workingDirectory += "/"
    metricsConfFile = appProperties.getProperty("metricsConfFile")
    inputDataFileName = appProperties.getProperty("inputDataFileName")
    sparkMaster = appProperties.getProperty("sparkMaster")
    numDataFields = appProperties.getProperty("numDataFields").toInt

    conf = new SparkConf()
      .setAppName("Explanation Table")
      .setMaster(sparkMaster)
//      .set("spark.executor.memory", "3g")
      .set("spark.locality.wait", "30000")
      .set("spark.executor.extraJavaOptions", "-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps")
      .set("spark.eventLog.enabled", "true")
      .set("spark.eventLog.dir", hostAddress + workingDirectory + "/sparkevents")
//      .set("spark.metrics.conf", metricsConfFile)
    sc = new SparkContext(conf)
  }

  def prepareData(numPartitions: Int = 0) {
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
    inputDataRDD.cache()
  }

  def buildTable()

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
}
