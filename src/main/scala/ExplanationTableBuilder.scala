import java.util.Properties

import core._
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

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
      .set("spark.executor.memory", "3g")
      .set("spark.executor.extraJavaOptions", "-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps")
      .set("spark.eventLog.enabled", "true")
      .set("spark.eventLog.dir", hostAddress + workingDirectory + "/sparkevents")
//      .set("spark.metrics.conf", metricsConfFile)
    sc = new SparkContext(conf)
  }

  def prepareData() {
    val bNumDataFields = sc.broadcast(numDataFields)
    val sourceData = sc.textFile(hostAddress + workingDirectory + inputDataFileName, 8).cache()
    inputDataRDD = sourceData
      .map(
        line => {
          val numDataFields = bNumDataFields.value
          parseInputLine(line, numDataFields)
        }
      )
  }

  def buildTable()
}
