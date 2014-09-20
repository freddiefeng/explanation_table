package hive

import java.io.{PrintWriter, File}
import java.sql.{ResultSet, Statement, DriverManager, Connection}
import java.util.Properties
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._
import scala.sys.process.{ProcessLogger, Process}

object HiveFlashlightETB {
  object BatchQuery {
//    val setting =
//      "set mapreduce.task.io.sort.mb=100;" +
//      "set hive.optimize.bucketmapjoin.sortedmerge=true;" +
//      "set hive.auto.convert.join=true;" +
//      "set hive.auto.convert.sortmerge.join=true;" +
//      "set hive.auto.convert.sortmerge.join.noconditionaltaks=true;"
    private var queries: String = ""

    def append(input: String): Unit = {
      queries += input + "; "
    }

    def commit() = {
      Process("hive", Seq("-e", queries))!

      queries = ""
    }
  }

  val appProperties = new Properties()
  appProperties.load(getClass.getClassLoader.getResourceAsStream("config.properties"))
  val driverName = "org.apache.hive.jdbc.HiveDriver"
  val tableName: String = appProperties.getProperty("tableName")
  var numSummary: Int = appProperties.getProperty("numSummary").toInt
  var numSample: Int = appProperties.getProperty("numSample").toInt
  var hiveURL: String = appProperties.getProperty("hiveURL")

  var tableSize: Long = 0
  var diffThreshold: Double = 0

  val GainFormula = " \n CASE WHEN pat.p/ct = q/ct THEN 0.0 \n" +
//    "WHEN pat.p/ct = 0.0 AND (ct - q) < 0.000001 THEN (ct/count(*))*(1-pat.p)*log(2,(ct-pat.p)/0.000001) \n" +
    "WHEN pat.p/ct = 0.0 THEN (ct/count(*))*(1-pat.p)*log(2,(ct-pat.p)/(ct-q)) \n" +
//    "WHEN pat.p/ct = 1.0 AND q = 0.0 THEN (ct/count(*))*(pat.p/ct)*log(2, pat.p/0.000001) \n" +
    "WHEN pat.p/ct = 1.0 THEN (ct/count(*))*(pat.p/ct)*log(2, pat.p/q) \n" +
//    "WHEN (ct - q) < 0.000001 AND q = 0.0 THEN ((ct/count(*))*(1-pat.p/ct)*log(2, (ct-pat.p)/0.000001)) + ((ct/count(*))*(1-pat.p/ct)*log(2, (ct-pat.p)/0.000001)) \n" +
//    "WHEN (ct - q) < 0.000001 THEN ((ct/count(*))*(1-pat.p/ct)*log(2, (ct-pat.p)/0.000001)) + ((ct/count(*))*(1-pat.p/ct)*log(2, (ct-pat.p)/q)) \n" +
//    "WHEN q = 0.0 THEN ((ct/count(*))*(1-pat.p/ct)*log(2, (ct-pat.p)/(ct-q))) + ((ct/count(*))*(1-pat.p/ct)*log(2, (ct-pat.p)/0.000001)) \n" +
    "ELSE  ((ct/count(*))*(1-pat.p/ct)*log(2, (ct-pat.p)/(ct-q))) + \n" +
    "((ct/count(*))*(pat.p/ct)*log(2,pat.p/q)) \n" +
    "END"

  val DiffFormula = "\n CASE WHEN avg(p) = avg(q) THEN 0.0 \n" +
    "WHEN (avg(p) = 0.0 AND avg(q) = 1.0) THEN count(*) \n" +
    "WHEN avg(p) = 0.0 THEN count(*) * log(2, 1.0/(1.0-avg(q))) \n" +
    "WHEN (avg(p) = 1.0 AND avg(q) = 0.0) THEN count(*) \n" +
    "WHEN avg(p) = 1.0 THEN count(*) * log(2, 1.0/avg(q)) \n" +
    "WHEN avg(q) = 0.0 THEN count(*)*(avg(p))* 9 + count(*)*(1-avg(p))*log(2, (1-avg(p))) \n" +
    "WHEN avg(q) = 1.0 THEN count(*)*(avg(p))*log(2, avg(p)) + count(*)*(1-avg(p)) * 9 \n" +
    "ELSE count(*)*(avg(p))*log(2, avg(p)/avg(q)) + count(*)*(1-avg(p))*log(2, (1-avg(p))/(1-avg(q))) \n" +
    "END \n"

  val MultiplierFormula = "\n CASE WHEN p = q THEN multiplier \n" +
    "WHEN p = 0.0 THEN multiplier - 9 \n" +
    "WHEN q = 1.0 THEN multiplier - 9 \n" +
    "WHEN p = 1.0 THEN multiplier + 9 \n" +
    "WHEN q = 0.0 THEN multiplier + 9 \n" +
    "ELSE multiplier + log(2, p/q) + log(2, (1-q)/(1-p)) \n" +
    "END "

  def composeViewSql(table: String, which: String, select: String, from: String, where: String, gby: String): String = {
    var ret = "CREATE VIEW "+table+"_"+which+" AS SELECT "+select+" FROM "+ from
    if (where != null)
      ret += " WHERE "+where
    if (gby != null)
      ret += " GROUP BY "+gby
    ret
  }

  def executeSQLUpdate(sql: String, stmt: Statement) {
    println(sql)
    stmt.executeUpdate(sql)
  }

  def executeHiveShell(sql: String): Unit = {
    Process("hive", Seq("-e", sql))!
  }

  def main(args: Array[String]): Unit = {
    try {
      Class.forName(driverName)
    } catch {
      case e: ClassNotFoundException => {
        e.printStackTrace()
        System.exit(1)
      }
    }

//    val con: Connection = DriverManager.getConnection(hiveURL, "g2feng", "")
//    val stmt: Statement = con.createStatement
    var res: ResultSet = null
//    res = stmt.executeQuery("describe " + tableName)

//    val columnsBuffer = ListBuffer.empty[String]
//    while (res.next) {
//        columnsBuffer += res.getString(1)
//    }
//    val allColumns = columnsBuffer.toList
//    val columns = allColumns.filter(field => !(field.equals("id") || field.equals("p")))

    val allColumns = List("id", "p") ++ (1 until 10 toList).map("a" + _)
    val columns = allColumns.filter(field => !(field.equals("id") || field.equals("p")))

    var sql: String = null

//    sql = "add jar " + jarPath
//    println(sql)
//    stmt.executeUpdate(sql)
//
//    sql = "CREATE temporary function power_set AS 'etbutil.GenericUDTFPowerSet'"
//    println(sql)
//    stmt.executeUpdate(sql)

//    sql = "select count(*) from " + tableName
//    println(sql)
//    res = stmt.executeQuery(sql)
//    if(res.next()) {
//      tableSize = res.getString(1).toLong
//      println(tableSize)
//      diffThreshold = tableSize / 5000 + 1
//    }

    Process("hive", Seq("-e", "select count(*) from " + tableName)) ! ProcessLogger(
      (o: String) => {
        println("out " + o)
        tableSize = o.split("\\s+")(0).trim.toLong
      },
      (e: String) => println("err " + e)
    )

    diffThreshold = tableSize / 5000 + 1

    sql = "drop table " + tableName + "_summary"
    println(sql)
//    stmt.executeUpdate(sql)
    BatchQuery.append(sql)

    sql =
      "create table " + tableName + "_summary(" +
      columns.map(_ + " string, ").reduce(_+_) +
      "support float," +
      "observation float," +
      "multiplier float," +
      "gain float," +
      "id bigint," +
      "kl float" +
      ") ROW FORMAT DELIMITED FIELDS TERMINATED BY \",\" TBLPROPERTIES('serialization.null.format'='')"
    println(sql)
//    stmt.executeUpdate(sql)
    BatchQuery.append(sql)

    sql = "drop table " + tableName + "_sample"
    println(sql)
//    stmt.executeUpdate(sql)
//    executeHiveShell(sql)
    BatchQuery.append(sql)

    sql =
      "create table " + tableName + "_sample(" +
      columns.map(_ + " string, ").reduce(_+_).dropRight(2) +
      ")"
    println(sql)
//    stmt.executeUpdate(sql)
//    executeHiveShell(sql)
    BatchQuery.append(sql)

    //    stmt.executeUpdate()
    val summaryTable = tableName + "_summary"
    val sampleTable = tableName + "_sample"
    val estimateView = tableName + "_estimate"
    val maxpatsView = tableName + "_maxpats"
    val subsetView = tableName + "_subsets"
    val aggpatView = tableName + "_aggpats"
    val correctedView = tableName + "_corrected"
    val richSummaryView = tableName + "_richsummary"

    sql = "drop view " + estimateView
    println(sql)
//    stmt.executeUpdate(sql)
//    executeHiveShell(sql)
    BatchQuery.append(sql)

    val dataCols = columns.map(tableName + "." + _)
    var summCols = columns.map(summaryTable + "." + _)
    var where = (dataCols, summCols)
      .zipped
      .map(
          (left, right) => {
          " (" + right + " IS NULL OR " + right + " = " + left + ") and"
        }
    )
      .reduce(_+_)
      .dropRight(3)
    var colList = allColumns.map(tableName + "." + _ + ",").reduce(_ + _)
    var groupBy = colList.dropRight(1)
    var select = colList + "(pow(2, sum(multiplier)))/(pow(2, sum(multiplier))+1) as q"
    var from = tableName + ", " + summaryTable
    sql = composeViewSql(tableName, "estimate", select, from, where, groupBy)

    println(sql)
//    stmt.executeUpdate(sql)
//    executeHiveShell(sql)
    BatchQuery.append(sql)

    sql = "drop view " + maxpatsView
    println(sql)
//    stmt.executeUpdate(sql)
//    executeHiveShell(sql)
    BatchQuery.append(sql)

    var estimateCols = columns.map(estimateView + "." + _)
    val sampleCols = columns.map(sampleTable + "." + _)
    select = (columns, estimateCols, sampleCols)
      .zipped
      .map(
          (col, estCol, samCol) =>
        {
          "(CASE WHEN " + estCol + " = " + samCol +
          " THEN " + estCol +
          " ELSE NULL END) " + " AS o" + col + ", "
        }
    ).reduce(_+_) + " " + estimateView + ".q, "  + estimateView + ".p "
    //    + "count(*) as oct, sum(q) as sumq,sum(p) as sump"
    from = estimateView + ", " + sampleTable
    where = null
    groupBy = null
    val subquery = "(SELECT " + select + " FROM "+ from + ") AS tmp "

    //Maxpats
    select = columns.map("o" + _ + ", ").reduce(_+_) + "count(*) as oct, sum(q) as sumq,sum(p) as sump"
    from = subquery
    where = null
    groupBy = columns.map("o" + _ + ",").reduce(_+_).dropRight(1)
    sql = composeViewSql(tableName, "maxpats", select, from, where, groupBy)

    println(sql)
//    stmt.executeUpdate(sql)
//    executeHiveShell(sql)
    BatchQuery.append(sql)

    //Subsets
    sql = "CREATE VIEW " + subsetView + " AS " +
      " SELECT " + columns.map(x => "p" + x + " AS " + x + ", ").reduce(_+_) + " CAST(oct AS DOUBLE) AS oct, sump, sumq " +
      " FROM " + maxpatsView +
      " LATERAL VIEW power_set(" + columns.map("o" + _ + ", ").reduce(_+_).dropRight(2) + ") ps AS " +
      columns.map("p" + _ + ", ").reduce(_+_).dropRight(2)

    println("drop view " + subsetView)
//    stmt.executeUpdate("drop view " + subsetView)
//    executeHiveShell("drop view " + subsetView)
    BatchQuery.append("drop view " + subsetView)

    println(sql)
//    stmt.executeUpdate(sql)
    BatchQuery.append(sql)

    //Aggpats
    select = columns.map(_ + ", ").reduce(_+_) + " sum(oct) as ct, sum(sump) as p, sum(sumq) as q "
    from = subsetView
    where = null
    groupBy = columns.map(_ + ", ").reduce(_+_).dropRight(2)
    sql = composeViewSql(tableName, "aggpats", select, from, where, groupBy)

    println("drop view " + aggpatView)
//    stmt.executeUpdate("drop view " + aggpatView)
    BatchQuery.append("drop view " + aggpatView)
    println(sql)
//    stmt.executeUpdate(sql)
    BatchQuery.append(sql)

    //Corrected
    select = columns.map("pat." + _ + ", ").reduce(_+_) +
      " ct/cast(count(*) as double) as newct, p/cast(ct as double) as p, q/cast(ct as double) as q, " +
      GainFormula + " AS gain, " +
      "\n CASE WHEN p = q THEN 0.0 \n" +
      " WHEN p = 0.0 THEN -9 \n" +
      " WHEN q = 0.0 THEN 9 \n" +
      " ELSE log(2, p / q) \n" +
      " END AS multiplier \n"
    from = sampleTable + " samp, " + aggpatView + " pat"
    val patCols = columns.map("pat." + _)
    val sampCols = columns.map("samp." + _)
    where = (patCols, sampCols)
      .zipped
      .map(
        (left, right) => {
          " (" + right + " IS NULL OR " + right + " = " + left + ") and"
        }
      )
      .reduce(_+_)
      .dropRight(3)
    where = null
    groupBy = columns.map("pat." + _ + ", ").reduce(_+_) + "ct,p,q"
    sql = composeViewSql(tableName, "corrected", select, from, where, groupBy)

    println("drop view " + correctedView)
    BatchQuery.append("drop view " + correctedView)

    println(sql)
//    stmt.executeUpdate(sql)
    BatchQuery.append(sql)

    //Richsummary
    select = columns.map("summ." + _ + ", ").reduce(_+_) + "summ.id,summ.multiplier,avg(p) as p, avg(q) as q, count(*) as ct,"+
      DiffFormula + " AS diff"
    from = estimateView + " est, " + summaryTable + " summ "
    summCols = columns.map("summ." + _)
    estimateCols = columns.map("est." + _)
    where = (estimateCols, summCols)
      .zipped
      .map(
        (left, right) => {
          " (" + right + " IS NULL OR " + right + " = " + left + ") and"
        }
      )
      .reduce(_+_)
      .dropRight(3)
    groupBy = columns.map("summ." + _ + ", ").reduce(_+_) + "summ.multiplier,summ.id"
    sql = composeViewSql(tableName, "richsummary", select, from, where, groupBy)

    println("drop view " + richSummaryView)
    BatchQuery.append("drop view " + richSummaryView)

    println(sql)
//    stmt.executeUpdate(sql)
    BatchQuery.append(sql)

    sql = "truncate table " + tableName + "_summary"
    println("truncate table " + tableName + "_summary")
//    stmt.executeUpdate(sql)
    BatchQuery.append(sql)

//    Main algorithm starts here
    val temp: File = File.createTempFile("temp",".csv")
    temp.deleteOnExit()
    val writer = new PrintWriter(temp.getAbsolutePath, "UTF-8")
    writer.println(",,,,,,,,,0.0,0.0,0.0,0.0,0,999999")
    writer.close()

    sql = "LOAD DATA LOCAL INPATH '" + temp.getAbsolutePath + "' INTO TABLE " + summaryTable
    println(sql)
//    stmt.executeUpdate(sql)
    BatchQuery.append(sql)

    BatchQuery.commit()

    var tcquery = 0
    var tsnsr =0
    var tsample = 0
    var tkl = 0

    var t0 = System.currentTimeMillis()

    var out = Array.empty[String]
    val logger = ProcessLogger(
      (o: String) => {
        println("out " + o)
        out = o.split("\\s+")
      },
      (e: String) => println("err " + e)
    )

    println("----------------------------------------------------\nExplanation table:")
    println("----------------------------------------------------")

    sql = "DROP TABLE IF EXISTS temp"
    println(sql)
//    stmt.executeUpdate(sql)
    BatchQuery.append(sql)

    sql = "CREATE TABLE IF NOT EXISTS temp(id bigint, p float, q float, diff float) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','"
    println(sql)
//    stmt.executeUpdate(sql)
    BatchQuery.append(sql)

    BatchQuery.commit()

    for (curNumRules <- 1 to numSummary) {
      var ts = System.currentTimeMillis()
      sql = "TRUNCATE TABLE " + tableName + "_sample"
      println(sql)
//      stmt.executeUpdate(sql)
      BatchQuery.append(sql)

      var ta = System.currentTimeMillis()

      sql = "TRUNCATE TABLE temp"
      println(sql)
//      stmt.executeUpdate(sql)
      BatchQuery.append(sql)

      BatchQuery.commit()

      breakable {
        while(true) {
          sql = "FROM " + richSummaryView + " INSERT OVERWRITE TABLE temp SELECT id, p, q, diff " +
            " WHERE " + "diff > " + diffThreshold.toString +
            " ORDER BY " + "diff DESC LIMIT 1"
          println(sql)
//          stmt.executeUpdate(sql)
          BatchQuery.append(sql)
          BatchQuery.commit()

          sql = "SELECT id, p, q, diff FROM temp"
//          res = stmt.executeQuery(sql)
          Process("hive", Seq("-e", sql)) ! logger

//          sys.exit()

          if (out.length == 4) {
//            println(res.getString("id"))
//            println(res.getString("p"))
//            println(res.getString("q"))
//            println(res.getString("diff"))

            out.foreach(println(_))

//            sql = "UPDATE " + summaryTable +
//              " SET multiplier = " + MultiplierFormula +
//              " FROM temp WHERE temp.id = " + summaryTable + ".id"
//            println(sql)

            sql = "INSERT OVERWRITE TABLE " + summaryTable + "\n" +
              " SELECT " + columns.map("s." + _ + ", ").reduce(_+_) + " s.support, s.observation, \n" +
              " CASE WHEN temp.id = s.id THEN " + MultiplierFormula +
              " ELSE multiplier END\n AS multiplier, s.gain, s.id, s.kl " +
              " FROM " + summaryTable + " s, temp"
            println(sql)
            BatchQuery.append(sql)
            BatchQuery.commit()

            out = Array.empty[String]
          } else {
            break()
          }
        }
      }

      ts = System.currentTimeMillis()
      println("finished big convergence loop: " + (ts - ta))


      ta = System.currentTimeMillis()
      sql = "INSERT INTO TABLE " + sampleTable +
            " SELECT " + columns.map(_ + ", ").reduce(_+_).dropRight(2) + " FROM " + estimateView +
            " TABLESAMPLE (" + numSample.toDouble / tableSize.toDouble + " PERCENT) s"
      println(sql)
//      stmt.executeUpdate(sql)
      executeHiveShell(sql)
      ts = System.currentTimeMillis()
      println("finished sampling: " + (ts - ta))

      ta = System.currentTimeMillis()
      sql = "INSERT INTO TABLE " + summaryTable +
      " SELECT " + columns.map("top." + _ + ", ").reduce(_+_) + "top.newct, top.q, top.multiplier, 0, " + curNumRules + ", 999999 " +
      " FROM (SELECT * FROM " + correctedView + " ORDER BY gain DESC LIMIT 1) top "
      println(sql)

//      Process("hive", Seq("-e", "\"DROP FUNCTION IF EXISTS power_set;" +
//        "CREATE temporary function power_set AS 'etbutil.GenericUDTFPowerSet' USING JAR \'" + jarPath + "\';" +
//        sql))!

      executeHiveShell(sql)

      ts = System.currentTimeMillis()
      println("finished new summary: " + (ts - ta))

//      println("SELECT * FROM " + summaryTable + " WHERE id = " + curNumRules)
//      res = stmt.executeQuery(
//        "SELECT * FROM " + summaryTable + " WHERE id = " + curNumRules
//      )
//      val rsmd = res.getMetaData
//
//      val numberOfColumns = rsmd.getColumnCount
//
//      for (j <- 1 to numberOfColumns) {
//        if (j > 1) System.out.print(",  ")
//        val columnName = rsmd.getColumnName(j)
//        System.out.print(columnName)
//      }
//
//      System.out.println("")
//
//      while (res.next()) {
//        for (j <- 1 to numberOfColumns) {
//          if (j > 1) System.out.print(",  ")
//          val columnValue = res.getString(j)
//          System.out.print(columnValue)
//        }
//        System.out.println("")
//      }
    }

//    stmt.close()
//    con.close()
  }
}
