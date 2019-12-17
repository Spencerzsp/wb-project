package com.wbbigdata.spark.hive

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Spark2Hive {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark2Hive")
      .master("local")
      .enableHiveSupport()
      .getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val databases = spark.sql("show databases")
//    databases.show()

    val df = spark.sql("select * from ods_helping_poor.ods_helping_poor2detailed")
    val table = df.createOrReplaceTempView("detailed")
    spark.sql("select * from detailed").show()

  }

}
