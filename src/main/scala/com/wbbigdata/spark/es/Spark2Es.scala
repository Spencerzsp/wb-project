package com.wbbigdata.spark.es

import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.sql.EsSparkSQL

object Spark2Es {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark2Es")
      .master("local")
      .config("es.index.auto.create", "true")
      .config("es.nodes", "wbbigdata00:9200")
      .config("es.nodes.wan.only", "true")
      .config("es.cluster.name", "my-application")
      .getOrCreate()

    val sc = spark.sparkContext

    val studentDF = EsSparkSQL.esDF(spark, "/person")
    studentDF.show()

  }
}
