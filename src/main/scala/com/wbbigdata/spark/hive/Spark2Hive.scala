package com.wbbigdata.spark.hive

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

object Spark2Hive {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
//      .appName("Spark2Hive")
//      .master("local")
      .enableHiveSupport()
      .getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val df1 = spark.sql(
      """-- 乡镇扶贫类型分布
    SELECT town.name, detailed.title, count(*) as count
    FROM dw_helping_poor.helping_poor_detailed detailed JOIN dw_helping_poor.helping_poor_town town on detailed.town_code = town.town_code
    GROUP BY detailed.title, town.name ORDER BY count DESC
      """.stripMargin)

    val schema1 = df1.schema.add(StructField("id", LongType))
    val tempRDD1 = df1.rdd.zipWithIndex()
    val rowRDD1 = tempRDD1.map(x => {
      Row.merge(x._1, Row(x._2))
    })
    val result1 = spark.createDataFrame(rowRDD1, schema1)
    println(result1.count)

    result1.write
      .format("jdbc")
      .option("url", "jdbc:mysql://wbbigdata01:3306/test?useUnicode=true&characterEncoding=utf8&rewriteBatchedStatements=true")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "bigdata")
      .option("dbtable", "town_title_analy")
      .mode(saveMode = SaveMode.Overwrite)
      .option("truncate","true")
      .option("batchsize",10000)
      .option("isolationLevel","NONE")
      .save()

    println("result1写入数据到mysql完成")

    val df2 = spark.sql(
      """
        -- 统计乡镇贫困户类型、主要致贫原因、脱贫状态并排序
        SELECT town.name, house.type, house.zhipin, house.tuopin_status, count(*) as count
        FROM dw_helping_poor.helping_poor_town town JOIN dw_helping_poor.helping_poor_house house on town.town_code = house.town_code
        GROUP BY town.name, house.type, house.zhipin, house.tuopin_status ORDER BY count DESC
      """.stripMargin)

    val schema2 = df2.schema.add(StructField("id", LongType))
    val tempRDD2 = df2.rdd.zipWithIndex()
    val rowRDD2 = tempRDD2.map(x => {
      Row.merge(x._1, Row(x._2))
    })
    val result2 = spark.createDataFrame(rowRDD2, schema2)
    println(result2.count())

    result2.write
      .format("jdbc")
      .option("url", "jdbc:mysql://wbbigdata01:3306/test?useUnicode=true&characterEncoding=utf8&rewriteBatchedStatements=true")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "bigdata")
      .option("dbtable", "town_details_analy")
      .mode(saveMode = SaveMode.Overwrite)
      .option("truncate","true")
      .option("batchsize",10000)
      .option("isolationLevel","NONE")
      .save()

    println("result2写入数据到mysql完成")

    val df3 = spark.sql(
      """
        -- 统计乡镇和村贫困人口、贫困户数
        SELECT town.name town_name, village.name village_name, town.poor_house_num town_poor_house_num, town.poor_person_num town_poor_person_num, village.poor_house_num village_poor_house_num, village.poor_person_num village_poor_person_num
        from dw_helping_poor.helping_poor_town town JOIN dw_helping_poor.helping_poor_village village ON town.town_code = village.town_code
      """.stripMargin)

    val schema3 = df3.schema.add(StructField("id", LongType))
    val tempRDD3 = df3.rdd.zipWithIndex()
    val rowRDD3 = tempRDD3.map(x => {
      Row.merge(x._1, Row(x._2))
    })
    val result3 = spark.createDataFrame(rowRDD3, schema3)
    println(result3.count())

    result3.write
      .format("jdbc")
      .option("url", "jdbc:mysql://wbbigdata01:3306/test?useUnicode=true&characterEncoding=utf8&rewriteBatchedStatements=true")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "bigdata")
      .option("dbtable", "town_village_analy")
      .mode(saveMode = SaveMode.Overwrite)
      .option("truncate","true")
      .option("batchsize",10000)
      .option("isolationLevel","NONE")
      .save()

    println("result3写入数据到mysql完成")

    spark.stop()
  }

}
