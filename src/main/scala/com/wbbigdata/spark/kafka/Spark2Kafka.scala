package com.wbbigdata.spark.kafka

import java.lang

import com.alibaba.fastjson.{JSON, JSONException}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory


object Spark2Kafka {

  private val logger = LoggerFactory.getLogger(Spark2Kafka.getClass)

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.WARN)

    // 设置运行时的用户
    System.setProperty("HADOOP_USER_NAME", "hdfs")

    val conf = new SparkConf()
      .setAppName("Spark2Kafka")
      .setMaster("local")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    val sc = spark.sparkContext

    val ssc = new StreamingContext(sc, Seconds(5))

    // 直连方式相当于跟kafka的topic直接连接
    // "auto.offset.reset" -> "earliest"(每次重启重新开始消费)，"auto.offset.reset" -> "latest"(每次重启从最新的offset消费)
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "wbbigdata00:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "fwmagic",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: lang.Boolean)
    )

    val topics = Array("access")

    val kafkaDStream = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe(topics, kafkaParams)
    )

    // 如果使用direct方式，生成的kafkaDStream必须调用foreachRDD
//    kafkaDStream.foreachRDD(kafkaRDD => {
//      if(!kafkaRDD.isEmpty()){
//        // 获取当前批次的RDD的偏移量
//        val offsetRanges = kafkaRDD.asInstanceOf[HasOffsetRanges].offsetRanges
//
//        // 拿出kafka中的数据
//        val lines = kafkaRDD.map(_.value())
//        // 将lines转换成json对象
//        val logBeanRDD = lines.map(line => {
//          var logBean: LogBean = null
//          try {
//            logBean = JSON.parseObject(line, classOf[LogBean])
//          } catch {
//            case e: JSONException => {
//              logger.error("json解析错误！line: " + line, e)
//            }
//          }
//          logBean
//        })
//        val filterRDD = logBeanRDD.filter(_ != null)
//
//        // 将filterRDD转换成DataFrame
//        import spark.implicits._
//        val df = filterRDD.toDF()
//        df.show()
//
//        // 将数据写到hdfs
//        df.repartition(1).write.mode(SaveMode.Append).parquet(args(0))
//
//        // 提交当前批次的偏移量，将偏移量最后写入kafka
//        kafkaDStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
//      }
//    })

    // 启动
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()

  }
}

case class LogBean(
  time:String,
  longitude:Double,
  latitude:Double,
  openid:String,
  page:String,
  evnet_type:Int
)
