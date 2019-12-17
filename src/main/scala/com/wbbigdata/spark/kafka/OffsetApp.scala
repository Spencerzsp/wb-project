package com.wbbigdata.spark.kafka

import java.lang

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalikejdbc.config.DBs
import scalikejdbc._

/**
  * spark streaming消费kafka数据，并将offset保存到mysql中
  */
object OffsetApp {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("OffsetApp")
      .setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(10))

    // 设置检查点
//    ssc.checkpoint("/input")

    // TODO spark streaming对接kafka
    // topics: Iterable[jl.String],
    // kafkaParams: collection.Map[String, Object]): ConsumerStrategy[K, V]
    val topics = Array("test_offset_topic","access")
    val kafkaParams = Map(
      "bootstrap.servers" -> "wbbigdata00:9092",
      "group.id" -> "test.group.id",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: lang.Boolean)
    )

    // 接收kafka数据

    // TODO 1.获取偏移量
    // TODO 1.1 读取mysql/zk/kafka/redis/hbase表中存放的offset数据

    DBs.setup() // 初始化
    val fromOffsets = DB.readOnly(implicit session => {
      SQL("select * from test.kafka_offset").map(rs => {
        (new TopicPartition(rs.string("topic"), rs.int("partition_id")), rs.long("untilOffset"))
      }).list().apply()
    }).toMap

    val stream:InputDStream[ConsumerRecord[String, String]] = if(fromOffsets.isEmpty){ // 第一次开始运行，从earliest开始消费
      KafkaUtils.createDirectStream(
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))
    } else { // 已存在offset
        KafkaUtils.createDirectStream(
          ssc,
          LocationStrategies.PreferConsistent,
          ConsumerStrategies.Subscribe[String, String](topics, kafkaParams, fromOffsets)
//          ConsumerStrategies.Assign[String, String](fromOffsets.keys.toList, kafkaParams, fromOffsets)
        )
    }

    // 获取当前批次的offset信息进行消费，消费完成后更新offset信息保存到mysql
    var offsetRanges: Array[OffsetRange] = Array.empty
    stream.transform(rdd => {
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition(partition => {
        // TODO 计算逻辑
        val o: OffsetRange = offsetRanges(TaskContext.get().partitionId())
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
        partition.foreach(pair => {
          println(pair.key(), pair.value())
        })
      })
      rdd
    })
      .map(x => {
      (x.key(), x.value())
    }).foreachRDD(rdd => {
//      rdd.foreach(println)
      // 遍历不同分区的offset信息，并更新到mysql中
      offsetRanges.foreach(x => {
        DB.autoCommit(implicit session => {
          SQL("replace into test.kafka_offset(topic, group_id, partition_id, fromOffset, untilOffset) values(?, ?, ?, ?, ?)")
          .bind(x.topic, "random", x.partition, x.fromOffset, x.untilOffset)
          .update().apply()
        })
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
