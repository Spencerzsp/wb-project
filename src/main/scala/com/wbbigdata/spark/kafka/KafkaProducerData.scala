package com.wbbigdata.spark.kafka

import java.util.Properties

import com.typesafe.config.ConfigFactory
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import org.apache.log4j.{Level, Logger}
import org.codehaus.jettison.json.JSONObject

import scala.util.Random

/**
  * 模拟向kafka中写入数据
  */
object KafkaProducerData {

  Logger.getRootLogger.setLevel(Level.WARN)

  private val time = Array(
    "1503364335202", "1503364335776",
    "1503364336578", "1503364337536",
    "1503364336340", "1503364335832",
    "1503364336726", "1503364336387",
    "1503364336691", "1503364335857"
  )
  private val longitude = Array(
    20.2, 77.6,
    57.8, 53.6,
    34.0, 83.2,
    72.6, 38.7,
    69.1, 85.7
  )
  private val latitude = Array(
    10.2, 67.6,
    47.8, 33.6,
    24.0, 73.2,
    62.6, 48.7,
    59.1, 95.7
  )
  private val openid = Array(
    "b2e4a1bb-741b-4700-8b03-18c06a298", "b2e4a1bb-741b-4700-8b03-18c06a232",
    "b2e4a1bb-741b-4700-8b03-18c06a224", "b2e4a1bb-741b-4700-8b03-18c06a258",
    "b2e4a1bb-741b-4700-8b03-18c06a280", "b2e4a1bb-741b-4700-8b03-18c06a248",
    "b2e4a1bb-741b-4700-8b03-18c06a275", "b2e4a1bb-741b-4700-8b03-18c06a266",
    "b2e4a1bb-741b-4700-8b03-18c06a268", "b2e4a1bb-741b-4700-8b03-18c06a212"
  )
  private val page = Array(

    "v2ff37ca54eda70e0c1b8902626cb6dd", "fb751fb989ce159e3ee5149927176474",
    "af89557c629d6b7af43378df4b8f30d9", "n3f164f9e9999eefa13064ac1e864fd8",
    "zbd6f5791a99249c3a513b21ce835038", "dc6470493c3c891db6f63326b19ef482",
    "k2917b1b391186ff8f032f4326778ef7", "ca796f74ee74360e169fc290f1e720c7",
    "h981bd53a475b4edc9b0ad5f72870b03", "p4064d445c9f4ff4d536dfeae965aa95"
  )
  private val evnet_type = Array(
    2, 1,
    1, 2,
    1, 2,
    2, 1,
    2, 1
  )
  private val random = new Random()

  def point(): Int ={
    random.nextInt(10)
  }
  def getTime(): String ={
    time(point)
  }
  def getLongitude(): Double ={
    longitude(point)
  }
  def getLatitude(): Double ={
    latitude(point)
  }
  def getOpenid(): String ={
    openid(point)
  }
  def getPage(): String ={
    page(point)
  }
  def getEvnet_type(): Int ={
    evnet_type(point)
  }

  def main(args: Array[String]): Unit = {

    implicit val conf = ConfigFactory.load()
    val topic = "access"
    val props = new Properties()
    props.put("metadata.broker.list", "wbbigdata00:9092")
    props.put("serializer.class", "kafka.serializer.StringEncoder")

    val producerConfig = new ProducerConfig(props)
    val producer = new Producer[String, String](producerConfig)

    while (true){
      val event = new JSONObject()
      event.put("time", getTime())
        .put("longitude", getLongitude())
        .put("latitude", getLatitude())
        .put("openid",getOpenid())
        .put("page", getPage())
        .put("evnet_type", getEvnet_type())
      val key = new Random().nextInt().toString
      producer.send(new KeyedMessage[String, String](topic, key, event.toString))
      println("Message sent: " + event)

      Thread.sleep(1000)
    }

  }
}
