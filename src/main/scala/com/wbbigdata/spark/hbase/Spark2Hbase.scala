package com.wbbigdata.spark.hbase

import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, HBaseAdmin}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Spark2Hbase {

  def getConnecttion(): Connection ={
    val conf = HBaseConfiguration.create()
    val connection = ConnectionFactory.createConnection(conf)
    connection
  }



  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Spark2Hbase")
      .master("local")
      .getOrCreate()

    val sc = spark.sparkContext

    Logger.getRootLogger.setLevel(Level.WARN)

    val connection = getConnecttion()
    val admin = connection.getAdmin
    val tableNames = admin.listTableNames()
    println(tableNames.mkString("[", ",", "]"))

    val tableName = "student"
    val conf = Spark2Hbase.getConnecttion().getConfiguration
    conf.set(TableInputFormat.INPUT_TABLE, tableName)

//    if(!admin.isTableAvailable(tableName)){
//      val tableDec = new HTableDescriptor(TableName.valueOf(tableName))
//      admin.createTable(tableDec)
//    }

//    读取数据并转换为RDD
    val hbaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
    classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
    classOf[org.apache.hadoop.hbase.client.Result])

//    hbaseRDD.foreach(println)

    hbaseRDD.foreach{case (_, result) => {
        //获取行键
      val key = Bytes.toString(result.getRow)
      //通过列族和列名获取列,且必须全部将字节转换成字符串
      val name = Bytes.toString(result.getValue("cf".getBytes,"name".getBytes))
      val age = Bytes.toString(result.getValue("cf".getBytes,"age".getBytes))
      println("Row key:"+key+" Name:"+name+" Age:"+age)
      }
    }
  }


}
