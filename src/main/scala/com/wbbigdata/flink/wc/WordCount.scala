package com.wbbigdata.flink.wc

import org.apache.flink.api.scala._

object WordCount {
  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // flink中和spark读取hdfs上文件不一样，必须跟上完整路径名
    val inputData = env.readTextFile("hdfs://wbbigdata00:8020/input/test.txt")
    val wordCount = inputData.flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)

    wordCount.print()
  }

}
