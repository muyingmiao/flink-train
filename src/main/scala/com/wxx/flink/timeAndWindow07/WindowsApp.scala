package com.wxx.flink.timeAndWindow07

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

object WindowsApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("192.168.31.171",9999)

    import org.apache.flink.api.scala._
    val data = text.flatMap(_.split(","))
      .map((_,1))
      .keyBy(0)
//      .timeWindow(Time.seconds(5))
      //sliding Window
        .timeWindow(Time.seconds(10), Time.seconds(5))
      .sum(0)
      .print()
      .setParallelism(1)
    env.execute("WindowsApp")
  }
}
