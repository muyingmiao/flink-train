package com.wxx.flink.timeAndWindow07

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

object WindowsApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("192.168.31.171",9999)

    import org.apache.flink.api.scala._
    val data = text.flatMap(_.split(","))
        .map(x => (1,x))
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .reduce((v1,v2) => (v1._1, v1._2 + v2._2))
      .print()
      .setParallelism(1)
    env.execute("WindowsApp")
  }
}
