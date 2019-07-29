package com.wxx.flink.project.source

import com.wxx.flink.project.PKMySQLSource
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object PKMySQLSourceTest {
  def main(args: Array[String]): Unit = {
    import org.apache.flink.api.scala._

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val data = env.addSource(new PKMySQLSource).setParallelism(1)

    data.print()

    env.execute("PKMySQLSourceTest")
  }
}
