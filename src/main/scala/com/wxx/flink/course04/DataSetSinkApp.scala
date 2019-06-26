package com.wxx.flink.course04

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.FileSystem.WriteMode

object DataSetSinkApp {
  def main(args: Array[String]): Unit = {

    import org.apache.flink.api.scala._
    val env = ExecutionEnvironment.getExecutionEnvironment
    val data = 1 to 10
    val text = env.fromCollection(data)

    val filepath = "G:\\data\\flink\\out"

    text.writeAsText(filepath, WriteMode.OVERWRITE).setParallelism(2)
    env.execute("DataSetSinkApp")
  }

}
