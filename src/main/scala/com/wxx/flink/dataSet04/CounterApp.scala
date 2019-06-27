package com.wxx.flink.dataSet04

import org.apache.flink.api.common.accumulators.LongCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode

object CounterApp {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val data = env.fromElements("hadoop", "spark", "flink", "pySpark", "storm")
    // 并行度后计数不准确
//    data.map(new RichMapFunction[String, Long] {
//      var counter = 0L
//      override def map(in: String): Long = {
//        counter = counter + 1
//        print(counter)
//        counter
//      }
//    }).setParallelism(4).print()
    val info = data.map(new RichMapFunction[String, String]() {
      var counter = new LongCounter()

      override def open(parameters: Configuration): Unit = {
        getRuntimeContext.addAccumulator("ele-counts-scala", counter)
      }
      override def map(in: String) : String = {
        counter.add(1)
        in
      }
    })
    val filepath= "G:\\data\\flink\\out-counter"
    info.writeAsText(filepath, WriteMode.OVERWRITE)
    val jobResult = env.execute("CounterApp")
    val num = jobResult.getAccumulatorResult[Long]("ele-counts-scala")
    println("num = " + num)
  }
}