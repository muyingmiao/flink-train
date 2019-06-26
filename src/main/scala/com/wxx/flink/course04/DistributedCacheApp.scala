package com.wxx.flink.course04

import org.apache.commons.io.FileUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration


object DistributedCacheApp {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val filePath = "G:\\data\\flink\\input.txt"
    env.registerCachedFile(filePath, "local-scala-doc")
    import org.apache.flink.api.scala._
    val data = env.fromElements("hadoop","spark","flink","storm")
    data.map(new RichMapFunction[String, String] {
      override def open(parameters: Configuration): Unit = {
        val file = getRuntimeContext.getDistributedCache.getFile("local-scala-doc")
        val lines = FileUtils.readLines(file)
        // java集合和scala集合不兼容
        import scala.collection.JavaConverters._
        for(ele<- lines.asScala){
          println(ele)
        }
      }
      override def map(in: String) :String = {
        in
      }
    }).print()

  }

}
