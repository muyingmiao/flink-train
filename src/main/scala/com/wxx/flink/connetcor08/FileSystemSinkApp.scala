package com.wxx.flink.connetcor08

import java.time.ZoneId

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.fs.StringWriter
import org.apache.flink.streaming.connectors.fs.bucketing.{BucketingSink, DateTimeBucketer}

object FileSystemSinkApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val data = env.socketTextStream("192.168.31.100",9999)

    data.print().setParallelism(1)
    val filepath = "file:///G://data//flink//hdfssink"

    val sink = new BucketingSink[String](filepath)
    sink.setBucketer(new DateTimeBucketer[String]("yyyy-MM-dd--HHmm", ZoneId.of("America/Los_Angeles")))
    sink.setWriter(new StringWriter())
    //sink.setBatchSize(1024 * 1024 * 400) // this is 400 MB,
    //sink.setBatchRolloverInterval(20 * 60 * 1000); // thi
    sink.setBatchRolloverInterval(20)
    data.addSink(sink)
    env.execute("FileSystemSinkApp")
  }
}
