package com.wxx.flink.connetcor08

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object KafkaConnectorConsumerApp {

  def main(args: Array[String]): Unit = {
   /* val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(4000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(10000)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    import org.apache.flink.api.scala._
    val topic = "pktest"
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","192.168.31.100:9092")
    properties.setProperty("group.id","test")
    val data =  env.addSource(new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(),properties))
    data.print()
    env.execute("KafkaConnectorConsumerApp")*/
  }

}
