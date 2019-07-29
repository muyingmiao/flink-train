package com.wxx.flink.connetcor08

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper

object KafkaConnectorProducerApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    // nc -lk 9999
    val data = env.socketTextStream("192.168.31.100",9999)

    val topic = "pktest"
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","192.168.31.100:9092")
//      val kafkaSink = new FlinkKafkaProducer[String](topic,
//      new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema()), properties)
    val kafkaSink = new FlinkKafkaProducer[String](topic,
   new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema()), properties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE)
    data.addSink(kafkaSink)
    env.execute("KafkaConnectorProducerApp")
  }
}
