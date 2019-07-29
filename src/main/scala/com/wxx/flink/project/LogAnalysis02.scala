package com.wxx.flink.project

import java.text.SimpleDateFormat
import java.util
import java.util.Properties

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
import org.slf4j.LoggerFactory

import scala.collection.mutable

object LogAnalysis02 {
  def main(args: Array[String]): Unit = {
    val logger = LoggerFactory.getLogger("LogAnalysis")
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    import org.apache.flink.api.scala._
    val tocpic = "pktest"

    val properties = new Properties()

    properties.setProperty("boostrap.servers", "192.168.31.100:9092")
    properties.setProperty("group.id", "test-pk-test")
    val consumer = new FlinkKafkaConsumer[String](tocpic, new SimpleStringSchema(), properties)

    val data = env.addSource(consumer)

    val logData = data.map(x =>{
      val splits = x.split("\t")
      val level = splits(2)
      val timeStr = splits(3)
      var time = 0l

      try{
        val sourceFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        time = sourceFormat.parse(timeStr).getTime
      }catch {
        case e:Exception =>{
          logger.error(s"time parse error : $timeStr" , e.getMessage)
        }
      }
      val domain = splits(5)
      val traffic = splits(6).toLong
      (level, time, domain, traffic)
    }).filter(_._2 != 0).filter(_._1 == "E")
      .map(x =>{
      (x._2, x._3, x._4)
    })

    val mysqlData = env.addSource(new PKMySQLSource)

    mysqlData.print()

    val connectData = logData.connect(mysqlData)
      .flatMap(new CoFlatMapFunction[(Long, String, Long), mutable.HashMap[String, String],String] {
        var userDomainMap = mutable.HashMap[String, String]()
        //log
        override def flatMap1(value: (Long, String, Long), out: Collector[String]) = {
          val domain = value._2
          val userId = userDomainMap.getOrElse(domain, "")

          println("------" + userId)

          out.collect(value._1 + "\t" + value._2 + "\t" + value._3 + "\t" + userId)
        }
        //mysql
        override def flatMap2(value: mutable.HashMap[String, String], out: Collector[String]) = {
          userDomainMap = value
        }
      })
    connectData.print()
    env.execute("LogAnalysis02")
  }



}
