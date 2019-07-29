package com.wxx.flink.project

import java.text.SimpleDateFormat
import java.util
import java.util.{Date, Properties}

import akka.remote.serialization.StringSerializer
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
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

import scala.collection.mutable.ArrayBuffer

object LogAnalysis {

  val logger = LoggerFactory.getLogger("LogAnalysis")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    import org.apache.flink.api.scala._
    val tocpic = "pktest"

    val properties = new Properties()

    properties.setProperty("bootstrap.servers", "192.168.31.100:9092")
    properties.setProperty("group.id", "test-pk-test")
    val consumer = new FlinkKafkaConsumer[String](tocpic, new SimpleStringSchema(), properties)

    val data = env.addSource(consumer)
    val logData = data.map(x => {
      val splits = x.split("\t")

      val level = splits(2)
      val timeStr = splits(3)

      var time = 0l
      try {
        val sourceFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        time = sourceFormat.parse(timeStr).getTime
      } catch {
        case e: Exception => {
          logger.error(s"time parse error: $timeStr", e.getMessage)
        }
      }

      val domain = splits(5)
      val traffic = splits(6).toLong
      (level, time, domain, traffic)
    }).filter(_._2 != 0).filter(_._1 == "E")
      .map( x =>{
        (x._2, x._3, x._4)
      })
    //logData.print().setParallelism(1)
    val resultData = logData.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(Long, String, Long)] {

      val maxOutOfOrderness = 10000L // 3.5 seconds

      var currentMaxTimestamp: Long = _

      override def getCurrentWatermark = {
        new Watermark(currentMaxTimestamp - maxOutOfOrderness)
      }

      override def extractTimestamp(element: (Long, String, Long), previousElementTimestamp: Long) = {
        val timestamp = element._1
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
        timestamp
      }
    }).keyBy(1)
      .window(TumblingEventTimeWindows.of(Time.seconds(60)))
      .apply(new WindowFunction[(Long, String, Long), (String, String, Long), Tuple, TimeWindow ] {
        override def apply(key: Tuple, window: TimeWindow, input: Iterable[(Long, String, Long)], out: Collector[(String, String, Long)]): Unit = {

          val domain = key.getField(0).toString

          var sum = 0l
          val times = ArrayBuffer[Long]()
          val iterator = input.iterator
          while (iterator.hasNext){
            val next = iterator.next()
            sum += next._3
            times.append(next._1)
          }
          val time = new SimpleDateFormat("yyyy-MM-dd HH:mm").format(new Date(times.max))
          out.collect(time, domain, sum)
        }
      })


    val httpHosts = new java.util.ArrayList[HttpHost]
    httpHosts.add(new HttpHost("192.168.31.100", 9200, "http"))

    val esSinkBuilder = new ElasticsearchSink.Builder[(String, String, Long)](//.Builer[(String, String, Long)](
      httpHosts,
      new ElasticsearchSinkFunction[(String, String, Long)] {
        override def process(t: (String, String, Long), runtimeContext: RuntimeContext, requestIndexer: RequestIndexer) = {
          requestIndexer.add(createIndexRequest(t))
        }

        def createIndexRequest(element: (String, String, Long)): IndexRequest = {
          val json = new java.util.HashMap[String, Any]
          json.put("time", element._1)
          json.put("domain", element._2)
          json.put("traffics", element._3)
          val id = element._1 + "-" + element._2
          return Requests.indexRequest()
            .index("cdn")
            .`type`("traffic")
             .id(id)
            .source(json)
        }
      }
    )

    esSinkBuilder.setBulkFlushMaxActions(1)
    resultData.addSink(esSinkBuilder.build())
//    resultData.print().setParallelism(1)





    env.execute("LogAnalysis")
    /*
    val logger = LoggerFactory.getLogger("LogAnalysis")


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

//    logData.print().setParallelism(1)
   val resultData = logData.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(Long, String, Long)] {
      val maxOutOfOrderness = 10000L

      var currentMaxTimestamp : Long = _
      override def getCurrentWatermark: Watermark = {
        new Watermark(currentMaxTimestamp - maxOutOfOrderness)
      }

      override def extractTimestamp(element: (Long, String, Long), l: Long): Long = {
        val timestamp = element._1
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
        timestamp
      }
    }).keyBy(1)
      .window(TumblingEventTimeWindows.of(Time.seconds(60)))
      .apply(new WindowFunction[(Long, String ,Long),(String, String ,Long),Tuple, TimeWindow] {
        override def apply(key: Tuple, window: TimeWindow, input: Iterable[(Long, String, Long)], out: Collector[(String, String, Long)]): Unit = {
          val domain = key.getField(0).toString
          var sum = 0;
          val times = ArrayBuffer[Long]()
          val iterator = input.iterator
          while (iterator.hasNext){
            val next = iterator.next()
            sum += next._3
            times.append(next._1)
          }
          val time = new SimpleDateFormat("yyyy-MM-dd HH:mm").format(new Date(times.max))
          out.collect((time,domain,sum))
        }
      })

    val httpHosts = new util.ArrayList[HttpHost]
    httpHosts.add(new HttpHost("192.168.31.100",9200,"http"))

    val esSinkBulider = new ElasticsearchSink.Builder[(String, String ,Long)](
      httpHosts,
      new ElasticsearchSinkFunction[(String, String ,Long)] {
       def createIndexFunction(element : (String, String ,Long)) : IndexRequest = {
          val json = new java.util.HashMap[String, Any]
          json.put("time", element._1)
          json.put("domain", element._2)
          json.put("traffics", element._3)
          val id = element._1 + "-" + element._2
          return Requests.indexRequest()
           .index("cdn")
           .`type`("traffic")
           .id(id)
           .source(json)
       }

        override def process(t: (String, String, Long), runtimeContext: RuntimeContext, requestIndexer: RequestIndexer) = {
          requestIndexer.add(createIndexFunction(t))
        }
      }
    )

    esSinkBulider.setBulkFlushMaxActions(1)
    resultData.addSink(esSinkBulider.build())
    env.execute("LogAnalysis")
  */}

}
