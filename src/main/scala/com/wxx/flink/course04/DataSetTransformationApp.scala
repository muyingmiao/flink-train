package com.wxx.flink.course04

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

import scala.collection.mutable.ListBuffer
object DataSetTransformationApp {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
//    mapfunction(env)
//    mapPartitionFunction(env)
//    firstFunction(env)
//    flatMapFunction(env)
//    distinctFunction(env)
//    joinFunction(env)
//    outerJoinFunction(env)
    crossFunction(env)
  }
   def crossFunction(env :ExecutionEnvironment): Unit ={

     val info1 = List("曼城","曼联")
     val info2 = List(3,1,0)
    val data1 = env.fromCollection(info1)
     val data2 = env.fromCollection(info2)
     data1.cross(data2).print()
   }

  def outerJoinFunction(env : ExecutionEnvironment): Unit ={
    val info1 = ListBuffer[(Int, String)]()
    info1.append((1, "AAA"))
    info1.append((2, "BBB"))
    info1.append((3, "CCC"))
    info1.append((4, "DDD"))

    val info2 = ListBuffer[(Int, String)]()
    info2.append((1, "北京"))
    info2.append((2, "上海"))
    info2.append((3, "深圳"))
    info2.append((5, "杭州"))

    val data1 = env.fromCollection(info1)
    val data2 = env.fromCollection(info2)

    //    data1.join(data2).where(0).equalTo(0).print()
//    data1.leftOuterJoin(data2).where(0).equalTo(0).apply((first, second) =>{
//      if(second == null){
//        (first._1,first._2,"-")
//      }else{
//        (first._1,first._2,second._2)
//      }
//    }).print()

//    data1.rightOuterJoin(data2).where(0).equalTo(0).apply((first, second) =>{
//      if(first == null){
//        (second._1,"-",second._2)
//      }else{
//        (second._1,first._2,second._2)
//      }
//    }).print()


    // fullOuterJoin
    data1.fullOuterJoin(data2).where(0).equalTo(0).apply((first, second) =>{
      if(first == null){
        (second._1,"-",second._2)
      }else if(second == null){
        (first._1,first._2,"-")
      }else{
        (second._1,first._2,second._2)
      }
    }).print()
  }

  def joinFunction(env : ExecutionEnvironment): Unit ={
    val info1 = ListBuffer[(Int, String)]()
    info1.append((1, "AAA"))
    info1.append((2, "BBB"))
    info1.append((3, "CCC"))
    info1.append((4, "DDD"))

    val info2 = ListBuffer[(Int, String)]()
    info2.append((1, "北京"))
    info2.append((2, "上海"))
    info2.append((3, "深圳"))
    info2.append((4, "杭州"))

    val data1 = env.fromCollection(info1)
    val data2 = env.fromCollection(info2)

//    data1.join(data2).where(0).equalTo(0).print()
    data1.join(data2).where(0).equalTo(0).apply((first, second) =>(first._1,second._2) ).print()
  }

  //distinct
  def distinctFunction(env : ExecutionEnvironment): Unit = {
    val info = ListBuffer[String]()
    info.append("Hadoop,Spark")
    info.append("Hadoop,Spark")
    info.append("Flink,Flink")

    val data = env.fromCollection(info)
    data.flatMap(_.split(",")).distinct().print()
  }
  //flatmap
  def flatMapFunction(env : ExecutionEnvironment): Unit ={
    val info = ListBuffer[ String]()
    info.append("Hadoop,Spark")
    info.append("Hadoop,Spark")
    info.append("Flink,Flink")

    val data = env.fromCollection(info)

//    data.flatMap(_.split(",")).print()
//    data.flatMap(_.split(",")).map((_, 1)).groupBy(0).sum(1).print()
    data.flatMap(_.split(",")).map((_, 1)).groupBy(0).sum(1).print()
  }

  // first
  def firstFunction(env : ExecutionEnvironment): Unit ={
    val info = ListBuffer[(Int, String)]()
    info.append((1, "Hadoop"))
    info.append((1, "Spark"))
    info.append((1, "Flink"))
    info.append((2, "Java"))
    info.append((2, "Spring boot"))
    info.append((3, "Linux"))
    info.append((4, "VUE"))
    val data = env.fromCollection(info)
    //    data.first(3).print()
    //    data.groupBy(0).first(2).print()
    data.groupBy(0).sortGroup(1,Order.ASCENDING).first(2).print()
  }
  def mapPartitionFunction(env :ExecutionEnvironment): Unit ={
    val students = new ListBuffer[String]
    for(i <- 1 to 100){
      students.append("Student" + i)
    }

    val data = env.fromCollection(students).setParallelism(4)
    data.mapPartition( x => {
      val connection = DBUtils.getConnnection()
      println(connection + "...")
      DBUtils.returnConnection(connection)
      x
    }).print()
  }

  def mapfunction(env: ExecutionEnvironment): Unit ={
    val data = env.fromCollection(List(1,2,3,4,5,6,7,8,9,10))
    data.map(_  + 1).print()
  }
}
