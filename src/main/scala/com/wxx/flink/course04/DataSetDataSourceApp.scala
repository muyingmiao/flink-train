package com.wxx.flink.course04

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

import scala.collection.mutable.ListBuffer
import org.apache.flink.api.scala._

object DataSetDataSourceApp {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

//    fromCollection(env)
//    textFile(env)
//    csvFile(env)
//    readRecuriseFiles(env)
//      readCompressionFiles(env)
  }


  //压缩文件读取
  def readCompressionFiles(env : ExecutionEnvironment): Unit ={
    val filePath = "G:\\data\\flink\\compresson.zip"
    env.readTextFile(filePath).print()
  }

  def readRecuriseFiles(env : ExecutionEnvironment): Unit ={
    val filePath = "G:\\data\\flink\\nested"
    val parameters = new Configuration
    parameters.setBoolean("recursive.file.enumeration", true)
    env.readTextFile(filePath).withParameters(parameters).print()
  }

  def csvFile(env : ExecutionEnvironment): Unit ={
    /*
    *
    * */
    import org.apache.flink.api.scala._
    val filePath =  "G:\\data\\flink\\people.csv";
//    env.readCsvFile[(String, Int, String)](filePath, ignoreFirstLine =true).print()
    /*(Jorge,30)
    (Bob,32)*/
//    env.readCsvFile[(String, Int)](filePath,ignoreFirstLine =true, includedFields = Array(0,1)).print()

//    env.readCsvFile[MyCaseClass](filePath, ignoreFirstLine = true,includedFields = Array(0,1)).print()
//    case class MyCaseClass(name:String, ane:Int)

    env.readCsvFile[Person](filePath, ignoreFirstLine = true, pojoFields = Array("name","age","work")).print()

  }

  def textFile(env : ExecutionEnvironment): Unit ={
    val filepath = "G:\\data\\flink\\input.txt";

    env.readTextFile(filepath).print()
  }

  def fromCollection(env : ExecutionEnvironment) :Unit = {
    import org.apache.flink.api.scala._
    val data = 1 to 10
    env.fromCollection(data).print()
  }
}
