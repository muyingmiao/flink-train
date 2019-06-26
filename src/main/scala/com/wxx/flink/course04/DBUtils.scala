package com.wxx.flink.course04

import scala.util.Random

object DBUtils {

  def getConnnection() ={
    new Random().nextInt(10) + ""
  }

  def returnConnection(conn : String): Unit ={

  }
}
