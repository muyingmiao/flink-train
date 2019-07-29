package com.wxx.flink.project


import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import scala.collection.mutable

class PKMySQLSource extends  RichParallelSourceFunction[mutable.HashMap[String,String]] {

  var connection : Connection = null
  var ps : PreparedStatement = null
  override def cancel() = {}

  override def run(sourceContext: SourceFunction.SourceContext[mutable.HashMap[String, String]]) = {
    val resulSet = ps.executeQuery()
    var collect = mutable.HashMap[String, String]()
    while (resulSet.next()){
      collect.put(resulSet.getNString("domain"), resulSet.getNString("user_id"))
    }
    sourceContext.collect(collect)
  }

  override def open(parameters: Configuration): Unit = {
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://192.168.31.100:3306/flink"
    val user = "root"
    val password = "root"
    Class.forName(driver)
    connection = DriverManager.getConnection(url, user, password)

    val sql = "select user_id ,domain from user_domain_config"

    ps = connection.prepareStatement(sql)
  }

  override def close(): Unit = {
    if(ps != null){
      ps.close()
    }
    if(connection != null){
      connection.close()
    }
  }
}
