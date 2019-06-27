package com.wxx.flink.tableAndSQL06

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.types.Row

object TableSQLAPI {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val tableEnv = TableEnvironment.getTableEnvironment(env)
    val filePath = "G:\\data\\flink\\sales.csv"
    import org.apache.flink.api.scala._
    val csv = env.readCsvFile[SalesLog](filePath, ignoreFirstLine  = true)
//    csv.print()
    val salesTable = tableEnv.fromDataSet(csv)
    tableEnv.registerTable("sales", salesTable)
    val resultTable = tableEnv.sqlQuery("select customerId, sum(amountPaid) money from sales group by customerId")

    tableEnv.toDataSet[Row](resultTable).print()

  }

  case class SalesLog(transactionId : String,
                      customerId:String,
                      itemId: String,
                      amountPaid: Double)
}
