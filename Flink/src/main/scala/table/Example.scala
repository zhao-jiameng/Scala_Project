package table

import api.SensorReading
import api.sink.MyJdbcSinkFunc
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._

/**
 *
 * @PROJECT_NAME: Flink
 * @PACKAGE_NAME: table
 * @author: 赵嘉盟-HONOR
 * @data: 2022-10-17 13:38
 * @DESCRIPTION
 *
 */
object Example {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    val inputPath="H:\\Scala程序\\Flink\\src\\main\\resources\\source.txt"
    val stream=env.readTextFile(inputPath)
    //先转换样例类（简单转换操作）
    val dataStream=stream
      .map(data=>{
        val arr=data.split(",")
        SensorReading(arr(0),arr(1).toLong,arr(2).toDouble)
      })

    //首先创建表执行环境
    val tableEnv=StreamTableEnvironment.create(env)
    //基于流创建一张表
    val dataTable=tableEnv.fromDataStream(dataStream)

    //调用table api 进行转换
    val resultTable=dataTable
      .select("id,temperature")
      .filter("id='sensor_1'")

    //直接用SQL实现
    tableEnv.createTemporaryView("dataTable",dataTable)
    val sql="select id,temperature from dataTAble where id='sensor_1'"
    val resultSqlTable=tableEnv.sqlQuery(sql)

    resultTable.toAppendStream[(String,Double)].print("result")
    resultSqlTable.toAppendStream[(String,Double)].print("result sql")

    env.execute("table api example")
  }
}
