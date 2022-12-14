package api.sink



import java.sql.{Connection, DriverManager, PreparedStatement}

import api.SensorReading
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala._

/**
 *
 * @PROJECT_NAME: Flink
 * @PACKAGE_NAME: api.sink
 * @author: 赵嘉盟-HONOR
 * @data: 2022-10-17 0:22
 * @DESCRIPTION
 *
 */
object MysqlSink {
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

    dataStream.addSink(new MyJdbcSinkFunc)

    env.execute("jdbc sink")
  }

}
class MyJdbcSinkFunc() extends RichSinkFunction[SensorReading] {
  //定义连接，预编译语句
  var conn: Connection=_
  var insertStmt:PreparedStatement=_
  var updateStem: PreparedStatement = _

  override def open(parameters: Configuration): Unit = {
    conn=DriverManager.getConnection("jdbc:mysql://localhost:3306/test","root","123456")
    insertStmt=conn.prepareStatement("insert into sensor_temp (id,temp) value (?,?)")
    updateStem=conn.prepareStatement("update sensor_temp set temp = ? where id = ?")
  }


  override def invoke(sensorReading:SensorReading): Unit = {    //先执行更新操作，查到就更新
    updateStem.setDouble(1,sensorReading.temperature)
    updateStem.setString(2,sensorReading.id)
    updateStem.execute()
    //如果更新没有查到数据，那么就插入
    if(updateStem.getUpdateCount==0){
      insertStmt.setString(1,sensorReading.id)
      insertStmt.setDouble(2,sensorReading.temperature)
      insertStmt.execute()
    }
  }

  override def close(): Unit = {
    insertStmt.close()
    updateStem.close()
    conn.close()

  }
}