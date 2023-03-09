package 比赛2

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import 比赛2.任务书3.{order_info3, strToLong3}
import java.math.BigDecimal
/**
 *
 * @PROJECT_NAME: Flink
 * @PACKAGE_NAME: 比赛2
 * @author: 赵嘉盟-HONOR
 * @data: 2023-02-10 11:39
 * @DESCRIPTION
 *
 */
object 任务书5 {
  case class one_order(id:String,create_time:String,option_time:String,status:String,price:Double)
  case class two_order(id:String,num:Int,create_time:String)
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputStream=env.readTextFile("src/main/resources/reader.txt")
      .map(data=>{
        val datas = data.split(",")
        one_order(datas(0),datas(10),datas(11),datas(4),datas(3).toDouble)
      }).filter(data=>{
      data.status != "1003" && data.status != "1005" && data.status != "1006"
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[one_order](Time.seconds(5)) {
      override def extractTimestamp(t: one_order): Long = {
        val createTime=strToLong3(t.create_time)
        val optionTime=strToLong3(t.option_time)
        if(optionTime isNaN) return createTime
        if(createTime >= optionTime) createTime else optionTime
      }
    })

    val inputStream2=env.readTextFile("src/main/resources/reader2.txt")
      .map(data=>{
        val datas = data.split(",")
        two_order(datas(1),datas(6).toInt,datas(7))
      }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[two_order](Time.seconds(5)) {
      override def extractTimestamp(t: two_order): Long = strToLong3(t.create_time)
    })

    val threeStream=inputStream.join(inputStream2)
      .where(_.id)
      .equalTo(_.id)
      .window(TumblingEventTimeWindows.of(Time.seconds(2)))
      .apply((e1,e2)=>{
        (e1.id,e1.price,e2.num)
      }).print()

/*
    threeStream.addSink(new RichSinkFunction[(String, Double, Int)] {
      var conn:Connection=_
      var insertStem:PreparedStatement=_
      override def open(parameters: Configuration): Unit = {
        conn=DriverManager.getConnection("jdbc:mysql://192.168.174.200/shud_result?characterEncoding=UTF-8","root","root")
        insertStem=conn.prepareStatement("insert into orderpostiveaggr values(?,?,?)")

      }

      override def invoke(value: (String, Double, Int), context: SinkFunction.Context[_]): Unit = {
        insertStem.setInt(1,value._1.toInt)
        insertStem.setBigDecimal(2,new BigDecimal(value._2))
        insertStem.setInt(3,value._3)
        insertStem.executeUpdate()

      }

      override def close(): Unit = {
        insertStem.close()
        conn.close()
      }

    })
*/


env.execute("job5")
  }
}
