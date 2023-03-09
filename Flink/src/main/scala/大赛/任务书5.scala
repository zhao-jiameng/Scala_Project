package 大赛

import java.text.SimpleDateFormat
import java.util.Properties
import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
/**
 *
 * @PROJECT_NAME: Flink
 * @PACKAGE_NAME: 大赛
 * @author: 赵嘉盟-HONOR
 * @data: 2023-02-09 12:19
 * @DESCRIPTION
 *
 */
object 任务书5 {
  case class one_order(id:String,orderPrice:Double,createTime:String,option_time:String)
  case class two_order(id:String,orderDetailCount:Int,createTime:String)
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","192.168.174.200:9092")
    //val inputstream=env.addSource(new FlinkKafkaConsumer011[String]("order",new SimpleStringSchema(),properties))
    val inputStream=env.readTextFile("src/main/resources/reader.txt")
      .map(data=>{
        val datas = data.split(",")
        one_order(datas(0),datas(3).toDouble,datas(10),datas(11))
      }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[one_order](Time.seconds(5)) {
      override def extractTimestamp(t: one_order): Long = {
        val creat_time=dataToStr(t.createTime)*1000
        val option_time=dataToStr(t.option_time)*1000
        if(option_time isNaN) return creat_time
        if (creat_time > option_time) creat_time else option_time
      }
    })

    val inputStream2=env.readTextFile("src/main/resources/reader2.txt")
      .map(data=>{
        val datas = data.split(",")
        two_order(datas(1),datas(6).toInt,datas(7))
      }).keyBy(_.id).sum(1).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[two_order](Time.seconds(5)) {
      override def extractTimestamp(t: two_order): Long = dataToStr(t.createTime)*1000
    })


    // TODO 任務三
    val finalStream=inputStream.join(inputStream2)
      .where(_.id)
      .equalTo(_.id)
      .window(TumblingEventTimeWindows.of(Time.seconds(2)))
      .apply((e1,e2)=>{
        (e2.id,e1.orderPrice,e2.orderDetailCount)
      })
    finalStream.print()
    finalStream.addSink(new RichSinkFunction[(String, Double, Int)] {
      var conn:Connection=_
      var insertStem:PreparedStatement=_
      override def open(parameters: Configuration): Unit = {
        conn=DriverManager.getConnection("jdbc:mysql://192.168.174.200/shud_result?characterEncoding=UTF8","root","root")
        insertStem=conn.prepareStatement("insert into orderpostiveaggr values (?,?,?)")
      }
      override def invoke(value: (String, Double, Int)): Unit = {
        insertStem.setInt(1,value._1.toInt)
        insertStem.setDouble(2,value._2)
        insertStem.setInt(3,value._3)
        insertStem.executeUpdate()
      }

      override def close(): Unit = {
        insertStem.close()
        conn.close()
      }
    })
    print("----------------------")
    env.execute("job5")
  }
  def dataToStr(str: String): Long ={
    val fm=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    fm.parse(str).getTime
  }
}
