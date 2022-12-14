package 大赛
import java.sql.{Connection, DriverManager, PreparedStatement}
import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.functions.sink.{PrintSinkFunction, RichSinkFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.sys.env
/**
 *
 * @PROJECT_NAME: Flink
 * @PACKAGE_NAME: 大赛
 * @author: 赵嘉盟-HONOR
 * @data: 2022-11-13 20:56
 * @DESCRIPTION
 *
 */
object 任務書五 {
  case class one_order(id:String,orderPrice:Double,create_time:String,option_time:String)
  case class tow_order(id:String,orderDetailCount:Int,create_time:String)
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val properties=new Properties()
    properties.setProperty("bootstrap.servers","master:9092")
    val input2Stream=env.readTextFile("src/main/resources/reader2.txt")
      .map(data=>{
        val i = data.split(",")
        tow_order(i(1),i(6).toInt,i(7))
      }).keyBy(_.id).sum(1).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[tow_order](Time.seconds(5)) {
      override def extractTimestamp(t: tow_order): Long = {
        dataToStr(t.create_time)*1000
      }
    })


    val input3Stream=env.readTextFile("src/main/resources/reader.txt")
      .map(data=>{
        val i = data.split(",")
        one_order(i(0),i(3).toDouble,i(10),i(11))
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[one_order](Time.seconds(5)) {
        override def extractTimestamp(t: one_order): Long = {
          val creat_time=dataToStr(t.create_time)
          val option_time=dataToStr(t.option_time)
          if (creat_time>option_time) creat_time*1000 else option_time*1000
        }
      })

    val inputStream = env.addSource(new FlinkKafkaConsumer011[String]("order", new SimpleStringSchema(), properties))
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[String](Time.seconds(5)) {
        override def extractTimestamp(t: String): Long = {
          val i = t.split(",")
          val creat_time=dataToStr(i(10))
          val option_time=dataToStr(i(11))
          if (option_time==Nil && option_time==0) i(11)=i(10)
          if (creat_time>option_time) creat_time*1000 else option_time*1000
        }
      })

/*
  任務一
    统计商城实时订单数量
    需要考虑订单表的状态，若有取消订单、申请退回、退回完成则不计入订单数量，其他状态则累加
    将key设置成totalcount存入Redis中
*/
//      val oneStream=inputStream.filter(data=>{
//        val i = data.split(",")
//        i(4)!="1003"&&i(4)!="1005"&&i(4)!="1006"
//      }).map(data=>("totalcount",1)).keyBy(0).sum(1)
//      val conf=new FlinkJedisPoolConfig.Builder().setHost("192.168.174.200").setPort(6379).build()
//      oneStream.addSink(new RedisSink[(String, Int)](conf,new RedisMapper[(String, Int)] {
//        override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET)
//        override def getKeyFromData(t: (String, Int)): String = t._1
//        override def getValueFromData(t: (String, Int)): String = t._2.toString
//      }))
/*
  任務二
    实时统计商城中消费额前3的商品（不考虑订单状态，不考虑打折）
    将key设置成top3itemconsumption存入Redis中
    value使用String数据格式，value为前3的商品信息并外层用[]包裹，其中按排序依次存放商品id:销售额，并用逗号分割
    示例如下：
      top3itemconsumption：[1:10020.2,42:4540.0,12:540]
*/
//      val map=mutable.Map("a"->1.0)
//      map.remove("a")
//      val towStream=input2Stream.map(data=>{
//        val i = data.split(",")
//        map+=(i(2)->i(5).toDouble*i(6).toInt)
//        val tuples = map.toList.sortBy(_._2).reverse.take(3)
//        tuples
//      })
//    towStream.addSink(new RedisSink[List[(String, Double)]](conf,new RedisMapper[List[(String, Double)]] {
//      override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET)
//
//      override def getKeyFromData(t: List[(String, Double)]): String = "top3itemconsumption"
//
//      override def getValueFromData(t: List[(String, Double)]): String = "["+t(0)._1+":"+t(0)._2+","+t(1)._1+":"+t(1)._2+","+t(2)._1+":"+t(2)._2+"]"
//    }))
/*
  任務三
    采用双流JOIN的方式（本系统稳定，无需担心数据迟到与丢失的问题,建议使用滚动窗口）
    结合订单信息和订单详细信息（需要考虑订单状态，若有取消订单、申请退回、退回完成则不进行统计）
    拼接成如下表所示格式，其中包含订单id、订单总金额、商品数
    将数据存入MySQL数据库shtd_result的orderpostiveaggr表中
*/
/*    input2Stream.intervalJoin(input3Stream)
      .between(Time.seconds(-10), Time.seconds(5))
      .process(new ProcessJoinFunction[String,String,(String,String,String)] {
        override def processElement(in1: String, in2: String, context: ProcessJoinFunction[String, String, (String, String, String)]#Context, collector: Collector[(String, String, String)]): Unit = {
          val i=in1.split(",")
          print(in1)
          val j=in2.split(",")
          print(in2)
          val map=(i(0),i(1),j(0))
          print(map)
          collector.collect(map)
        }
      }).print("任務三")*/
    val finalStream = input2Stream.join(input3Stream)
      .where(_.id)
      .equalTo(_.id)
      .window(TumblingEventTimeWindows.of(Time.seconds(2)))
      .apply((e1, e2) => {
       // e1(0) + "," + e1(1) + "," + e1(2) + ":" + e2(0) + "," + e2(1) + "," + e2(2)
        (e2.id,e2.orderPrice,e1.orderDetailCount)
      })
    finalStream.addSink(new RichSinkFunction[(String, Double, Int)] {
      var conn:Connection=_
      var insterStmt:PreparedStatement=_
      var CreateStmt:PreparedStatement=_

      override def open(parameters: Configuration): Unit = {
        conn=DriverManager.getConnection("jdbc:mysql://192.168.174.200:3306/shud_result?characterEncoding=UTF-8","root","root")
        insterStmt=conn.prepareStatement("insert into orderpostiveaggr values (?,?,?)")
      }

      override def invoke(value: (String, Double, Int)): Unit = {
        insterStmt.setInt(1,value._1.toInt)
        insterStmt.setDouble(2,value._2)
        insterStmt.setInt(3,value._3)
        insterStmt.executeUpdate()
      }

      override def close(): Unit = {
        insterStmt.close()
        conn.close()
      }
    })


    env.execute("任務書五")
  }
  def dataToStr(str: String) = {
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    format.parse(str).getTime
  }
}
