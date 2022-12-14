package 大赛
import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.util.Collector
/**
 *
 * @PROJECT_NAME: Flink
 * @PACKAGE_NAME: 大赛
 * @author: 赵嘉盟-HONOR
 * @data: 2022-11-10 17:35
 * @DESCRIPTION
 *
 */
object 任務書二 {
  case class order_info(id:String,create_time:String,operation_time:String,status:String)
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","192.168.174.200:9092")
    val inputStream = env.addSource(new FlinkKafkaConsumer011[String]("order", new SimpleStringSchema(), properties))
    val th_order=new OutputTag[order_info]("th")
    val qx_order=new OutputTag[order_info]("qx")

    val dateStream=inputStream.map(data=>{
      val datas = data.split(",")
       order_info(datas(0), datas(10), datas(11), datas(4))
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[order_info](Time.seconds(5)) {
      override def extractTimestamp(t: order_info): Long = {
        val create_time=strToLong(t.create_time)
        var option_time=strToLong(t.operation_time)
        if (option_time==Nil ) option_time=create_time
        if (create_time>=option_time) create_time else option_time
      }
    })

    val orderSizeStream=dateStream.process(new ProcessFunction[order_info,order_info] {
      override def processElement(i: order_info, context: ProcessFunction[order_info, order_info]#Context, collector: Collector[order_info]): Unit = {
        i.status match {
          case "1005" => context.output(th_order,i)
          case "1003" => context.output(qx_order,i)
          case _ => collector.collect(i)
        }
      }
    })

/*
任务一
    使用Flink消费Kafka中的数据
    统计商城实时订单数量
    需要考虑订单状态，若有取消订单、申请退回、退回完成则不计入订单数量，其他状态则累加
    将key设置成totalcount存入Redis中
*/
//    val oneStream = orderSizeStream.filter(data => {
//        data.status != "1003" && data.status != "1005" && data.status != "1006"
//    }).map(data => ("totalcount", 1)).keyBy(0).sum(1)
//    val conf=new FlinkJedisPoolConfig.Builder()
//      .setHost("192.168.174.200")
//      .setPort(6379)
//      .build()
//      oneStream.addSink( new RedisSink[(String, Int)](conf,new RedisMapper[(String, Int)] {
//        override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET)
//
//        override def getKeyFromData(t: (String, Int)): String = t._1
//
//        override def getValueFromData(t: (String, Int)): String = t._2.toString
//      }))

/*
任务二
    在任务1进行的同时，使用侧边流
    统计每分钟申请退回订单的数量
    将key设置成refundcountminute存入Redis中
*/
//val towOutputStream = orderSizeStream.getSideOutput(th_order)
//  .filter(data => data.status == "1005")
//  .map(data => ("refundcountminute", 1))
//  .timeWindowAll(Time.minutes(1))
//  .sum(1)
//    val conf = new FlinkJedisPoolConfig.Builder()
//      .setHost("192.168.174.200")
//      .setPort(6379)
//      .build()
//    towOutputStream.addSink(new RedisSink[(String, Int)](conf,new RedisMapper[(String, Int)] {
//      override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET)
//
//      override def getKeyFromData(t: (String, Int)): String = t._1
//
//      override def getValueFromData(t: (String, Int)): String = t._2.toString
//    }))

    /*
        任务三
            在任务1进行的同时，使用侧边流
            计算每分钟内状态为取消订单占所有订单的占比
            将key设置成cancelrate存入Redis中
            value存放取消订单的占比（为百分比，保留百分比后的一位小数，四舍五入，例如12.1%）
        */
    var count=0
    var list=0.0
    val finalStream= dateStream.timeWindowAll(Time.seconds(1))
      .process(new ProcessAllWindowFunction[order_info,String,TimeWindow] {
        override def process(context: Context, elements: Iterable[order_info], out: Collector[String]): Unit = {
          elements.foreach(data=>{
            if(data.status=="1003"){
               count+=1
            }
          })
          elements.foreach(data=>{
             list+=1.0
          })//.toList.size
          val str=(count/list*100).formatted("%2.1f")
          out.collect(str)
        }
      })
    val conf =new FlinkJedisPoolConfig.Builder().setHost("192.168.174.200").setPort(6379).build()
    finalStream.addSink(new RedisSink[String](conf,new RedisMapper[String] {
      override def getCommandDescription: RedisCommandDescription =  new RedisCommandDescription(RedisCommand.SET)

      override def getKeyFromData(t: String): String = "cancelrate"

      override def getValueFromData(t: String): String = t+"%"
    }))



    env.execute("任务书二")
  }
  def strToLong(create_time: String) = {
    val format=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    format.parse(create_time).getTime
  }
}
