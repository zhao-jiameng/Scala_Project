package 大赛

import java.sql
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
 * @data: 2023-02-08 11:57
 * @DESCRIPTION
 *
 */
object 任务书2 {
  case class order_info(id:String,createTime:String,operationTime:String,status:String)
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val properties = new Properties()
    properties.setProperty("bootstrap.server","192.168.174.200:9092")
    //val inputStream=env.addSource(new FlinkKafkaConsumer011[String]("order",new SimpleStringSchema(),properties))
    val inputStream=env.readTextFile("src/main/resources/reader.txt")
    val th_order=new OutputTag[order_info]("th")
    val qx_order=new OutputTag[order_info]("qx")
    val dataStream=inputStream.map(data=>{
      val datas = data.split(",")
      order_info(datas(0),datas(10),datas(11),datas(4))
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[order_info](Time.seconds(5)) {
      override def extractTimestamp(t: order_info): Long = {
        val create_timee=strToLong(t.createTime)
        var operation_time=strToLong(t.operationTime)
        if(operation_time == Nil) return create_timee
        if(create_timee >= operation_time) create_timee else operation_time
      }
    })
    val orderSizeStream=dataStream.process(new ProcessFunction[order_info,order_info] {
      override def processElement(i: order_info, context: ProcessFunction[order_info, order_info]#Context, collector: Collector[order_info]): Unit = {
        i.status match {
          case "1005" => context.output(th_order,i)
          case "1003" => context.output(qx_order,i)
          case _ => collector.collect(i)
        }
      }
    })


    val oneSteram=orderSizeStream.filter(data=>{
      data.status != "1003" && data.status != "1005" && data.status != "1006"
    }).map(data=> ("totalcount",1)).keyBy(_._1).sum(1)
    val conf=new FlinkJedisPoolConfig.Builder()
      .setHost("192.168.23.51")
      .setPort(6379)
      .build()
    oneSteram.addSink(new RedisSink[(String, Int)](conf,new RedisMapper[(String, Int)] {
      override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET)
      override def getKeyFromData(t: (String, Int)): String = t._1
      override def getValueFromData(t: (String, Int)): String = t._2.toString
    }))


    val towStream=orderSizeStream.getSideOutput(th_order)
      .filter(data=>data.status=="1005")
      .map(data=>("refundcountminute",1))
      .timeWindowAll(Time.minutes(1))
      .sum(1)
    towStream.addSink(new RedisSink[(String, Int)](conf,new RedisMapper[(String, Int)] {
      override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET)
      override def getKeyFromData(t: (String, Int)): String = t._1
      override def getValueFromData(t: (String, Int)): String = t._2.toString
    }))

    var count=0
    var list=0.0
    val finalStream=dataStream.timeWindowAll(Time.minutes(1))
      .process(new ProcessAllWindowFunction[order_info,String,TimeWindow] {
        override def process(context: Context, elements: Iterable[order_info], out: Collector[String]): Unit = {
          elements.foreach(data=>{
            if(data.status=="1003") count+=1
            list+=1
          })
          val str=(count/list*100).formatted("%2.1f")
          out.collect(str)
        }
      })
    finalStream.addSink(new RedisSink[String](conf,new RedisMapper[String] {
      override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET)
      override def getKeyFromData(t: String): String = "cancelrate"

      override def getValueFromData(t: String): String = t+"%"
    }))
    env.execute("job")

  }
  def strToLong(str: String): Long ={
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    format.parse(str).getTime
  }
}
