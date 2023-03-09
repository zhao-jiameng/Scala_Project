package 比赛2.大賽三
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
 * @PACKAGE_NAME: 比赛2.大賽三
 * @author: 赵嘉盟-HONOR
 * @data: 2023-02-21 21:37
 * @DESCRIPTION
 *
 */
object demo1 {
  case class order_info(id:String,status:String,c:String,o:String,num:Double)
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val properties = new Properties()
    properties.setProperty("","")
    val inputStream1=env.addSource(new FlinkKafkaConsumer011[String]("order",new SimpleStringSchema(),properties))
    val inputStream=env.readTextFile("src/main/resources/reader.txt").map(data=>{
      val s = data.split(",")
      order_info(s(0),s(4),s(10),s(11),s(3).toDouble)
    }).assignTimestampsAndWatermarks(new  BoundedOutOfOrdernessTimestampExtractor[order_info](Time.seconds(5)) {
      override def extractTimestamp(t: order_info): Long = {
        val o=StrToLong(t.o)
        val c=StrToLong(t.c)
        if(o isNaN) return c
        if(o >c) o else c
      }
    })
    val qx_order=OutputTag("qx")
    val outputTagStream=inputStream.process(new ProcessFunction[order_info,order_info] {
      override def processElement(i: order_info, context: ProcessFunction[order_info, order_info]#Context, collector: Collector[order_info]): Unit = {
        i.status match {
          case "1003" => context.output(qx_order,i)
          case _ => collector.collect(i)
        }
      }
    })
    val oneStream=inputStream1.timeWindowAll(Time.seconds(5)).process(new ProcessAllWindowFunction[order_info,order_info,TimeWindow] {
      override def process(context: Context, elements: Iterable[order_info], out: Collector[order_info]): Unit = {
        out.collect(elements.foreach(data=>data))
      }
    })
    val conf=new FlinkJedisPoolConfig.Builder().setPort().setHost().build()
    inputStream.addSink(new RedisSink[order_info](conf,new RedisMapper[order_info] {
      override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.HSET,"set")

      override def getKeyFromData(t: order_info): String = t.status

      override def getValueFromData(t: order_info): String = t.o
    }))

  }
  def StrToLong(c: String):Long = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(c).getTime
}
