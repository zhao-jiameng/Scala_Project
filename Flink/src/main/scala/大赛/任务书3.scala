package 大赛

import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer


/**
 *
 * @PROJECT_NAME: Flink
 * @PACKAGE_NAME: 大赛
 * @author: 赵嘉盟-HONOR
 * @data: 2023-02-09 12:18
 * @DESCRIPTION
 *
 */
object 任务书3 {
  case class order_info(id:String,creatTime:String,operateTime:String,status:String,mun:Double,name:String)
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputStream=env.readTextFile("src/main/resources/reader.txt")
    val qx_order=new OutputTag[order_info]("qx")
    val outputStream=inputStream.map(data=>{
      val datas = data.split(",")
      order_info(datas(5),datas(10),datas(11),datas(4),datas(3).toDouble,datas(1))
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[order_info](Time.seconds(5)) {
      override def extractTimestamp(t: order_info): Long = {
        val create_time=strToLong(t.creatTime)
        val option_time=strToLong(t.operateTime)
        if(option_time isNaN) return create_time
        if(create_time >= option_time) create_time else option_time
      }
    })
    val tagStream=outputStream.process(new ProcessFunction[order_info,order_info] {
      override def processElement(i: order_info, context: ProcessFunction[order_info, order_info]#Context, collector: Collector[order_info]): Unit = {
        i.status match {
          case "1003" => context.output(qx_order,i)
          case _ => collector.collect(i)
        }
      }
    })
    var id1=""
    var id2 = ""
    var money1 = ""
    var money2 = ""
    var name1 = ""
    var name2 = ""
    var tag = 0
    val oneStream=tagStream.filter(data=>{
      data.status != "1003" && data.status != "1005" && data.status != "1006"
    }).map(data=>(data.id,data.mun,data.name))
    val tuples: ListBuffer[(String, Double, String)] = new ListBuffer[(String, Double, String)]
    val oneStream2=oneStream.process(new ProcessFunction[(String,Double,String),String] {
      override def processElement(i: (String, Double, String), context: ProcessFunction[(String, Double, String), String]#Context, collector: Collector[String]): Unit = {
        tuples.append((i._1, i._2, i._3))
        val top2= tuples.sortBy(_._2).reverse.take(2)
        top2.foreach(a=>{
          if(tag==0){
            id1=a._1
            name1 = a._3
            money1 = a._2.toString
            tag += 1
          } else {
            id2 = a._1
            name2 = a._3
            money2 = a._2.toString
            tag -= 1
          }
        })
        val info=s"[${id2}:${name2}:${money2},${id1}:${name1}:${money1}]"
        collector.collect(info)
      }
    })
    oneStream2.print()
    val conf = new FlinkJedisPoolConfig.Builder().setHost("192.168.174.200").setPort(6379).build()
    oneStream2.addSink(new RedisSink[String](conf,new RedisMapper[String] {
      override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET)
      override def getKeyFromData(t: String): String = "top2consumption"

      override def getValueFromData(t: String): String = t
    }))

    env.execute("job3")
  }
  def strToLong(str: String):Long={
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    format.parse(str).getTime
  }
}
