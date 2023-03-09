package 比赛2

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
 * @PACKAGE_NAME: 比赛2
 * @author: 赵嘉盟-HONOR
 * @data: 2023-02-10 9:31
 * @DESCRIPTION
 *
 */
case class order_info2(id:String,create_time:String,option_time:String,status:String)
object 任务书2 {
  //3448,时友裕,13908519819,217.00,1005,5525,第19大街第27号楼9单元874门,描述286614,675628874311147,迪奥（Dior）烈艳蓝金唇膏 口红 3.5g 999号 哑光-经典正红等1件商品,2020-04-25 18:47:14,2020-04-26 18:55:02,2020-04-25 19:02:14,,,http://img.gmall.com/553516.jpg,32,55.00,252.00,20.00

  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","192.168.174.200:9092")
    val inputStream=env.addSource(new FlinkKafkaConsumer011[String]("order1",new SimpleStringSchema(),properties))
    //val inputStream=env.readTextFile("src/main/resources/reader.txt")
    val outputStream: DataStream[order_info2] = inputStream.map(data => {
      val datas = data.split(",")
      order_info2(datas(0), datas(10), datas(11), datas(4))
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[order_info2](Time.seconds(5)) {
      override def extractTimestamp(t: order_info2): Long = {
        val createTime = strToLong2(t.create_time)
        val optionTime = strToLong2(t.option_time)
        if (optionTime isNaN) return createTime
        if (createTime >= optionTime) createTime else optionTime
      }
    })
    val th_order=new OutputTag[order_info2]("th")
    val qx_order=new OutputTag[order_info2]("qx")
    val tagStream = outputStream.process(new ProcessFunction[order_info2, order_info2] {
      override def processElement(i: order_info2, context: ProcessFunction[order_info2, order_info2]#Context, collector: Collector[order_info2]): Unit = {
        i.status match {
          case "1003" => context.output(qx_order, i)
          case "1005" => context.output(th_order, i)
          case _ => collector.collect(i)
        }
      }
    })

    //TODO 任务一
/*    val oneStream=tagStream.filter(data=>{
      data.status != "1003" && data.status != "1005" && data.status != "1006"
    }).map(data=>("totalcount",1)).keyBy(_._1).sum(1)


    oneStream.addSink(new RedisSink[(String, Int)](conf,new RedisMapper[(String, Int)] {
      override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET)

      override def getKeyFromData(t: (String, Int)): String = t._1

      override def getValueFromData(t: (String, Int)): String = t._2.toString
    }))


    val twoStream=tagStream.getSideOutput(th_order).filter(data=>data.status == "1005")
      .map(data=>("refundcountminute",1)).timeWindowAll(Time.minutes(1)).sum(1)

    twoStream.addSink(new RedisSink[(String, Int)](conf,new RedisMapper[(String, Int)] {
      override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET)
      override def getKeyFromData(t: (String, Int)): String = t._1
      override def getValueFromData(t: (String, Int)): String = t._2.toString
    }))*/

    //TODO 任務三
    var count=0;
    var list=0.0
    val threeStream=outputStream.timeWindowAll(Time.minutes(1))
      .process(new ProcessAllWindowFunction[order_info2,String,TimeWindow] {
        override def process(context: Context, elements: Iterable[order_info2], out: Collector[String]): Unit = {
          elements.foreach(data=>{
            if(data.status == "1003") count+=1
            list+=1
          })
          val str=(count/list*100).formatted("%2.1f")
          out.collect(str)
        }
      }).print()
/*    val conf=new FlinkJedisPoolConfig.Builder().setHost("192.168.174.200").setPort(6379).build()
    threeStream.addSink(new RedisSink[String](conf,new RedisMapper[String] {
      override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET)

      override def getKeyFromData(t: String): String = "cancelrate"

      override def getValueFromData(t: String): String = t+"%"
    }))*/
    env.execute("job2")
  }
  def strToLong2(str: String):Long={
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    format.parse(str).getTime
  }
}
