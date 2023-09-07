package onetest

import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.util.Collector
/**
 *
 * @PROJECT_NAME: 国赛
 * @PACKAGE_NAME: onetest
 * @author: 赵嘉盟-HONOR
 * @data: 2023-05-22 15:41
 * @DESCRIPTION
 *
 */
object demo2 {
  case class order_info(id:String,createTime:String,operationTime:String,status:String)
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val qv_tag=new OutputTag[order_info]("id"){}
    val th_tag=new OutputTag[order_info]("id"){}

    val properties = new Properties()
    properties.setProperty("","")
    val inputStream=env.readTextFile("src/main/resources/order_info.txt")
    //val inputStream = env.addSource(new FlinkKafkaConsumer[String]("order", new SimpleStringSchema(), properties))

    val timeStream=inputStream.map(data=>{
      val datas = data.split(",")
      order_info(datas(0),datas(10),datas(11),datas(4))
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[order_info](Time.seconds(5)) {
      override def extractTimestamp(t: order_info): Long = {
        val createTime=strTOLong(t.createTime)
        val optionTime=strTOLong(t.operationTime)
        if(optionTime isNaN) return createTime
        if(createTime >= optionTime)  createTime else  optionTime
      }
    })

    val orderSizeStream=timeStream.process(new ProcessFunction[order_info,order_info] {
      override def processElement(i: order_info, context: ProcessFunction[order_info, order_info]#Context, collector: Collector[order_info]): Unit = {
        i.status match {
          case "1005" => context.output(th_tag,i)
          case "1003" => context.output(qv_tag,i)
          case _ => collector.collect(i)
        }
      }
    })

    //TODO 任务一：
    /**
     * 1、使用Flink消费Kafka中的数据，统计商城实时订单数量（需要考虑订单状态，若有取消订单、申请退回、退回完成则不计入订单数量，其他状态则累加）
     * 将key设置成totalcount存入Redis中。使用redis cli以get key方式获取totalcount值
     * 将结果截图粘贴至客户端桌面【Release\任务D提交结果.docx】中对应的任务序号下，需两次截图，第一次截图和第二次截图间隔1分钟以上，第一次截图放前面，第二次截图放后面；
     */

    val conf=new FlinkJedisPoolConfig.Builder().setPort(6379).setHost("192.168.174.201").build()
    timeStream.filter(data=> data.status != "1003" && data.status != "1006" && data.status != "1005").map(data=>{
      ("totalcount",1)
    }).keyBy(0).sum(1)//.print("totalcount")
      .addSink(new RedisSink[(String, Int)](conf,new RedisMapper[(String, Int)] {
      override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET)
      override def getKeyFromData(t: (String, Int)): String = t._1
      override def getValueFromData(t: (String, Int)): String = t._2.toString
    }))
    //TODO 任务二：
    /**
     * 在任务1进行的同时，使用侧边流，统计每分钟申请退回订单的数量，将key设置成refundcountminute存入Redis中
     * 使用redis cli以get key方式获取refundcountminute值，将结果截图粘贴至客户端桌面【Release\任务D提交结果.docx】中对应的任务序号下
     * 需两次截图，第一次截图和第二次截图间隔1分钟以上，第一次截图放前面，第二次截图放后面；
     */
    val twoStream=orderSizeStream.getSideOutput(th_tag).filter(data=> data.status == "1005").map(data=>{
      ("refundcountminute",1)
    }).timeWindowAll(Time.minutes(1)).sum(1)//.print("refundcountminute")
      .addSink(new RedisSink[(String, Int)](conf,new RedisMapper[(String, Int)] {
        override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET)
        override def getKeyFromData(t: (String, Int)): String = t._1
        override def getValueFromData(t: (String, Int)): String = t._2.toString
      }))
    //TODO 任务三：
    /**
     * 3、在任务1进行的同时，使用侧边流，计算每分钟内状态为取消订单占所有订单的占比，将key设置成cancelrate存入Redis中
     * value存放取消订单的占比（为百分比，保留百分比后的一位小数，四舍五入，例如12.1%）。使用redis cli以get key方式获取cancelrate值
     * 将结果截图粘贴至客户端桌面【Release\任务D提交结果.docx】中对应的任务序号下，需两次截图，第一次截图和第二次截图间隔1分钟以上，第一次截图放前面，第二次截图放后面。
     */
    val finalStream=timeStream.timeWindowAll(Time.minutes(1))
      .process(new ProcessAllWindowFunction[order_info,String,TimeWindow] {
        var count = 0
        var list = 0.0
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
    env.execute("demo2")
  }
  def strTOLong(time:String) = {
    val format = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss")
    format.parse(time).getTime
  }
}
