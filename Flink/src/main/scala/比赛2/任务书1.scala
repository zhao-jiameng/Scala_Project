package 比赛2

import java.text.SimpleDateFormat
import java.util.Properties
import java.sql.{Connection, DriverManager, PreparedStatement, Timestamp}
import java.math.BigDecimal

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
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
 * @data: 2023-02-09 15:10
 * @DESCRIPTION
 *
 */
//3596,纪顺信,13238531462,319.00,1004,9243,第1大街第28号楼4单元328门,描述363494,444814173213313,迪奥（Dior）烈艳蓝金唇膏口红3.5g999号哑光-经典正红等2件商品,2020-04-26 18:55:01,2020-04-26 18:55:02,2020-04-26 19:10:01,,,http://img.gmall.com/846851.jpg,6,194.00,504.00,9.00
object 任务书1 {
  case class Order_info(id:String,create_time:String,operate_time:String,status:String,mun:Double)
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val qx_output=new OutputTag[Order_info]("qx")
    val th_order=new OutputTag[Order_info]("th")

    val properties = new Properties()
    properties.setProperty("bootstrap.servers","192.168.174.200:9092")
    //val inputStream = env.addSource(new FlinkKafkaConsumer011[String]("order", new SimpleStringSchema(), properties))
    val inputStream=env.readTextFile("src/main/resources/reader.txt")
    val outputStream = inputStream.map(data => {
      val datas = data.split(",")
      Order_info(datas(0), datas(10), datas(11), datas(4), datas(3).toDouble)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Order_info](Time.seconds(5)) {
      override def extractTimestamp(t: Order_info): Long = {
        val createTime=strToLong(t.create_time)
        val operateTime=strToLong(t.operate_time)
        if(operateTime isNaN) return createTime
        if(createTime >= operateTime) createTime else operateTime
      }
    })
    val orderSizeStream=outputStream.process(new ProcessFunction[Order_info,Order_info] {
      override def processElement(i: Order_info, context: ProcessFunction[Order_info, Order_info]#Context, collector: Collector[Order_info]): Unit = {
        i.status match {
          case "1003" => context.output(qx_output,i)
          case "1006" => context.output(th_order,i)
          case _ => collector.collect(i)
        }
      }
    })

    // TODO 任務一
    val oneStream = orderSizeStream.filter(data => {
      data.status != "1003" && data.status != "1005" && data.status != "1006"
    }).map(data => {
      ("totalprice", data.mun)
    }).keyBy(_._1).sum(1)

    val conf = new FlinkJedisPoolConfig.Builder().setHost("192.168.174.200").setPort(6379).build()
    oneStream.addSink(new RedisSink[(String, Double)](conf,new RedisMapper[(String, Double)] {
      override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET)

      override def getKeyFromData(t: (String, Double)): String = t._1

      override def getValueFromData(t: (String, Double)): String = t._2.toString
    }))

    // TODO 任務二
    val twoStream=orderSizeStream.getSideOutput(th_order).filter(_.status=="1006").map(data=>{
      ("totalrefundordercount",data.mun)
    }).keyBy(_._1).sum(1)
    twoStream.addSink(new RedisSink[(String, Double)](conf,new RedisMapper[(String, Double)] {
      override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET)

      override def getKeyFromData(t: (String, Double)): String = t._1

      override def getValueFromData(t: (String, Double)): String = t._2.toString
    }))

    // TODO 任務三
    val finalStream = inputStream.filter(data => {
      val datas = data.split(",")
      datas(4) == "1003"
    })

    finalStream.addSink(new RichSinkFunction[String] {
      var conn:Connection=_
      var insertStem:PreparedStatement=_
      override def open(parameters: Configuration): Unit = {
       conn=DriverManager.getConnection("jdbc:mysql://192.168.174.200/shud_result?characterEncoding=UTF-8","root","root")
        insertStem=conn.prepareStatement("insert into order_info values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
      }
      override def invoke(value: String): Unit = {
        val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val datas=value.split(",")
        insertStem.setLong(1,datas(0).toLong)
        insertStem.setString(2,datas(1))
        insertStem.setString(3,datas(2))
        insertStem.setBigDecimal(4,new BigDecimal(datas(3)))
        insertStem.setString(5,datas(4))
        insertStem.setLong(6,datas(5).toLong)
        insertStem.setString(7,datas(6))
        insertStem.setString(8,datas(7))
        insertStem.setString(9,datas(8))
        insertStem.setString(10,datas(9))
        insertStem.setTimestamp(11,new Timestamp(format.parse(datas(10)).getTime))
        insertStem.setTimestamp(12,new Timestamp(format.parse(datas(11)).getTime))
        insertStem.setTimestamp(13,new Timestamp(format.parse(datas(12)).getTime))
        insertStem.setString(14,null)
        insertStem.setString(15,null)
        insertStem.setString(16,datas(15))
        insertStem.setInt(17,datas(16).toInt)
        insertStem.setBigDecimal(18,new BigDecimal(datas(17)))
        insertStem.setBigDecimal(19,new BigDecimal(datas(18)))
        insertStem.setBigDecimal(20,new BigDecimal(datas(19)))
        insertStem.executeUpdate()
      }

      override def close(): Unit = {
        insertStem.close()
        conn.close()
      }
    })
    env.execute("job1")
  }
  def strToLong(t: String):Long = {
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    format.parse(t).getTime
  }
}
