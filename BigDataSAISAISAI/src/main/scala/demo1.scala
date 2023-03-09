import java.text.SimpleDateFormat
import java.util.Properties
import java.sql.{Connection, DriverManager, PreparedStatement, Timestamp}
import java.math.BigDecimal

import jdk.nashorn.internal.objects.annotations.Property
import org.apache.flink.api.common.functions.RichFunction
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
import org.apache.flink.streaming.connectors.redis.common.config.{FlinkJedisClusterConfig, FlinkJedisPoolConfig}
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerConfig

/**
 *
 * @PROJECT_NAME: BigDataSAISAISAI
 * @PACKAGE_NAME:
 * @author: 赵嘉盟-HONOR
 * @data: 2023-02-14 10:12
 * @DESCRIPTION
 *
 */

object demo1 {
  case class order_info(id:String,status:String,create_time:String,opention_time:String,sum:Double)
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val qv_order=new OutputTag[order_info]("qx")
    val th_order=new OutputTag[order_info]("th")
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","192.168.23.51:9092")

    val inputStream = env.addSource(new FlinkKafkaConsumer011[String]("order", new SimpleStringSchema(), properties))
    val timeStream: DataStream[order_info] = inputStream.map(data => {
      val datas = data.split(",")
      order_info(datas(0), datas(4), datas(10), datas(11), datas(3).toDouble)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[order_info](Time.seconds(5)) {
      override def extractTimestamp(t: order_info): Long = {
        val createTime = strToLong(t.create_time)
        val optionTime = strToLong(t.opention_time)
        if (optionTime isNaN) return createTime
        if (createTime >= optionTime) createTime else optionTime
      }
    })
    val outStream=timeStream.process(new ProcessFunction[order_info,order_info] {
      override def processElement(i: order_info, context: ProcessFunction[order_info, order_info]#Context, collector: Collector[order_info]): Unit = {
        i.status match {
          case "1003" => context.output(qv_order,i)
          case "1006" => context.output(th_order,i)
          case _ =>collector.collect(i)
        }
      }
    })
   //TODO 任務一
    val conf=new FlinkJedisPoolConfig.Builder().setPort(6379).setHost("192.168.23.51").build()
    val oneStream=timeStream.filter(data=> data.status != "1003" && data.status != "1005" && data.status != "1006" ).map(data=>{
      ("totalprice",data.sum)
    }).keyBy(_._1).sum(1).addSink(new RedisSink[(String, Double)](conf,new RedisMapper[(String, Double)] {
      override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET)
      override def getKeyFromData(t: (String, Double)): String = t._1
      override def getValueFromData(t: (String, Double)): String = t._2.toString
    }))

    //TODO 任務二
    val towStream=outStream.getSideOutput(th_order).filter(_.status=="1006").map(data=>{
      ("totalrefundordercount",data.sum)
    }).keyBy(_._1).sum(1).addSink(new RedisSink[(String, Double)](conf,new RedisMapper[(String, Double)] {
      override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET)
      override def getKeyFromData(t: (String, Double)): String = t._1
      override def getValueFromData(t: (String, Double)): String = t._2.toString
    }))
    //TODO 任務三
    val mysqlStream=inputStream.filter(data=>data.split(",")(4)=="1003").addSink(new RichSinkFunction[String] {
      var conn:Connection=_
      var insertStem:PreparedStatement=_
      override def open(parameters: Configuration): Unit = {
        conn=DriverManager.getConnection("jdbc:mysql://192.168.23.51/shtd_result?characterEncoding=UTF-8","root","123456")
        insertStem=conn.prepareStatement("insert into order_info values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
      }
      override def invoke(value: String): Unit = {
        val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val data = value.split(",")
        insertStem.setLong(1,data(0).toLong)
        insertStem.setString(2,data(1))
        insertStem.setString(3,data(2))
        insertStem.setBigDecimal(4,new BigDecimal(data(3)))
        insertStem.setString(5,data(4))
        insertStem.setLong(6,data(5).toLong)
        insertStem.setString(7,data(6))
        insertStem.setString(8,data(7))
        insertStem.setString(9,data(8))
        insertStem.setString(10,data(9))
        insertStem.setTimestamp(11,new Timestamp(format.parse(data(10)).getTime))
        insertStem.setTimestamp(12,new Timestamp(format.parse(data(11)).getTime))
        insertStem.setTimestamp(13,new Timestamp(format.parse(data(12)).getTime))
        insertStem.setString(14,null)
        insertStem.setString(15,null)
        insertStem.setString(16,data(15))
        insertStem.setInt(17,data(16).toInt)
        insertStem.setBigDecimal(18,new BigDecimal(data(17)))
        insertStem.setBigDecimal(19,new BigDecimal(data(18)))
        insertStem.setBigDecimal(20,new BigDecimal(data(19)))
        insertStem.executeUpdate()

      }
      override def close(): Unit = {
        insertStem.close()
        conn.close()

      }
    })
    env.execute("job1")
  }
  def strToLong(str: String):Long = {
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    format.parse(str).getTime
  }
}
