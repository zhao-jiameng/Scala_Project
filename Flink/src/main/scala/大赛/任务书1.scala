package 大赛

import java.sql.{Connection, DriverManager, PreparedStatement, Timestamp}
import java.text.SimpleDateFormat
import java.util.Properties

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
import java.math.BigDecimal
/**
 *
 * @PROJECT_NAME: Flink
 * @PACKAGE_NAME: 大赛
 * @author: 赵嘉盟-HONOR
 * @data: 2023-02-07 11:07
 * @DESCRIPTION
 *
 */
object 任务书1 {
  case class Order_Info(id:String,cerateTime:String,openateTime:String,status:String,mun:Double)
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val properties = new Properties()
    properties.setProperty("bootstrap.server","192.168.174.200:9092")
    //val inputStream=env.addSource(new FlinkKafkaConsumer011[String]("order",new SimpleStringSchema(),properties))
    val inputStream=env.readTextFile("src/main/resources/reader.txt")
    val th_output=new OutputTag[Order_Info]("th")
    val qx_output=new OutputTag[Order_Info]("qx")
    val dataStream=inputStream.map(data=>{
      val datas = data.split(",")
      Order_Info(datas(0),datas(10),datas(11),datas(4),datas(3).toDouble)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Order_Info](Time.seconds(5)) {
      override def extractTimestamp(t: Order_Info): Long = {
        val create_time=stringtoLong(t.cerateTime)
        val option_time=stringtoLong(t.openateTime)
        if (option_time isNaN) return create_time
        if(create_time>option_time) create_time else option_time
      }
    })




    val orderSizeStream=dataStream.process(new ProcessFunction[Order_Info,Order_Info] {
      override def processElement(i: Order_Info, context: ProcessFunction[Order_Info, Order_Info]#Context, collector: Collector[Order_Info]): Unit = {
        i.status match {
          case "1006" => context.output(th_output,i)
          case "1003" =>context.output(qx_output,i)
          case _ => collector.collect(i)
        }
      }
    })
    val oneStream=orderSizeStream.filter(data=>{
      data.status != "1003" && data.status != "1005" && data.status != "1006"
    }).map(data=>{
      ("totalcount",data.mun)
    }).keyBy(_._1).sum(1)

    val conf=new FlinkJedisPoolConfig.Builder()
      .setHost("192.168.174.200")
      .setPort(6379)
      .build()
    oneStream.addSink(new RedisSink[(String, Double)](conf,new RedisMapper[(String, Double)] {
      override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET)

      override def getKeyFromData(t: (String, Double)): String = t._1

      override def getValueFromData(t: (String, Double)): String = t._2.toString
    }))



    val towStream=orderSizeStream.getSideOutput(th_output).filter(_.status=="1006").map(data=>{
      ("totalrefundordercount",data.mun)
    }).keyBy(_._1).sum(1)
    towStream.addSink(new RedisSink[(String, Double)](conf,new RedisMapper[(String, Double)] {
      override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET)

      override def getKeyFromData(t: (String, Double)): String = t._1

      override def getValueFromData(t: (String, Double)): String = t._2.toString
    }))
    val finalStream=inputStream.filter(data=>{
      val datas = data.split(",")
      datas(4)=="1006"
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
    env.execute("job")
  }
  def stringtoLong(data: String) ={
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    format.parse(data).getTime
  }
}
