package 大赛
import java.util.Properties
import java.sql.{Connection, Date, DriverManager, PreparedStatement, Timestamp}
import java.math._
import java.text.SimpleDateFormat

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.tools.nsc.interpreter.replProps.format
/**
 *
 * @PROJECT_NAME: Flink
 * @PACKAGE_NAME: 大赛
 * @author: 赵嘉盟-HONOR
 * @data: 2022-11-13 20:56
 * @DESCRIPTION
 *
 */
object 任務書四 {
  def main(args: Array[String]): Unit = {
    case class reader2(id:String,sum:Double)
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val proprtties=new Properties()
    proprtties.setProperty("bootstrap.servers","192.168.174.200:9092")
    val inputStream = env.addSource(new FlinkKafkaConsumer011[String]("order", new SimpleStringSchema(), proprtties))
    val input2Stream= env.readTextFile("src/main/resources/reader2.txt")
    val qx_order= new OutputTag[String]("qv")
    val dd_order=new OutputTag[String]("dd")
    val outputStream = inputStream.process(new ProcessFunction[String, String] {
      override def processElement(i: String, context: ProcessFunction[String, String]#Context, collector: Collector[String]): Unit = {
        val strings = i.split(",")
        if (strings(4) == "1003") context.output(qx_order, i)
        else if (strings(4) != "1003" && strings(4) != "1005" && strings(4) != "1006") context.output(dd_order, i)
        collector.collect(i)
      }
    })
/*
  任務一
    统计商城实时订单数量（需要考虑订单状态，若有取消订单、申请退回、退回完成则不计入订单数量，其他状态则累加），将key设置成totalcount存入Redis中
*/
    val oneStream=outputStream.getSideOutput(dd_order).map(data=>("totalcount",1)).keyBy(0).sum(1)
    val conf=new FlinkJedisPoolConfig.Builder().setHost("192.168.174.200").setPort(6379).build()
    oneStream.addSink(new RedisSink[(String, Int)](conf,new RedisMapper[(String, Int)] {
      override def getCommandDescription: RedisCommandDescription = new  RedisCommandDescription(RedisCommand.SET)
      override def getKeyFromData(t: (String, Int)): String = t._1
      override def getValueFromData(t: (String, Int)): String = t._2.toString
    }))

/*
  任務二
    实时统计商城中销售量前3的商品（不考虑订单状态，不考虑打折）
    将key设置成top3itemamount存入Redis中
    （value使用String数据格式，value为前3的商品信息并且外层用[]包裹，其中按排序依次存放商品id:销售量，并用逗23号分割）
    示例如下：
        top3itemamount：[1:700,42:500,41:100]
*/
    val map=mutable.Map("a"->1.0)
    map.remove("a")
    val towStream=input2Stream.map(data=>{
      val datas = data.split(",")
      val sum=(datas(5).toDouble)*(datas(6).toInt)
      map+=(datas(0)->sum)
      val tuples = map.toList.sortBy(_._2).reverse.take(3)
      tuples
    })
    towStream.addSink(new RedisSink[List[(String, Double)]](conf,new RedisMapper[List[(String, Double)]] {
      override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET)

      override def getKeyFromData(t: List[(String, Double)]): String = "top3itemconsumption"

      override def getValueFromData(t: List[(String, Double)]): String = "["+t(0)._1+":"+t(0)._2+","+t(1)._1+":"+t(1)._2+","+t(2)._1+":"+t(2)._2+"]"
    }))

/*
    监控order_status字段为取消订单的数据,将数据存入MySQL数据库shtd_result的order_info表中
*/
    val finalStream=outputStream.getSideOutput(qx_order).addSink(new RichSinkFunction[String] {
      var conn:Connection=_
      var insertStem:PreparedStatement=_

      override def open(parameters: Configuration): Unit = {
        conn=DriverManager.getConnection("jdbc:mysql//192.168.174.200:3306/shud_result? characterEncoding=UTF-8","root","root")
        insertStem=conn.prepareStatement("insert into order_info values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
      }

      override def invoke(value: String): Unit = {
        val fm=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val i = value.split(",")
        insertStem.setLong(1,i(0).toLong)
        insertStem.setString(2,i(1))
        insertStem.setString(3,i(2))
        insertStem.setBigDecimal(4,new BigDecimal(i(3)))
        insertStem.setString(5,i(4))
        insertStem.setLong(6,i(5).toLong)
        insertStem.setString(7,i(6))
        insertStem.setString(8,i(7))
        insertStem.setString(9,i(8))
        insertStem.setString(10,i(9))
        insertStem.setTimestamp(11,new Timestamp(fm.parse(i(10)).getTime))
        insertStem.setTimestamp(12,new Timestamp(fm.parse(i(11)).getTime))
        insertStem.setTimestamp(13,new Timestamp(fm.parse(i(12)).getTime))
        insertStem.setString(14,i(13))
        insertStem.setLong(15,i(14).toLong)
        insertStem.setString(16,i(15))
        insertStem.setInt(17,i(16).toInt)
        insertStem.setBigDecimal(18,new BigDecimal(i(17)))
        insertStem.setBigDecimal(19,new BigDecimal(i(18)))
        insertStem.setBigDecimal(20,new BigDecimal(i(19)))
        insertStem.executeUpdate()
      }

      override def close(): Unit = {
        insertStem.close()
        conn.close()
      }
    })
    env.execute("任務書四")

  }
}
