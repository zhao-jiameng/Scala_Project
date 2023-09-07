package onetest

import java.math.BigDecimal
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
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.util.Collector
/**
 *
 * @PROJECT_NAME: 国赛
 * @PACKAGE_NAME: demo1
 * @author: 赵嘉盟-HONOR
 * @data: 2023-05-15 14:51
 * @DESCRIPTION
 *
 */
object demo1 {
  case class order_info(id: String, status: String, create_time: String, option_time: String, sun: Double)
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "192.168.23.51:9092")
    val qv_order = new OutputTag[order_info]("qv"){}
    val th_order = new OutputTag[order_info]("th"){}
    //val inputStream=env.readTextFile("src/main/resources/order_info.txt")
    val inputStream = env.addSource(new FlinkKafkaConsumer[String]("order", new SimpleStringSchema(), properties))
    val timeStream = inputStream.map(data => {
      val datas = data.split(",")
      order_info(datas(0), datas(4), datas(10), datas(11), datas(3).toDouble)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[order_info](Time.seconds(5)) {
      override def extractTimestamp(t: order_info): Long = {
        val createTime = strToLong(t.create_time)
        val optionTime = strToLong(t.option_time)
        if (order_info equals ("None")) return createTime
        if (createTime >= optionTime) createTime else optionTime
      }
    })
    val outStream = timeStream.process(new ProcessFunction[order_info, order_info] {
      override def processElement(i: order_info, context: ProcessFunction[order_info, order_info]#Context, collector: Collector[order_info]): Unit = {
        i.status match {
          case "1003" => context.output(qv_order, i)
          case "1006" => context.output(th_order, i)
          case _ => collector.collect(i)
        }
      }
    })
    //TODO 任务一
    /**
     * 使用Flink消费Kafka中的数据，统计商城实时订单实收金额（需要考虑订单状态，若有取消订单、申请退回、退回完成则不计入订单实收金额，其他状态的则累加）
     * 将key设置成totalprice存入Redis中。使用redis cli以get key方式获取totalprice值
     * 将结果截图粘贴至客户端桌面【Release\任务D提交结果.docx】中对应的任务序号下
     * 需两次截图，第一次截图和第二次截图间隔1分钟以上，第一次截图放前面，第二次截图放后面；
     */
    val conf=new FlinkJedisPoolConfig.Builder().setPort(6379).setHost("192.168.23.51").build()
    //filter:过滤,表示只保留1004的数据
    //timeStream.filter(_.status=="1004")
    timeStream.filter(data=>data.status != "1003" && data.status != "1005" && data.status != "1006")
      .map(data=>{
      ("totalprice",data.sun)
    }).keyBy(0).sum(1)
      .addSink(new RedisSink[(String, Double)](conf,new RedisMapper[(String, Double)] {
      override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET)
      override def getKeyFromData(t: (String, Double)): String = t._1
      override def getValueFromData(t: (String, Double)): String = t._2.toString
    }))

    //TODO 任务二
    /**
     * 2、在任务1进行的同时，使用侧边流，监控若发现order_status字段为退回完成,
     * 将key设置成totalrefundordercount存入Redis中，value存放用户退款消费额
     * 使用redis cli以get key方式获取totalrefundordercount值，将结果截图粘贴至客户端桌面【Release\任务D提交结果.docx】中对应的任务序号下
     * 需两次截图，第一次截图和第二次截图间隔1分钟以上，第一次截图放前面，第二次截图放后面；
     */
    outStream.getSideOutput(th_order).filter(_.status=="1006").map(data=>{
      ("totalrefundordercount",data.sun)
    }).keyBy(0).sum(1).addSink(new RedisSink[(String, Double)](conf,new RedisMapper[(String, Double)] {
      override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET)
      override def getKeyFromData(t: (String, Double)): String = t._1
      override def getValueFromData(t: (String, Double)): String = t._2.toString
    }))
    //TODO 任务三
    /**
     * 在任务1进行的同时，使用侧边流，监控若发现order_status字段为取消订单
     * 将数据存入MySQL数据库shtd_result的order_info表中
     * 然后在Linux的MySQL命令行中根据id降序排序，查询列id、consignee、consignee_tel、final_total_amount、feight_fee
     * 查询出前5条，将SQL语句复制粘贴至客户端桌面【Release\任务D提交结果.docx】中对应的任务序号下
     * 将执行结果截图粘贴至客户端桌面【Release\任务D提交结果.docx】中对应的任务序号下
     */
    inputStream.filter(_.split(",")(4)=="1003").addSink(new RichSinkFunction[String] {
      var conn:Connection=_
      var insertStem:PreparedStatement=_
      override def open(parameters: Configuration): Unit = {
        conn=DriverManager.getConnection("jdbc:mysql://192.168.23.51:3306/shtd_store?characterEncoding=UTF-8","root","root")
        insertStem=conn.prepareStatement("insert into order_info1 values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
      }
      override def invoke(value: String): Unit = {
        val data=value.split(",")
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
        insertStem.setTimestamp(11,new Timestamp(strToLong(data(10))))
        insertStem.setTimestamp(12,new Timestamp(strToLong(data(11))))
        insertStem.setTimestamp(13,new Timestamp(strToLong(data(12))))
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
    env.execute("demo1")
  }
  def strToLong(str: String) = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(str).getTime
}