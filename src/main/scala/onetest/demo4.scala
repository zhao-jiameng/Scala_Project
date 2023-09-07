package onetest

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.text.SimpleDateFormat
import java.util.{Date, Optional, Properties}

import onetest.demo4.longTOTime
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
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
 * @PROJECT_NAME: BIgData
 * @PACKAGE_NAME:
 * @author: 赵嘉盟-HONOR
 * @data: 2023-07-27 19:04
 * @DESCRIPTION
 *
 */
object demo4 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "192.168.3.55:9092")
    val inputStream=env.addSource(new FlinkKafkaConsumer[String]("ChangeRecord",new SimpleStringSchema(),properties))
    //val inputStream = env.readTextFile("src/main/resources/producerecord.txt")
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[String](Time.seconds(5)) {
        override def extractTimestamp(t: String): Long = dateToLong(new Date())
    })
    //TODO 任务一：
    /**
     * 1、使用Flink消费Kafka中ProduceRecord主题的数据，统计在已经检验的产品中，各设备每5分钟生产产品总数，将结果存入Redis中
     * key值为“totalproduce”，value值为“设备id，最近五分钟生产总数”。使用redis cli以HGETALL key方式获取totalproduce值
     * 将结果截图粘贴至客户端桌面【Release\任务D提交结果.docx】中对应的任务序号下，需两次截图，第一次截图和第二次截图间隔5分钟以上，第一次截图放前面，第二次截图放后面；
     * 注：ProduceRecord主题，生产一个产品产生一条数据；
     * change_handle_state字段为1代表已经检验，0代表未检验；
     */
    val conf=new FlinkJedisPoolConfig.Builder().setHost("192.168.174.201").setPort(6379).build()
/*    inputStream
      //.filter(_.split(",")(9)==1)
      .map(data=>{
      val datas = data.split(",")
      (datas(1),1)
    }).keyBy(0).timeWindowAll(Time.minutes(5)).sum(1).addSink(new RedisSink[(String, Int)](conf,new RedisMapper[(String, Int)] {
      override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.HSET,"totalproduce")
      override def getKeyFromData(t: (String, Int)): String = t._1
      override def getValueFromData(t: (String, Int)): String = t._2.toString
    }))*/
    //TODO 任务二：
    /**
     * 2、使用Flink消费Kafka中ChangeRecord主题的数据，当某设备30秒状态连续为“预警”，输出预警信息。
     * 当前预警信息输出后，最近30秒不再重复预警（即如果连续1分钟状态都为“预警”只输出两次预警信息）
     * 将结果存入Redis中，key值为“warning30sMachine”，value值为“设备id，预警信息”。使用redis cli以HGETALL key方式获取warning30sMachine值
     * 将结果截图粘贴至客户端桌面【Release\任务D提交结果.docx】中对应的任务序号下，需两次截图，第一次截图和第二次截图间隔1分钟以上，第一次截图放前面，第二次截图放后面；
     * 注：时间使用change_start_time字段，忽略数据中的change_end_time不参与任何计算。忽略数据迟到问题。
     * Redis的value示例：115,2022-01-01 09:53:10:设备115 连续30秒为预警状态请尽快处理！
     * (2022-01-01 09:53:10 为change_start_time字段值，中文内容及格式必须为示例所示内容。)
     */
    env.readTextFile("src/main/resources/chgerText.txt")
    //env.socketTextStream("localhost",10086)
      .map(data=>{
      val datas = data.split(",")
      (datas(1), datas(3), datas(4))
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, String, String)](Time.minutes(5)) {
      override def extractTimestamp(t: (String, String, String)): Long = strTOLong(t._3)
    }).keyBy(_._1).process(new KeyedProcessFunction[String,(String,String,String),(String,String)] {
      lazy val Time=getRuntimeContext.getState(new ValueStateDescriptor[Long]("Time",classOf[Long]))
      lazy val Time1=getRuntimeContext.getState(new ValueStateDescriptor[String]("Time1",classOf[String]))
      override def processElement(i: (String, String, String), context: KeyedProcessFunction[String, (String, String, String), (String,String)]#Context, collector: Collector[(String,String)]): Unit = {
        val time=Time.value()
        if(i._2=="待机" && time==0L){
          val ts=context.timerService().currentProcessingTime()+1L
          context.timerService().registerProcessingTimeTimer(ts)
          Time.update(ts)
          Time1.update(i._3)
        }else if(i._2!="待机"){
          context.timerService().deleteProcessingTimeTimer(time)
          Time.clear()
        }
      }
      override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, (String, String, String), (String,String)]#OnTimerContext, out: Collector[(String,String)]): Unit = {
        out.collect(ctx.getCurrentKey,Time1.value())
        Time.clear()
      }
    }).keyBy(_._1).process(new KeyedProcessFunction[String,(String,String),(String,String)] {
          lazy val Sj=getRuntimeContext.getState(new ValueStateDescriptor[String]("sj",classOf[String]))
          override def processElement(i: (String, String), context: KeyedProcessFunction[String, (String, String), (String, String)]#Context, collector: Collector[(String, String)]): Unit =
            if (Sj.value() == null) {
              val ts = context.timerService().currentProcessingTime() + 1L
              context.timerService().registerProcessingTimeTimer(ts)
              Sj.update(i._1+","+i._2)
              collector.collect(i)
            }
          override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, (String, String), (String, String)]#OnTimerContext, out: Collector[(String, String)]): Unit = Sj.clear()
        }).print("demo4")
/*      .addSink(new RedisSink[(String, String)](conf,new RedisMapper[(String, String)] {
      override def getCommandDescription: RedisCommandDescription =new RedisCommandDescription(RedisCommand.HSET,"warning30sMachine")
      override def getKeyFromData(t: (String, String)): String = t._1
      override def getValueFromData(t: (String, String)): String = t._2+"设备："+t._1+" 连续30秒为预警状态请尽快处理！"
    }))*/

    //TODO 任务三：
    /**
     * 3、使用Flink消费Kafka中ChangeRecord主题的数据，统计每3分钟各设备状态为“预警”且未处理的数据总数
     * 将结果存入MySQL数据库shtd_industry的threemin_warning_state_agg表中（追加写入，表结构如下）
     * 请将任务启动命令复制粘贴至客户端桌面【Release\任务D提交结果.docx】中对应的任务序号下
     * 启动且数据进入后按照设备id升序排序查询threemin_warning_state_agg表进行截图
     * 第一次截图后等待3分钟再次查询并截图,将结果截图粘贴至客户端桌面【Release\任务D提交结果.docx】中对应的任务序号下。
     **/
    env.readTextFile("src/main/resources/changerecord.txt")
      .map(data => {
      val datas = data.split(",")
      (datas(1), datas(2),datas(3), longTOTime(strTOLong(datas(4))),1)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, String, String, String, Int)](Time.seconds(5)) {
        override def extractTimestamp(t: (String, String, String, String, Int)): Long = strTOLong(t._4)
      }).filter(data=>data._3=="待机" && data._2=="0").keyBy(_._1).timeWindowAll(Time.minutes(3)).sum(4)
      .addSink(new RichSinkFunction[(String, String, String, String, Int)] {
        var conn:Connection=_
        var insterStem:PreparedStatement=_

        override def open(parameters: Configuration): Unit ={
          conn=DriverManager.getConnection("jdbc:mysql://192.168.174.201/gy_db?characterEncoding=UTF-8","root","root")
          insterStem=conn.prepareStatement("insert into threemin_warning_state_agg values (?,?,?)")
        }
        override def invoke(value: (String, String, String, String, Int)): Unit = {
          insterStem.setInt(1,value._1.toInt)
          insterStem.setInt(2,value._5)
          insterStem.setString(3,value._4)
          insterStem.executeUpdate()
        }
        override def close(): Unit ={
          insterStem.close()
          conn.close()
        }
      })
    env.execute("dome4")
  }
  def strTOLong(str: String): Long =  new SimpleDateFormat("dd/MM/yyyy HH:mm:ss").parse(str).getTime
  def longTOTime(long: Long):String=new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(long)
  def dateToLong(date: Date): Long = date.getTime
}
