package onetest

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.{AllWindowFunction, ProcessAllWindowFunction}
import org.apache.flink.streaming.api.windowing.assigners.{SlidingProcessingTimeWindows, TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.util.Collector

import scala.collection.mutable

object demo8 {
  case class EnvironmentData(id:String,Temperature:Double,time:Long)
  def main(args: Array[String]): Unit = {
    //TODO 准备环境：
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    var conf=new FlinkJedisPoolConfig.Builder().setPort(6379).setHost("192.168.174.201").build()
    //TODO 任务一：
    /**
    1、使用Flink消费Kafka中EnvironmentData主题的数据,监控各环境检测设备数据，当温度（Temperature字段）持续3分钟高于38度时记录为预警数据。
    将结果存入Redis中，key值为“env_temperature_monitor”，value值为“设备id-预警信息生成时间，预警信息”（预警信息生成时间格式：yyyy-MM-dd HH:mm:ss）。
    使用redis cli以HGETALL key方式获取env_temperature_monitor值，将结果截图粘贴至客户端桌面【Release\任务D提交结果.docx】中对应的任务序号下，需要Flink启动运行6分钟以后再截图；
    注：时间语义使用Processing Time。
      value示例：114-2022-01-01 14:12:19，设备114连续三分钟温度高于38度请及时处理！
      中文内容及格式必须为示例所示内容。
      同一设备3分钟只预警一次。
     */
    env.readTextFile("src/main/resources/environmentdata.txt")
    //env.socketTextStream("localhost",7777)
      .map(date => {
        val datas = date.split(",")
        EnvironmentData(datas(1), datas(5).toDouble, strToLong(datas(10)))
      }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[EnvironmentData](Time.seconds(5)) {
      override def extractTimestamp(t: EnvironmentData): Long = t.time
    }).keyBy(_.id).process( new MykeyedProcessFuntion(180000L)).keyBy(_.split(",")(0)).process(new TimePeocessFuntion(180000L))
      .addSink(new RedisSink[String](conf,new RedisMapper[String] {
        override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.HSET,"env_temperature_monitor")
        override def getKeyFromData(t: String): String =t+"-"+dateTodate(new Date())
        override def getValueFromData(t: String): String = "设备"+t+"连续三分钟温度高于38度请及时处理！"
      }))
    //TODO 任务二：
    /**
    2、使用Flink消费Kafka中ChangeRecord主题的数据，每隔1分钟输出最近3分钟的预警次数最多的设备。将结果存入Redis中
    key值为“warning_last3min_everymin_out”，value值为“窗口结束时间，设备id”（窗口结束时间格式：yyyy-MM-dd HH:mm:ss）
    使用redis cli以HGETALL key方式获取warning_last3min_everymin_out值
    将结果截图粘贴至客户端桌面【Release\任务D提交结果.docx】中对应的任务序号下，需两次截图，第一次截图和第二次截图间隔1分钟以上，第一次截图放前面，第二次截图放后面；
    注：时间语义使用Processing Time。
     */
    val map=mutable.Map("0"->0)
    env.readTextFile("src/main/resources/changerecord.txt")
    //env.socketTextStream("localhost",7777)
      .map(date=>{
      val datas = date.split(",")
      (datas(1),datas(3))
    }).filter(_._2=="待机").map(data=> (data._1,1)).keyBy(_._1).sum(1)
      .windowAll(SlidingProcessingTimeWindows.of(Time.seconds(5),Time.seconds(1)))
      //.windowAll(TumblingProcessingTimeWindows.of(Time.milliseconds(1)))
      .apply(new AllWindowFunction[(String,Int),(String,String),TimeWindow] {
        override def apply(window: TimeWindow, input: Iterable[(String, Int)], out: Collector[(String, String)]): Unit = {
          input.foreach(date=>{
            map+=(date._1->date._2)
          })
          out.collect((map.toList.sortBy(_._2).reverse.take(1)(0)._1,dateTodate(new Date(window.getEnd))))
        }
      }).addSink(new RedisSink[(String, String)](conf,new RedisMapper[(String, String)] {
      override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.HSET,"warning_last3min_everymin_out")
      override def getKeyFromData(t: (String, String)): String = t._2
      override def getValueFromData(t: (String, String)): String =t._1
    }))
    //TODO 任务三：
    /**
    3、使用Flink消费Kafka中ChangeRecord主题的数据，实时统计每个设备从其他状态转变为“运行”状态的总次数
    将结果存入MySQL数据库shtd_industry的change_state_other_to_run_agg表中（表结构如下）
    请将任务启动命令复制粘贴至客户端桌面【Release\任务D提交结果.docx】中对应的任务序号下
    启动1分钟后根据change_machine_id降序查询change_state_other_to_run_agg表并截图粘贴至客户端桌面【Release\任务D提交结果.docx】中对应的任务序号下
    启动2分钟后根据change_machine_id降序查询change_state_other_to_run_agg表并再次截图粘贴至客户端桌面【Release\任务D提交结果.docx】中对应的任务序号下。
    注：时间语义使用Processing Time。
      change_state_other_to_run_agg表：
        字段	类型	中文含义
        change_machine_id	int	设备id
        last_machine_state	varchar	上一状态。即触发本次统计的最近一次非运行状态
        total_change_torun	int	从其他状态转为运行的总次数
        in_time	varchar	flink计算完成时间（yyyy-MM-dd HH:mm:ss）
     */
    //参考demo5-2
    env.execute("demo8")
  }

  def strToLong(date: String): Long = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss").parse(date).getTime
  def dateTodate(date: Date):String=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date)

  class TimePeocessFuntion(Time:Long) extends KeyedProcessFunction[String,String,String] {
    lazy val Sj=getRuntimeContext.getState(new ValueStateDescriptor[Long]("sj",classOf[Long]))
    override def processElement(i: String, context: KeyedProcessFunction[String,String,String]#Context, collector: Collector[String]): Unit = {
      val sj=Sj.value()
      if(sj==0l){
        val ts=context.timerService().currentProcessingTime()+Time
        context.timerService().registerProcessingTimeTimer(ts)
        Sj.update(ts)
        collector.collect(i)
      }
    }
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, String, String]#OnTimerContext, out: Collector[String]): Unit = Sj.clear()
  }
  class MykeyedProcessFuntion(Time:Long) extends KeyedProcessFunction[String,EnvironmentData,String] {
    lazy val DTime=getRuntimeContext.getState(new ValueStateDescriptor[Long]("time",classOf[Long]))
    override def processElement(i: EnvironmentData, context: KeyedProcessFunction[String, EnvironmentData, String]#Context, collector: Collector[String]): Unit = {
      val time=DTime.value()
      if(i.Temperature > 30 && time==0L){
        val ts = context.timerService().currentProcessingTime() + Time
        context.timerService().registerProcessingTimeTimer(ts)
        DTime.update(ts)
      }else if(i.Temperature<30){
        context.timerService().deleteProcessingTimeTimer(time)
        DTime.clear()
      }
    }
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, EnvironmentData, String]#OnTimerContext, out: Collector[String]): Unit = {
      out.collect(ctx.getCurrentKey+","+timestamp.toString)
      DTime.clear()
    }
  }
}
