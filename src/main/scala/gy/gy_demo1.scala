package gy

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{SlidingProcessingTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.util.Collector
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

import scala.collection.mutable

/**
 *
 * @PROJECT_NAME: BIgData
 * @PACKAGE_NAME: gy
 * @author: 赵嘉盟-HONOR
 * @data: 2023-09-05 15:08
 * @DESCRIPTION
 *
 */
object gy_demo1 extends Runnable{
  case class change_record(id:String,status:String,change_start_time:String)
  case class environment_data(id:Int,status:Double)
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(100,0L))
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "192.168.23.60:9092")
    val conf=new FlinkJedisPoolConfig.Builder().setHost("192.168.23.60").setPort(6379).build()
    val changeStream = env.addSource(new FlinkKafkaConsumer[String]("ChangeRecord", new SimpleStringSchema(), properties))
    val ProduceRecord = env.addSource(new FlinkKafkaConsumer[String]("ProduceRecord", new SimpleStringSchema(), properties))
    val EnvironmentData = env.addSource(new FlinkKafkaConsumer[String]("EnvironmentData", new SimpleStringSchema(), properties))
    //TODO Dome1
    /**
     * 1、使用Flink消费Kafka中ProduceRecord主题的数据，统计在已经检验的产品中，各设备每五分钟生产产品总数，
     * 将结果存入Redis中，key值为“totalproduce”，value值为“设备id，最近五分钟生产总数”。
     * 使用redis cli以HGETALL key方式获取totalproduce值，将结果截图粘贴至对应报告中，需两次截图，第一次截图和第二次截图间隔五分钟以上，第一次截图放前面，第二次放后面；
     * 注：ProduceRecord主题，生产一个产品产生一条数据；
     * change_handle_state字段为1代表已经检验，0代表未检验；
     * 时间语义使用Processing Time。
     */
      ProduceRecord.map(data=>{
        val datas = data.split(",")
        (datas(1),datas(9).toInt)
      }).filter(_._2==1).map(data=>(data._1,1)).keyBy(_._1).window(TumblingProcessingTimeWindows.of(Time.minutes(5))).sum(1)
        .addSink(new RedisSink[(String, Int)](conf,new RedisMapper[(String, Int)] {
          override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.HSET,"totalproduce")
          override def getKeyFromData(t: (String, Int)): String = t._1
          override def getValueFromData(t: (String, Int)): String = t._2.toString
        }))
    //TODO Dome2
    /**
     * 2、使用Flink消费Kafka中ChangeRecord主题的数据，当某设备30秒状态连续为“预警”，输出预警信息。
     * 当前预警信息输出后，最近30秒不再重复预警（即如果连续1分钟状态都为“预警”只输出两次预警信息）。
     * 将结果存入Redis中，key值为“warning30sMachine”，value值为“设备id，预警信息”。
     * 使用redis cli以HGETALL key方式获取warning30sMachine值，将结果截图粘贴至对应报告中，需两次截图，第一次截图和第二次截图间隔一分钟以上，第一次截图放前面，第二次放后面；
     * 注：时间使用change_start_time字段，忽略数据中的change_end_time不参与任何计算。忽略数据迟到问题。
     * Redis的value示例：115,2022-01-01 09:53:10:设备115 连续30秒为预警状态请尽快处理！
     * (2022-01-01 09:53:10 为change_start_time字段值，中文内容及格式必须为示例所示内容。)
     */
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
      changeStream.map(data=>{
        val datas = data.split(",")
        change_record(datas(1),datas(3),datas(4))
      }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[change_record](Time.seconds(5)) {
        override def extractTimestamp(t: change_record): Long = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(t.change_start_time).getTime
      }).keyBy(_.id).process(new KeyedProcessFunction[String,change_record, (String,String)] {
        lazy val Time=getRuntimeContext.getState(new ValueStateDescriptor[Long]("Time",classOf[Long]))
        lazy val Date=getRuntimeContext.getState(new ValueStateDescriptor[String]("Date",classOf[String]))
        override def processElement(i: change_record, context: KeyedProcessFunction[String, change_record, (String,String)]#Context, collector: Collector[ (String,String)]): Unit = {
          val time=Time.value()
          if("预警".equals(i.status) && time==0L){
            val ts=context.timerService().currentProcessingTime()+30000L
            context.timerService().registerProcessingTimeTimer(ts)
            Time.update(ts)
            Date.update(i.change_start_time)
          }else if(i.status != "预警"){
            context.timerService().deleteProcessingTimeTimer(time)
            Time.clear()
          }
        }
        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, change_record,  (String,String)]#OnTimerContext, out: Collector[ (String,String)]): Unit = {
            out.collect((ctx.getCurrentKey,Date.value()))
            Time.clear()
        }
      }).keyBy(_._1).process(new KeyedProcessFunction[String,(String,String),(String,String)] {
        lazy val Sj=getRuntimeContext.getState(new ValueStateDescriptor[String]("sj",classOf[String]))
        override def processElement(i: (String, String), context: KeyedProcessFunction[String, (String, String), (String, String)]#Context, collector: Collector[(String, String)]): Unit = {
          if (Sj.value() == null) {
            val ts = context.timerService().currentProcessingTime() + 30000L
            context.timerService().registerProcessingTimeTimer(ts)
            Sj.update(i._1)
            collector.collect((i._1,i._2))
          }
        }
          override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, (String, String), (String, String)]#OnTimerContext, out: Collector[(String, String)]): Unit = Sj.clear()
      }).addSink(new RedisSink[(String, String)](conf,new RedisMapper[(String, String)] {
        override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.HSET,"warning30sMachine")
        override def getKeyFromData(t: (String, String)): String = t._1+","+t._2
        override def getValueFromData(t: (String, String)): String ="设备"+t._1+"连续30秒为预警状态请尽快处理！"
      }))
    //TODO Dome3
    /**
     * 3、使用Flink消费Kafka中ChangeRecord主题的数据，统计每三分钟各设备状态为“预警”且未处理的数据总数。
     * 将结果存入MySQL的shtd_industry.threemin_warning_state_agg表（追加写入），表结构如下，同时备份到Hbase一份，表结构同MySQL表的。
     * 请在将任务启动命令截图，启动且数据进入后按照设备id倒序排序查询threemin_warning_state_agg表进行截图，第一次截图后等待三分钟再次查询并截图,将结果截图粘贴至对应报告中。
     *    threemin_warning_state_agg表：
     *        字段	              类型	    中文含义
     *        change_machine_id	int	    设备id
     *        totalwarning	    int	    未被处理预警的数据总数
     *        window_end_time	  varchar	窗口结束时间（yyyy-MM-dd HH:mm:ss）
     * 注：时间语义使用Processing Time。
     */
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    changeStream.map(data => {
      val datas = data.split(",")
      change_record(datas(1), datas(3), datas(6))
    }).filter(_.status == "预警").filter(_.change_start_time.toInt % 2 == 0).map(data => (data.id, 1))
      .keyBy(_._1).sum(1).windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)))
      .apply(new AllWindowFunction[(String, Int), (String, Int, String), TimeWindow] {
        override def apply(window: TimeWindow, input: Iterable[(String, Int)], out: Collector[(String, Int, String)]): Unit = {
          input.foreach(data => {
            out.collect(data._1, data._2, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(window.getEnd))
          })
        }
      }).addSink(new RichSinkFunction[(String, Int, String)] {
      var Mconn:java.sql.Connection=_
      var MinsertStem:java.sql.PreparedStatement=_
      var Hconn:org.apache.hadoop.hbase.client.Connection=_
      var Htable:org.apache.hadoop.hbase.client.Table=_
      override def open(parameters: Configuration): Unit = {
        Mconn=DriverManager.getConnection("jdbc:mysql://192.168.23.60:3306/shtd_industry?characterEncoding=UTF-8&useSSL=false","root","123456")
        val Hconf=org.apache.hadoop.hbase.HBaseConfiguration.create()
        Hconf.set("hbase.zookeeper.quorum","bigdata1,bigdata2,bigdata3")
        Hconn=org.apache.hadoop.hbase.client.ConnectionFactory.createConnection(Hconf)
        MinsertStem=Mconn.prepareStatement("replace into threemin_warning_state_agg values (?,?,?)")
        Htable=Hconn.getTable(TableName.valueOf("shtd_industry.threemin_warning_state_agg"))
      }
      override def invoke(value: (String, Int, String)): Unit = {
        MinsertStem.setInt(1,value._1.toInt)
        MinsertStem.setInt(2,value._2)
        MinsertStem.setString(3,value._3)
        val put = new Put(value._1.getBytes())
        put.addColumn("info".getBytes,"change_machine_id".getBytes(),value._1.getBytes())
        put.addColumn("info".getBytes,"totalwarning".getBytes(),value._2.toString.getBytes())
        put.addColumn("info".getBytes,"window_end_time".getBytes(),value._3.getBytes())
        MinsertStem.executeUpdate()
        Htable.put(put)
      }
      override def close(): Unit = {
        Htable.close()
        MinsertStem.close()
        Hconn.close()
        Mconn.close()
      }
    })
    //TODO Dome4
    /**
     * 4、使用Flink消费Kafka中ChangeRecord主题的数据，实时统计每个设备从其他状态转变为“运行”状态的总次数。
     * 将结果存入MySQL的shtd_industry.change_state_other_to_run_agg表，表结构如下，同时备份到Hbase一份，表结构同MySQL表的。
     * 请在将任务启动命令截图，启动一分钟后根据change_machine_id降序查询change_state_other_to_run_agg表并截图
     * 启动两分钟后根据change_machine_id降序查询change_state_other_to_run_agg表并再次截图；
     * 注：时间语义使用Processing Time。
     *    change_state_other_to_run_agg表：
     *        字段	                类型	      中文含义
     *        change_machine_id	  int	      设备id
     *        last_machine_state	varchar	  上一状态。即触发本次统计的最近一次非运行状态
     *        total_change_torun	int	      从其他状态转为运行的总次数
     *        in_time	            varchar	  flink计算完成时间（yyyy-MM-dd HH:mm:ss）
     */
    val foreStream = changeStream.map(data => {
      val datas = data.split(",")
      change_record(datas(1), datas(3), datas(4))
    }).keyBy(_.id).process(new KeyedProcessFunction[String, change_record, String] {
      lazy val Status = getRuntimeContext.getState(new ValueStateDescriptor[String]("status", classOf[String]))
      lazy val Count = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count", classOf[Long]))

      override def processElement(i: change_record, context: KeyedProcessFunction[String, change_record, String]#Context, collector: Collector[String]): Unit = {
        if(Status.value()==null) Status.update("未获取")
        val status = Status.value()
        if (i.status != "运行" && status =="未获取") {
          Status.update(i.status)
        } else if (i.status == "运行") {
          Count.update(Count.value() + 1)
          collector.collect(context.getCurrentKey + "," + status + "," + Count.value() + "," + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()))
        } else {
          Status.update(i.status)
        }
      }
    })
    foreStream.addSink(new RichSinkFunction[String] {
      var conn:java.sql.Connection=_
      var insertStem:java.sql.PreparedStatement=_
      override def open(parameters: Configuration): Unit = {
        conn=java.sql.DriverManager.getConnection("jdbc:mysql://192.168.23.60:3306/shtd_industry?characterEncoding=UTF-8&amp;useSSL=false","root","123456")
        insertStem=conn.prepareStatement("replace into change_state_other_to_run_agg values(?,?,?,?)")
      }
      override def invoke(value: String): Unit = {
        val datas = value.split(",")
        insertStem.setInt(1,datas(0).toInt)
        insertStem.setString(2,datas(1))
        insertStem.setInt(3,datas(2).toInt)
        insertStem.setString(4,datas(3))
        insertStem.executeUpdate()
      }
      override def close(): Unit = {
        insertStem.close()
        conn.close()
      }
    })
    foreStream.addSink(new RichSinkFunction[String] {
      var conn: org.apache.hadoop.hbase.client.Connection = _
      var HBtable: org.apache.hadoop.hbase.client.Table= _

      override def open(parameters: Configuration): Unit = {
        val HBconf=HBaseConfiguration.create()
        HBconf.set("hbase.zookeeper.quorum","bigdata1,bigdata2,bigdata3")
        conn=org.apache.hadoop.hbase.client.ConnectionFactory.createConnection(HBconf)
        HBtable=conn.getTable(TableName.valueOf("shtd_industry:change_state_other_to_run_agg"))
      }
      override def invoke(value: String): Unit = {
        val datas = value.split(",")
        val put = new Put(datas(0).getBytes())
        put.addColumn("info".getBytes(),"change_machine_id".getBytes(),datas(0).getBytes())
        put.addColumn("info".getBytes(),"last_machine_state".getBytes(),datas(1).getBytes())
        put.addColumn("info".getBytes(),"total_change_torun".getBytes(),datas(2).getBytes())
        put.addColumn("info".getBytes(),"in_time".getBytes(),datas(3).getBytes())
        HBtable.put(put)
      }
      override def close(): Unit = {
        HBtable.close()
        conn.close()
      }
    })
    //TODO Dome5
    /**
     * 5、使用Flink消费Kafka中ChangeRecord主题的数据，每隔一分钟输出最近三分钟的预警次数最多的设备。
     * 将结果存入Redis中，key值为“warning_last3min_everymin_out”
     * value值为“窗口结束时间，设备id”（窗口结束时间格式：yyyy-MM-dd HH:mm:ss）
     * 使用redis cli以HGETALL key方式获取warning_last3min_everymin_out值，将结果截图粘贴至对应报告中
     * 需两次截图，第一次截图和第二次截图间隔一分钟以上，第一次截图放前面，第二次放后面；
     * 注：时间语义使用Processing Time。
     */
    val map=mutable.Map("a"->0)
      changeStream.map(data=>{
        val datas = data.split(",")
        change_record(datas(1), datas(3), datas(4))
      }).filter(_.status=="预警").map(data=>(data.id,1)).keyBy(_._1).sum(1)
        .windowAll(SlidingProcessingTimeWindows.of(Time.minutes(3),Time.minutes(1)))
        .apply(new AllWindowFunction[(String,Int),(String,String),TimeWindow]() {
          override def apply(window: TimeWindow, input: Iterable[(String, Int)], out: Collector[(String, String)]): Unit = {
            input.foreach(data=>map+=(data._1->data._2))
            out.collect((map.toList.sortBy(_._2).reverse.take(1)(0)._1,new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(window.getEnd)))
          }
        }).addSink(new RedisSink[(String, String)](conf,new RedisMapper[(String, String)] {
        override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.HSET,"warning_last3min_everymin_out")
        override def getKeyFromData(t: (String, String)): String = t._2
        override def getValueFromData(t: (String, String)): String = t._1
      }))
    //TODO Dome6
    /**
     * 6、使用Flink消费Kafka中EnvironmentData主题的数据,监控各环境检测设备数据，当温度（Temperature字段）持续3分钟高于38度时记录为预警数据。
     * 将结果存入Redis中，key值为“env_temperature_monitor”，value值为“设备id-预警信息生成时间，预警信息”（预警信息生成时间格式：yyyy-MM-dd HH:mm:ss）
     * 使用redis cli以HGETALL key方式获取env_temperature_monitor值，将结果截图粘贴至对应报告中，需要两次截图间隔三分钟以上，第一次截图放前面，第二次放后面。
     * 注：时间语义使用Processing Time。
     * value示例：114-2022-01-01 14:12:19，设备114连续三分钟温度高于38度请及时处理！
     * 中文内容及格式必须为示例所示内容。
     * 同一设备3分钟只预警一次。
     */
    EnvironmentData.map(data=>{
      val datas = data.split(",")
      environment_data(datas(1).toInt,datas(5).toDouble)
    }).keyBy(_.id).process(new KeyedProcessFunction[Int,environment_data,String] {
      lazy val Time=getRuntimeContext.getState(new ValueStateDescriptor[Long]("Time",classOf[Long]))
      override def processElement(i: environment_data, context: KeyedProcessFunction[Int, environment_data, String]#Context, collector: Collector[String]): Unit = {
        val time=Time.value()
        if(i.status>=38 && time==0L){
          val ts=context.timerService().currentProcessingTime()+180000L
          context.timerService().registerProcessingTimeTimer(ts)
          Time.update(ts)
        }else if(i.status<38){
          context.timerService().deleteProcessingTimeTimer(time)
        }
      }
      override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Int, environment_data, String]#OnTimerContext, out: Collector[String]): Unit = {
        out.collect(ctx.getCurrentKey+","+new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()))
      }
    }).addSink(new RedisSink[String](conf,new RedisMapper[String] {
      override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.HSET,"env_temperature_monitor")
      override def getKeyFromData(t: String): String =t.split(",")(0)+"-"+t.split(",")(1)
      override def getValueFromData(t: String): String = "设备"+t.split(",")(0)+"连续三分钟温度高于38度请及时处理！"
    }))
    new Thread(gy_demo1).start()
    env.execute("gy_demo1")
  }
  override def run(): Unit = {
    var count = 1
    while (true) {
      Thread.sleep(60000)
      println("GY_Demo1=>程序已运行" + count + "分钟")
      count += 1
    }
  }
}
