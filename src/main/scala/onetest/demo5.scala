package onetest

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{ProcessAllWindowFunction, ProcessWindowFunction}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.util.Collector
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Table}

import scala.collection.mutable
/**
 *
 * @PROJECT_NAME: 国赛
 * @PACKAGE_NAME: onetest
 * @author: 赵嘉盟-HONOR
 * @data: 2023-06-03 21:00
 * @DESCRIPTION
 *
 */
object demo5 {
  case class produceecord(id:String,time:String,status:Int,state:Int)
  case class changerecord(id:String,zt:String,time:String)
  def main(args: Array[String]): Unit = {
    //TODO 准备环境：
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val inputStream=env.readTextFile("src/main/resources/producerecord.txt").map(data=>{
      val datas = data.split(",")
      produceecord(datas(1),datas(5),datas(2).toInt,datas(9).toInt)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[produceecord](Time.seconds(5)) {
      override def extractTimestamp(t: produceecord): Long = strToLong(t.time)
    })
    val inputStream2=env.readTextFile("src/main/resources/changerecord.txt").map(date=>{
      val datas = date.split(",")
      changerecord(datas(1),datas(3),datas(4))
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[changerecord](Time.seconds(5)) {
      override def extractTimestamp(t: changerecord): Long = strToLong(t.time)
    })
    //TODO 任务一：
    /**
     *1、使用Flink消费Kafka中ProduceRecord主题的数据，统计在已经检验的产品中，各设备每5分钟生产产品总数
     * 将结果存入HBase中的gyflinkresult:Produce5minAgg表，rowkey“设备id-系统时间”（如：123-2023-01-01 12:06:06.001）
     * 将结果截图粘贴至客户端桌面【Release\任务D提交结果.docx】中对应的任务序号下，需两次截图，第一次截图和第二次截图间隔5分钟以上，第一次截图放前面，第二次截图放后面；
      注：ProduceRecord主题，每生产一个产品产生一条数据；
      change_handle_state字段为1代表已经检验，0代表为检验；
      时间语义使用Processing Time。
        字段	          类型	    中文含义
        rowkey	      String	设备id-系统时间（如：123-2023-01-01 12:06:06.001）
        machine_id	  String	设备id
        total_produce	String	最近5分钟生产总数
     */
    inputStream.filter(_.state == 0).map(data => (data.id, 1)).keyBy(0).timeWindowAll(Time.minutes(5)).sum(1)
      .addSink(new RichSinkFunction[(String, Int)] {
        var connection: org.apache.hadoop.hbase.client.Connection = _
        var hbTable: Table = _
        override def open(parameters: Configuration): Unit = {
          val conf = HBaseConfiguration.create()
          conf.set("hbase.zookeeper.quorum", "master,slave1,slave2")
          connection = ConnectionFactory.createConnection(conf)
          hbTable = connection.getTable(TableName.valueOf("gyflinkresult:Produce5minAgg"))
        }
        override def invoke(value: (String, Int)): Unit = {
          val put = new Put((value._1+"-"+dateTodate(new Date())).getBytes())
          put.addColumn("info".getBytes,"machine_id".getBytes(),value._1.getBytes)
          put.addColumn("info".getBytes,"total_produce".getBytes(),value._2.toString.getBytes)
          hbTable.put(put)
        }
        override def close(): Unit = {
          hbTable.close()
          connection.close()
        }
      })
    //TODO 任务二：
    /**
     *2、使用Flink消费Kafka中ChangeRecord主题的数据，实时统计每个设备从其他状态转变为“运行”状态的总次数
     * 将结果存入MySQL数据库shtd_industry的change_state_other_to_run_agg表中（表结构如下）
     * 请将任务启动命令复制粘贴至客户端桌面【Release\任务D提交结果.docx】中对应的任务序号下
     * 启动1分钟后根据change_machine_id降序查询change_state_other_to_run_agg表并截图粘贴至客户端桌面【Release\任务D提交结果.docx】中对应的任务序号下
     * 启动2分钟后根据change_machine_id降序查询change_state_other_to_run_agg表并再次截图粘贴至客户端桌面【Release\任务D提交结果.docx】中对应的任务序号下；
        注：时间语义使用Processing Time。
        change_state_other_to_run_agg表：
          字段	                类型	      中文含义
          change_machine_id	  int	      设备id
          last_machine_state	varchar	  上一状态。即触发本次统计的最近一次非运行状态
          total_change_torun	int	      从其他状态转为运行的总次数
          in_time	            varchar	  flink计算完成时间（yyyy-MM-dd HH:mm:ss）
     */
    //val joinStream1 = inputStream2.filter(_.zt == "运行").map(date => (date.id, 1)).keyBy(_._1).sum(1)
    val joinStream2 = inputStream2.map(data => (data.id, data.zt)).keyBy(_._1).window(TumblingEventTimeWindows.of(Time.days(1)))
      .process(new ProcessWindowFunction[(String,String),(String,String,Int,String),String,TimeWindow] {
        override def process(key: String, context: Context, elements: Iterable[(String, String)], out: Collector[(String,String,Int,String)]): Unit = {
          var count=0
          var zt="待机"
          elements.foreach(data=>{
            if("运行"==data._2) count+=1
            else {
              zt=data._2
              out.collect(data._1,data._2,count,dateTodate(new Date()))}
          })
        }
      })
     .addSink(new RichSinkFunction[(String, String, Int, String)] {
      var conn:java.sql.Connection=_
      var insert:java.sql.PreparedStatement=_
      override def open(parameters: Configuration): Unit ={
        conn=DriverManager.getConnection("jdbc:mysql://localhost:3306/gy_pub?characterEncoding=utf-8","root","123456")
        insert=conn.prepareStatement("replace into change_state_other_to_run_agg values (?,?,?,?)")
        //insert=conn.prepareStatement("insert into change_state_other_to_run_agg values (?,?,?,?)")
      }
      override def invoke(value: (String, String, Int, String)): Unit = {
        insert.setString(1,value._1)
        insert.setString(2,value._2)
        insert.setInt(3,value._3)
        insert.setString(4,value._4)
        insert.executeUpdate()
      }
      override def close(): Unit = {
        insert.close()
        conn.close()
      }
    })
/*      .process(new KeyedProcessFunction[String, (String, String), (String, String)] {
        override def processElement(i: (String, String), context: KeyedProcessFunction[String, (String, String), (String, String)]#Context, collector: Collector[(String, String)]): Unit = {
          var zt = "运行"
          if ("运行" != i._2) {
            zt = i._2
            collector.collect((i._1, zt))
          }
        }
      })*/
/*    joinStream1.join(joinStream2)
      .where(_._1).equalTo(_._1)
      .window(TumblingEventTimeWindows.of(Time.days(1)))
      .apply((e1,e2)=>(e1._1,e1._2,e2._2)).print("sum")*/

/*val joinStream3 = inputStream2.map(date => (date.id, date.zt)).keyBy(_._1).flatMapWithState[(String,Int,String),String]({
  case (date:(String,String),None ) => (List.empty,Some(date._2))
  case (date:(String,String),lastZt:Some[String] ) =>{
    var count=0
    if (date._2=="运行" ) count+=1
    if(lastZt != "运行") (List((date._1,count,date._2)),Some(date._2))
    else (List.empty,Some(date._2))
  }
})*/
    //TODO 任务三：
    /**
     *3、使用Flink消费Kafka中ChangeRecord主题的数据，每隔1分钟输出最近3分钟的预警次数最多的设备，将结果存入Redis中
     * key值为“warning_last3min_everymin_out”，value值为“窗口结束时间，设备id”（窗口结束时间格式：yyyy-MM-dd HH:mm:ss）
     * 使用redis cli以HGETALL key方式获取warning_last3min_everymin_out值，将结果截图粘贴至客户端桌面【Release\任务D提交结果.docx】中对应的任务序号下
     * 需两次截图，第一次截图和第二次截图间隔1分钟以上，第一次截图放前面，第二次截图放后面。
     */
      val conf=new FlinkJedisPoolConfig.Builder().setPort(6379).setHost("1921.68.174.201").build()
    val map=mutable.Map("a"->1)
    inputStream.map(data=>(data.id,1)).keyBy(_._1).sum(1)
      .windowAll(TumblingEventTimeWindows.of(Time.minutes(3)))
      .process(new ProcessAllWindowFunction[(String,Int),List[(String,Int)],TimeWindow] {
        override def process(context: Context, elements: Iterable[(String, Int)], out: Collector[List[(String,Int)]]): Unit = {
          elements.foreach(date=>{
            map+=(date._1->date._2)
          })
          val tuples = map.toList.sortBy(_._2).reverse.take(1)
          out.collect(tuples)
        }
      }).addSink(new RedisSink[List[(String, Int)]](conf,new RedisMapper[List[(String, Int)]] {
      override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.HSET,"warning_last3min_everymin_out")
      override def getKeyFromData(t: List[(String, Int)]): String = dateTodate(new Date())
      override def getValueFromData(t: List[(String, Int)]): String = t(0)._1

    }))
    Thread.sleep(6000)
    env.execute("demo5")
  }
  def strToLong(str: String): Long = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss").parse(str).getTime
  def dateTodate(date: Date):String=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date)
}
