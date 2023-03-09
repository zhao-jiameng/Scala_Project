package 比赛2

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.text.SimpleDateFormat

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.util.Collector

import scala.collection.mutable

/**
 *
 * @PROJECT_NAME: Flink
 * @PACKAGE_NAME: 比赛2
 * @author: 赵嘉盟-HONOR
 * @data: 2023-02-10 10:35
 * @DESCRIPTION
 *
 */
object 任务书3 {
  case class order_info3(id:String,create_time:String,option_time:String,status:String,mun:Double,name:String)
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    val env1 = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputStream=env.readTextFile("src/main/resources/reader.txt")
    val inputStream2=env.readTextFile("src/main/resources/reader2.txt")

/* val mysqlStream=env addSource new RichSinkFunction[String] {
   var conn :Connection= _
   var selectStem:PreparedStatement= _

   override def open(parameters: Configuration): Unit = {
     conn = DriverManager.getConnection("jdbc:mysql://192.168.174.200:3306/shud_result?characterEncoding=UTF-8", "root", "root")
     selectStem = conn.prepareStatement("select user_id,name, from user_info ")

   }
   override def invoke(value: String): Unit = {
     val rs=selectStem.executeQuery()
     while (rs.next()){
       var id=rs.getInt("user_id")
       var name=rs.getString(2)

     }


   }
   override def close(): Unit = {
     selectStem.close()
     conn.close()
   }
 }*/

/*    val outputStream=inputStream.map(data=>{
      val datas = data.split(",")
      order_info3(datas(0),datas(10),datas(11),datas(4),datas(3).toDouble,datas(1))
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[order_info3](Time.seconds(5)) {
      override def extractTimestamp(t: order_info3): Long = {
        val createTime=strToLong3(t.create_time)
        val optionTime=strToLong3(t.option_time)
        if(optionTime isNaN) return createTime
        if(createTime >= optionTime) createTime else optionTime
      }
    })*/
/*
    // TODO 任务1
    val mysqlSourceSql =
      """
        |create table mysqlSourceTable (
        |  ID bigint,
        |  NAME string,
        |  DEVELOPER_ID bigint,
        |  DEVELOPER_SHARE decimal(11,2),
        |  STATE tinyint,
        |  WEB_OPEN tinyint,
        |  IS_MULTIPLE tinyint,
        |  CP_GAME_NAME string,
        |  SM_GAME_NAME string,
        |  REMARK string,
        |  CP_GAME_ID int,
        |  UPDATE_TIME TIMESTAMP
        |) with (
        | 'connector' = 'jdbc',
        | 'url' = 'jdbc:mysql://192.168.100.39:3306/game_platform?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
        | 'username' = 'root',
        | 'password' = '123456',
        | 'table-name' = 'games',
        | 'driver' = 'com.mysql.cj.jdbc.Driver',
        | 'scan.fetch-size' = '200'
        |)
    """.stripMargin

    val insertSql =
      """
        |insert into printSinkTable
        |select * from mysqlSourceTable
      """.stripMargin

    // TODO 任务2
    var qx_count=0
    var qb_count=0.0
    val towStream=outputStream.timeWindowAll(Time.minutes(1)).process(new ProcessAllWindowFunction[order_info3,String,TimeWindow] {
      override def process(context: Context, elements: Iterable[order_info3], out: Collector[String]): Unit = {
        elements.foreach(data=>{
          if(data.status == "1003") qx_count+=1
          qb_count+=1
        })
        val str=(qx_count/qb_count*100).formatted("%2.1f")
        out.collect(str)
      }
    })
    val conf=new FlinkJedisPoolConfig.Builder().setHost("192.168.174.200").setPort(6379).build()
    towStream.addSink(new RedisSink[String](conf,new RedisMapper[String] {
      override def getCommandDescription: RedisCommandDescription = new  RedisCommandDescription(RedisCommand.SET)
      override def getKeyFromData(t: String): String = "cancelrate"

      override def getValueFromData(t: String): String = t+"%"
    }))

    //TODO 任务3
    val map=mutable.Map("a"->0,"b"->0)
    val threeStream=inputStream2.map(data=>{
      val datas = data.split(",")
      (datas(2),datas(6).toInt)
    }).keyBy(_._1).sum(1).map(data=>{
      map+=(data._1->data._2)
      val tuples = map.toList.sortBy(_._2).reverse.take(3)
      tuples
    }).print()*/
/*    threeStream.addSink(new RedisSink[List[(String, Double)]](conf,new RedisMapper[List[(String, Double)]] {
      override def getCommandDescription: RedisCommandDescription = new  RedisCommandDescription(RedisCommand.SET)

      override def getKeyFromData(t: List[(String, Double)]): String = "top3itemconsumption"

      override def getValueFromData(t: List[(String, Double)]): String = "["+t(0)._1+":"+t(0)._2+","+t(1)._1+":"+t(1)._2+","+t(2)._1+":"+t(2)._2+"]"
    }))*/
    env.execute("job3")
  }
  def strToLong3(str: String):Long={
    val format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    format.parse(str).getTime
  }

}
