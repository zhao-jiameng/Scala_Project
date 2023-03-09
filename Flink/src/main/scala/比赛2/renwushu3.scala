package 比赛2

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import 比赛2.任务书3.strToLong3

import scala.collection.mutable

/**
 *
 * @PROJECT_NAME: Flink
 * @PACKAGE_NAME: 比赛2
 * @author: 赵嘉盟-HONOR
 * @data: 2023-02-10 21:23
 * @DESCRIPTION
 *
 */
object renwushu3 {
  case class OrderInfo(id:Int,num:Double,status:String,create_time:String,option_time:String)
  case class OrderInfo2(id:Int,name:String,create_time:String,option_time:String)
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val mysqlStream=env.addSource( new mysqlSource()).map(data=>{
      OrderInfo2(data._1,data._2,data._3,data._4)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderInfo2](Time.minutes(5)) {
      override def extractTimestamp(t: OrderInfo2): Long = {
        val createTime=strToLong3(t.create_time)
        val optionTime=strToLong3(t.option_time)
        if(optionTime isNaN) return createTime
        if(createTime >= optionTime) createTime else optionTime

      }

    })
    val inputStream=env.readTextFile("src/main/resources/reader.txt").map(data=>{
      val datas = data.split(",")
      OrderInfo(datas(5).toInt,datas(3).toDouble,datas(4),datas(10),datas(11))
    }).filter(data=>data.status != "1003" && data.status!= "1005" && data.status != "1006" ).keyBy(_.id).sum(1)
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderInfo](Time.minutes(5)) {
        override def extractTimestamp(t: OrderInfo): Long = {
          val createTime=strToLong3(t.create_time)
          val optionTime=strToLong3(t.option_time)
          if(optionTime isNaN) return createTime
          if(createTime >= optionTime) createTime else optionTime
        }
      })
    val map=mutable.Map((1,"b")->0.1)
    val conf=new FlinkJedisPoolConfig.Builder().setPort(6379).setHost("192.168.174.200").build()
   mysqlStream.join(inputStream)
        .where(_.id)
        .equalTo(_.id)
        .window(TumblingEventTimeWindows.of(Time.days(5),Time.seconds(1)))
        .apply((e1, e2) => {
          (e1.id, e1.name, e2.num)
        }).map(data=>{
     map+=((data._1,data._2)->data._3)
     map.toList.sortBy(_._2).reverse.take(2)

   }).addSink(new RedisSink[List[((Int, String), Double)]](conf,new RedisMapper[List[((Int, String), Double)]] {
     override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET)
     override def getKeyFromData(t: List[((Int, String), Double)]): String = "top2"
     override def getValueFromData(t: List[((Int, String), Double)]): String = "["+t(0)._1._1+":"+t(0)._1._2+":"+t(0)._2+","+t(1)._1._1+":"+t(1)._1._2+":"+t(1)._2+"]"
   }))
  env.execute("job3")
  }
 class mysqlSource extends SourceFunction[(Int,String,String,String)]{
   var running:Boolean=true
   var conn :Connection= _
   var selectStem:PreparedStatement= _
   override def run(sourceContext: SourceFunction.SourceContext[(Int,String,String,String)]): Unit = {
     conn = DriverManager.getConnection("jdbc:mysql://192.168.174.200:3306/shud_result?characterEncoding=UTF-8", "root", "root")
     selectStem = conn.prepareStatement("select id,name,create_time,ifnull(operate_time,create_time) from user_info ")
     val rs=selectStem.executeQuery()
     while (rs.next()){
       var id=rs.getInt(1)
       var name=rs.getString(2)
       var c=rs.getString(3)
       var o=rs.getString(4)
       sourceContext.collect((id,name,c,o))
     }
   }
   override def cancel(): Unit ={
     selectStem.close()
     conn.close()
     running=false
   }
 }
}
