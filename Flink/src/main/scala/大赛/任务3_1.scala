package 大赛

import java.text.SimpleDateFormat
import java.sql.{Connection, DriverManager, PreparedStatement}

import com.sun.org.glassfish.external.statistics.TimeStatistic
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

import scala.collection.mutable
/**
 *
 * @PROJECT_NAME: Flink
 * @PACKAGE_NAME: 大赛
 * @author: 赵嘉盟-HONOR
 * @data: 2023-02-11 14:02
 * @DESCRIPTION
 *
 */
object 任务3_1 {
  case class Order_info(id:Int,status:String,sum:Double,create_time:String,option_time:String)
  case class Order_info2(id:Int,name:String,create_time:String,option_time:String)
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputStream=env.readTextFile("src/main/resources/reader.txt").map(data=>{
      val datas = data.split(",")
      Order_info(datas(5).toInt,datas(4),datas(3).toDouble,datas(10),datas(11))
    }).filter(data=> data.status != "1003" && data.status != "1005" && data.status != "1006").keyBy(_.id).sum(2)
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Order_info](Time.seconds(5)) {
        override def extractTimestamp(t: Order_info): Long = {
          val create_time=strToLong(t.create_time)
          val option_time=strToLong(t.option_time)
          if (option_time isNaN) return create_time
          if(create_time >=option_time) create_time else option_time

        }
      })

    val mysqlStream=env.addSource( new mysqlSource()).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Order_info2](Time.seconds(5)) {
      override def extractTimestamp(t: Order_info2): Long = {
        val create_time=strToLong(t.create_time)
        val option_time=strToLong(t.option_time)
        if (option_time isNaN) return create_time
        if(create_time >=option_time) create_time else option_time
      }
    })

    //TODO 任务1
    val conf=new FlinkJedisPoolConfig.Builder().setPort(6379).setHost("192.168.174.200").build()
    val map=mutable.Map((1,"a")->0.1)
    inputStream.join(mysqlStream)
      .where(_.id)
      .equalTo(_.id)
      .window(SlidingEventTimeWindows.of(Time.days(1),Time.seconds(1)))
      .apply((e1,e2)=>{
        (e1.id,e2.name,e1.sum)
      }).map(data=>{
      map+=((data._1,data._2)->data._3)
      map.toList.sortBy(_._2).reverse.take(2)
    }).addSink(new RedisSink[List[((Int, String), Double)]](conf,new RedisMapper[List[((Int, String), Double)]] {
      override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET)
      override def getKeyFromData(t: List[((Int, String), Double)]): String = "top2userconsumption"

      override def getValueFromData(t: List[((Int, String), Double)]): String ="["+t(0)._1._1+":"+t(0)._1._2+":"+t(0)._2+","+t(1)._1._1+":"+t(1)._1._2+":"+t(1)._2+"]"
    }))
    env.execute("job3")

  }
  def strToLong(str: String):Long={
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    format.parse(str).getTime
  }
  class mysqlSource extends SourceFunction[Order_info2]{
    var conn:Connection=_
    var selectStem:PreparedStatement=_
    var running=true
    override def run(sourceContext: SourceFunction.SourceContext[Order_info2]): Unit = {
      conn=DriverManager.getConnection("jdbc:mysql://192.168.174.200/shud_result?characterEncoding=UTF-8","root","root")
      selectStem=conn.prepareStatement("select id,name,create_time,ifnull(operate_time,create_time) from user_info")
      val rs=selectStem.executeQuery()
      while (rs.next()){
        var id=rs.getInt(1)
        var name=rs.getString(2)
        var c=rs.getString(3)
        var o=rs.getString(4)
        sourceContext.collect(Order_info2(id,name,c,o))
      }

    }

    override def cancel(): Unit = {
      selectStem.close()
      conn.close()
      running=false
    }
  }
}
