/*package 大赛


import java.io.{File, FileWriter}
import java.sql.{Connection, DriverManager, PreparedStatement}
import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{AllWindowFunction, ProcessAllWindowFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.util.Collector

import scala.collection.mutable



/**
 *
 * @PROJECT_NAME: Flink
 * @PACKAGE_NAME: 大赛
 * @author: 赵嘉盟-HONOR
 * @data: 2022-11-12 4:11
 * @DESCRIPTION
 *
 */
object 任務書三 {
  case class order_detail(order_id:Long,order_price:Double,sku_num:Long)
  case class order_info3(user_id:String,user_name:String,sum:Double,create_time:String, var operate_time:String,status:String)
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val properties=new Properties()
    properties.setProperty("bootstrap.servers","192.168.174.200:9092")
    val inputStream = env.addSource(new FlinkKafkaConsumer011[String]("order1", new SimpleStringSchema(), properties))
    val dateStream = inputStream.map(data => {
      val datas = data.split(",")
      order_info3(datas(5), datas(1), datas(3).toDouble, datas(10), datas(11), datas(4))
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[order_info3](Time.seconds(5)) {
      override def extractTimestamp(t: order_info3): Long = {
        val create_time = strToLong3(t.create_time)
        val option_time = strToLong3(t.operate_time)
        if (option_time == Nil && option_time == 0) t.operate_time = t.create_time
        if (create_time > option_time) create_time*1000 else option_time*1000
      }
    })
    val qx_outSize=new OutputTag[order_info3]("qx")
    val outPutStream = dateStream.process(new ProcessFunction[order_info3, order_info3] {
      override def processElement(i: order_info3, context: ProcessFunction[order_info3, order_info3]#Context, collector: Collector[order_info3]): Unit = {
        if (i.status=="1003") context.output(qx_outSize,i)
        collector.collect(i)
      }
    })

/*
          任務一
            使用Flink消费Kafka中的数据，实时统计商城中消费额前2的用户
            需要考虑订单状态，若有取消订单、申请退回、退回完成则不计入订单消费额，其他的相加
            将key设置成top2userconsumption存入Redis中
            value使用String数据格式，value为前2的用户信息并且外层用[]包裹，其中按排序依次存放为该用户id:用户名称:消费总额，用逗号分割
            其中用户名称为user_info表中的name字段，可从MySQL中获取
            示例如下：
              top2userconsumption：[1:张三:10020,42:李四:4540]
        */
    val oneStream = outPutStream.filter(data => {
      data.status != "1003" && data.status != "1005" && data.status != "1006"
    }).map(data => (data.user_id, data.sum)).keyBy(0).sum(1)
      .timeWindowAll(Time.minutes(1),Time.seconds(1))
      .apply(new AllWindowFunction[(String,Double),List[(String, Double)], TimeWindow] {
        override def apply(window: TimeWindow, input: Iterable[(String,Double)], out: Collector[List[(String, Double)]]): Unit = {
          out.collect(input.toList.sortBy(_._2).reverse)
        }
      })

    oneStream.addSink(new RichSinkFunction[(String, Double)] {
      var conn :Connection= _
      var selectStem:PreparedStatement= _
      var instStem:PreparedStatement=_

      override def open(parameters: Configuration): Unit = {
        conn = DriverManager.getConnection("jdbc:mysql://192.168.174.200:3306/shud_result?characterEncoding=UTF-8", "root", "root")
        instStem = conn.prepareStatement("insert into one_info values (?,?)")
        selectStem = conn.prepareStatement("select o.user_id,u.name,sum(o.final_total_amount) as num from user_info u join order_info o on o.user_id=u.id group by o.user_id order by num desc limit 2")
      }

      override def invoke(value:(String, Double)): Unit= {
        instStem.setString(1,value._1)
        instStem.setDouble(2,value._2)
        instStem.executeUpdate()
        val rs=selectStem.executeQuery()
/*        rs.next()
        id = rs.getInt("user_id")
        name = rs.getString("name")
        sum=rs.getDouble("num")
        rs.next()
        id2 = rs.getInt("user_id")
        name2 = rs.getString("name")
        sum2=rs.getDouble("num")
    fw.write(id+"\r\n")
    fw.flush*/
      }

      override def close(): Unit = {
        selectStem.close()
        instStem.close()
        conn.close()
      }

      }

/*
  任務二
    计算每分钟内状态为取消订单占所有订单的占比
    将key设置成cancelrate存入Redis中
    value存放取消订单的占比（为百分比，保留百分比后的一位小数，四舍五入，例如12.1%）
*/
//    var count=0
//    var list=0.0
//    val towStream = outPutStream.timeWindowAll(Time.seconds(1)).process(new ProcessAllWindowFunction[order_info3, String, TimeWindow] {
//      override def process(context: Context, elements: Iterable[order_info3], out: Collector[String]): Unit = {
//        elements.foreach(data => {
//          if (data.status == "1003") count += 1
//          list += 1
//        })
//        val sum = (count / list * 100).formatted("%2.1f")
//        out.collect(sum)
//      }
//    })
//    val conf =new FlinkJedisPoolConfig.Builder().setHost("192.168.174.200").setPort(6379).build()
//    towStream.addSink(new RedisSink[String](conf,new RedisMapper[String] {
//      override def getCommandDescription: RedisCommandDescription =  new RedisCommandDescription(RedisCommand.SET)
//
//      override def getKeyFromData(t: String): String = "cancelrate"
//
//      override def getValueFromData(t: String): String = t+"%"
//    }))

/*
  任務三
    使用Flink消费Kafka中的订单详细信息的数据
    实时统计商城中销售额前3的商品（不考虑订单状态，不考虑打折，销售额为order_price*sku_num）
    将key设置成top3itemconsumption存入Redis中（value使用String数据格式，value为前3的商品信息并且外层用[]包裹，其中按排序依次存放商品id:销售额，并用逗号分割）
    示例如下：
      top3itemconsumption：[1:10020.2,42:4540.0,12:540]
*/

//    val mysqlStream=env.addSource(new RichSourceFunction[order_detail] {
//        var conn :Connection= _
//        var instStem:PreparedStatement=_
//        var isRunning=true
//        override def open(parameters: Configuration): Unit = {
//              conn=DriverManager.getConnection("jdbc:mysql://192.168.174.200:3306/shud_result?characterEncoding=UTF-8","root","root")
//              instStem=conn.prepareStatement(" select * from order_detail")
//
//        }
//
//      override def run(sourceContext: SourceFunction.SourceContext[order_detail]): Unit = {
//        while (isRunning) {
//          val set = instStem.executeQuery()
//          while (set.next()){
//            var id=set.getLong("sku_id")
//            var price=set.getDouble("order_price")
//            var num=set.getLong("sku_num")
//            sourceContext.collect(order_detail(id, price, num))
//          }
//        Thread.sleep(5000)
//        }
//      }
//
//      override def cancel(): Unit = isRunning=false
//
//      override def close(): Unit = {
//        instStem.close()
//        conn.close()
//      }
//})
//    val conf=new FlinkJedisPoolConfig.Builder().setHost("192.168.174.200").setPort(6379).build()
//    val map=mutable.Map("a"->1.0,"b"->1.4)
//    mysqlStream.map(data=>{
//      (data.order_id,data.sku_num*data.order_price)
//    }).keyBy(0).sum(1).map(data=>{
//      map+= (data._1.toString->data._2)
//      val tuples = map.toList.sortBy(_._2).reverse.take(3)
//      tuples
//    }).addSink(new RedisSink[List[(String, Double)]](conf,new RedisMapper[List[(String, Double)]] {
//      override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET)
//
//      override def getKeyFromData(t: List[(String, Double)]): String = "top3itemconsumption"
//
//      override def getValueFromData(t: List[(String, Double)]): String = {
//        //top3itemconsumption：[1:10020.2,42:4540.0,12:540]
//        "["+t(0)._1+":"+t(0)._2+","+t(1)._1+":"+t(1)._2+","+t(2)._1+":"+t(2)._2+"]"
//      }
//    }))


    env.execute("任務書三")
  }
  def strToLong3(time: String): Long ={
    val format=new SimpleDateFormat("yyyy-HH-dd hh:MM:ss")
    format.parse(time).getTime
  }

}*/
