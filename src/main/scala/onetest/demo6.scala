package onetest

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

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
object demo6 {
  case class order_info(id: String, status: String, create_time: String, option_time: String, sun: Double)
  case class order_count(id:String,order_price:Double,sku_num:Int)
  def main(args: Array[String]): Unit = {
    //TODO 准备环境：
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val properties = new Properties()
    properties.setProperty("","")
    //val inputStream=env.a                                                                                                                                                                                                                                           ddSource(new FlinkKafkaConsumer[String]("order",new SimpleStringSchema(),properties))
    val inputStream=env.readTextFile("src/main/resources/order_info.txt")
    val timeStream=inputStream.map(data=>{
      val datas = data.split(",")
      order_info(datas(0), datas(4), datas(10), datas(11), datas(3).toDouble)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[order_info](Time.seconds(5)) {
      override def extractTimestamp(t: order_info): Long = {
        val createtime=strToLong(t.create_time)
        if(t.option_time.equals("")) return createtime
        val optiontime = strToLong(t.option_time)
        if(createtime >= optiontime) createtime else optiontime
      }
    })
    //TODO 任务一：
    /**
     *1、使用Flink消费Kafka中的数据，统计商城实时订单数量（需要考虑订单状态，若有取消订单、申请退回、退回完成则不计入订单数量，其他状态则累加）
     * 将key设置成totalcount存入Redis中。使用redis cli以get key方式获取totalcount值，将结果截图粘贴至客户端桌面【Release\任务D提交结果.docx】中对应的任务序号下
     * 需两次截图，第一次截图和第二次截图间隔1分钟以上，第一次截图放前面，第二次截图放后面；
     */
    val conf=new FlinkJedisPoolConfig.Builder().setPort(6379).setHost("192.168.174.201").build()
    val oneStream=timeStream.filter(data=> data.status!="1003" && data.status!="1005" && data.status!="1006").map(data=>{
      ("totalcount",1)
    }).keyBy(_._1).sum(1).addSink(new RedisSink[(String, Int)](conf,new RedisMapper[(String, Int)] {
      override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET)
      override def getKeyFromData(t: (String, Int)): String = t._1
      override def getValueFromData(t: (String, Int)): String = t._2.toString
    }))
    //TODO 任务二：
    /**
     *2、在任务1进行的同时，使用侧边流，使用Flink消费Kafka中的订单详细信息的数据，实时统计商城中销售量前3的商品（不考虑订单状态，不考虑打折）
     * 将key设置成top3itemamount存入Redis中（value使用String数据格式，value为前3的商品信息并且外层用[]包裹，其中按排序依次存放商品id:销售量，并用逗号分割）
     * 使用redis cli以get key方式获取top3itemamount值，将结果截图粘贴至客户端桌面【Release\任务D提交结果.docx】中对应的任务序号下，需两次截图，第一次截图和第二次截图间隔1分钟以上，第一次截图放前面，第二次截图放后面；
      示例如下：
      top3itemamount：[1:700,42:500,41:100]
     */
/*    val mysqlSource=env.addSource(new RichSourceFunction[order_count] {
      var conn :Connection= _
      var selectStem:PreparedStatement=_
      var isRunning=true
      override def open(parameters: Configuration): Unit = {
         conn=DriverManager.getConnection("jdbc:mysql://192.168.174.201:3306/ds_pul?characterEncoding=utf-8","root","root")
         selectStem=conn.prepareStatement("select * from order_detail")
      }
      override def run(sourceContext: SourceFunction.SourceContext[order_count]): Unit = {
        while (isRunning){
          val set = selectStem.executeQuery()
          while (set.next()){
            var id=set.getLong("sku_id")
            var price=set.getDouble("order_price")
            var num=set.getInt("sku_num")
            sourceContext.collect(order_count(id = id.toString, order_price = price, sku_num = num))
          }
          Thread.sleep(2000)
        }
      }
      override def cancel(): Unit = {isRunning=false}
      override def close(): Unit ={
        selectStem.close()
        conn.close()
      }
    })*/
    val map=mutable.Map("a"->1,"b"->0)
    val detailStream = env.readTextFile("src/main/resources/order_detail.txt").map(data => {
      val datas = data.split(",")
      order_count(datas(2), datas(5).toDouble, datas(6).toInt)
    })
    detailStream.map(data=>{
      (data.id,data.sku_num)
    }).keyBy(0).sum(1).map(data=>{
      map+=(data._1->data._2)
      val tuples = map.toList.sortBy(_._2).reverse.take(3)
      tuples
    }).addSink(new RedisSink[List[(String, Int)]](conf,new RedisMapper[List[(String, Int)]] {
      override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET)
      override def getKeyFromData(t: List[(String, Int)]): String = "top3itemamount"
      override def getValueFromData(t: List[(String, Int)]): String = "["+t(0)._1+":"+t(0)._2+","+t(1)._1+":"+t(1)._2+","+t(2)._1+":"+t(2)._2+"]"
    }))
    //TODO 任务三：
    /**
     *3、在任务1进行的同时，使用侧边流，使用Flink消费Kafka中的订单详细信息的数据，实时统计商城中销售额前3的商品（不考虑订单状态，不考虑打折，销售额为order_price*sku_num）
     * 将key设置成top3itemconsumption存入Redis中（value使用String数据格式，value为前3的商品信息并且外层用[]包裹，其中按排序依次存放商品id:销售额，并用逗号分割）
     * 使用redis cli以get key方式获取top3itemconsumption值，将结果截图粘贴至客户端桌面【Release\任务D提交结果.docx】中对应的任务序号下，需两次截图，第一次截图和第二次截图间隔1分钟以上，第一次截图放前面，第二次截图放后面。
     * 示例如下：
        top3itemconsumption：[1:10020.2,42:4540.0,12:540]
     */
    val map1=mutable.Map("a"->0.1,"b"->0.1)
    detailStream.map(data=>{
      (data.id,(data.sku_num*data.order_price))
    }).keyBy(0).sum(1).map(data=>{
      map1+=(data._1->data._2)
      val tuples=map1.toList.sortBy(_._2).reverse.take(3)
      tuples
    }).addSink(new RedisSink[List[(String, Double)]](conf,new RedisMapper[List[(String, Double)]] {
      override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET)
      override def getKeyFromData(t: List[(String, Double)]): String = "top3itemamount"
      override def getValueFromData(t: List[(String, Double)]): String = "["+t(0)._1+":"+t(0)._2+","+t(1)._1+":"+t(1)._2+","+t(2)._1+":"+t(2)._2+"]"
    }))
  }
  def strToLong(str: String): Long =new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(str).getTime
}
