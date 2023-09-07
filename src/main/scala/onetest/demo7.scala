package onetest
import java.util.Properties

import onetest.demo6.strToLong
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Table}

import scala.collection.mutable
/**
 *
 * @PROJECT_NAME: 国赛
 * @PACKAGE_NAME: onetest
 * @author: 赵嘉盟-HONOR
 * @data: 2023-06-03 21:01
 * @DESCRIPTION
 *
 */
object demo7 {
  case class order_info(id: String,status: String, create_time: String, option_time: String, sun: Double)
  case class order_count(id:String,sku_id:String,order_price:Double,sku_num:Int,create_time: String)
  def main(args: Array[String]): Unit = {
    //TODO 准备环境：
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val properties = new Properties()
    properties.setProperty("","")
    //val inputStream=env.addSource(new FlinkKafkaConsumer[String]("order",new SimpleStringSchema(),properties))
    val inputStream=env.readTextFile("src/main/resources/order_info.txt").map(data=>{
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
    val inputStream1=env.readTextFile("src/main/resources/order_detail.txt").map(data=>{
      val datas = data.split(",")
      order_count(datas(1),datas(2),datas(5).toDouble,datas(6).toInt,datas(7))
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[order_count](Time.seconds(5)) {
      override def extractTimestamp(t: order_count): Long = strToLong(t.create_time)
    })

    //TODO 任务一：
    /**
     *1、使用Flink消费Kafka中的数据，统计商城实时订单数量（需要考虑订单表的状态，若有取消订单、申请退回、退回完成则不计入订单数量，其他状态则累加）
     * 将key设置成totalcount存入Redis中。使用redis cli以get key方式获取totalcount值
     * 将结果截图粘贴至客户端桌面【Release\任务D提交结果.docx】中对应的任务序号下，需两次截图，第一次截图和第二次截图间隔1分钟以上，第一次截图放前面，第二次截图放后面；
     *
     */
      val conf=new FlinkJedisPoolConfig.Builder().setPort(6375).setHost("").build()
      val oneStream=inputStream.filter(data=>data.status!="1003" && data.status!="1005" && data.status!="1006" ).map(data=>{
        ("totalcount",1)
    }).keyBy(0).sum(1).addSink(new RedisSink[(String, Int)](conf,new RedisMapper[(String, Int)] {
        override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET)
        override def getKeyFromData(t: (String, Int)): String = t._1
        override def getValueFromData(t: (String, Int)): String = t._2.toString
      }))
    //TODO 任务二：
    /**
     *2、在任务1进行的同时，使用侧边流，使用Flink消费Kafka中的订单详细信息数据，实时统计商城中消费额前3的商品（不考虑订单状态，不考虑打折）
     * 将key设置成top3itemconsumption存入Redis中（value使用String数据格式，value为前3的商品信息并外层用[]包裹，其中按排序依次存放商品id:销售额，并用逗号分割）
     * 使用redis cli以get key方式获取top3itemconsumption值，将结果截图粘贴至客户端桌面【Release\任务D提交结果.docx】中对应的任务序号下，需两次截图，第一次截图和第二次截图间隔1分钟以上，第一次截图放前面，第二次截图放后面；
      示例如下：
      top3itemconsumption：[1:10020.2,42:4540.0,12:540]
     */
      val map=mutable.Map("a"->0.1)
      val towStream=inputStream1.map(data=>(data.sku_id,data.order_price*data.sku_num)).keyBy(0).sum(1).map(data=>{
        map+=(data._1->data._2)
        val tuples = map.toList.sortBy(_._1).reverse.take(3)
        tuples
      }).addSink(new RedisSink[List[(String, Double)]](conf,new RedisMapper[List[(String, Double)]] {
        override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET)
        override def getKeyFromData(t: List[(String, Double)]): String = "top3itemconsumption"
        override def getValueFromData(t: List[(String, Double)]): String = "["+t(0)._1+":"+t(0)._2+","+t(1)._1+":"+t(1)._2+","+t(2)._1+":"+t(2)._2+"]"
      }))
    //TODO 任务三：
    /**
     *3、采用双流JOIN的方式（本系统稳定，无需担心数据迟到与丢失的问题,建议使用滚动窗口）
     * 结合订单信息和订单详细信息（需要考虑订单状态，若有取消订单、申请退回、退回完成则不进行统计），拼接成如下表所示格式
     * 其中包含订单id、订单总金额、商品数，将数据存入HBase数据库(namespace)shtd_result的的orderpositiveaggr表中（表结构如下）
     * 然后在Linux的HBase shell命令行中查询出任意5条数据，查询列orderprice、orderdetailcount
     * 将执行结果截图粘贴至客户端桌面【Release\任务D提交结果.docx】中对应的任务序号下，将执行结果截图粘贴至客户端桌面【Release\任务D提交结果.docx】中对应的任务序号下。

      表空间为：shtd_result，表为orderpositiveaggr，列族为：info
        字段	              类型	    中文含义	    备注
        rowkey	          string	HBase的主键	值为id
        id	              bigint	订单id
        orderprice	      string	订单总金额	  统计订单信息中 final_total_amount字段
        orderdetailcount	string	商品数量总计	统计订单详细信息中 sku_num字段

     */
    val joinStream=env.readTextFile("src/main/resources/order_info.txt").map(data=>{
      val strings = data.split(",")
      (strings(0),strings(3))
    })
    val joinStream2=env.readTextFile("src/main/resources/order_detail.txt").map(data => {
      val strings = data.split(",")
      (strings(1), strings(6))
    })
    joinStream.join(joinStream2)
      .where(_._1).equalTo(_._1)
      .window(SlidingEventTimeWindows.of(Time.days(1),Time.seconds(5)))
      .apply((e1,e2)=>(e1._1,e1._2,e2._2))
      .addSink(new RichSinkFunction[(String, String, String)] {
        var conn:org.apache.hadoop.hbase.client.Connection=_
        var HBtable:Table=_
        override def open(parameters: Configuration): Unit = {
          val conf=HBaseConfiguration.create()
          conf.set("","")
          conn=ConnectionFactory.createConnection()
          HBtable=conn.getTable(TableName.valueOf("shtd_result:orderpositiveaggr")) //tablename
        }
        override def invoke(value: (String, String, String)): Unit = {
          val put = new Put(value._1.getBytes()) //rowkey
          put.addColumn("info".getBytes,"id".getBytes,value._1.getBytes()) // 列族，列名，值
          put.addColumn("info".getBytes,"orderprice".getBytes,value._2.getBytes())
          put.addColumn("info".getBytes,"orderdetailcount".getBytes,value._3.getBytes())
          HBtable.put(put)
        }
        override def close(): Unit = {
          HBtable.close()
          conn.close()
        }
      })
    env.execute("demo7")
  }
}
