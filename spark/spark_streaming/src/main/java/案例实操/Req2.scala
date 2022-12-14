package 案例实操

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import 案例实操.util.JDBCUtil

import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable.ListBuffer

object Req2 {
  def main(args: Array[String]): Unit = {
    //TODO 创建环境对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("kafak")
    val ssc=new StreamingContext(sparkConf,Seconds(3))
    //TODO 逻辑处理
    //定义 Kafka 参数
    val kafkaPara: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG ->
        "hadoop101:9092,hadoop102:9092,hadoop103:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "zjm",    //消费者组
      "key.deserializer" ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" ->
        "org.apache.kafka.common.serialization.StringDeserializer"
    )
    //读取 Kafka 数据创建 DStream
    val kafkaDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](ssc,
      LocationStrategies.PreferConsistent, //生产者策略
      ConsumerStrategies.Subscribe[String, String](Set("zjmnew"), kafkaPara)) //消费者策略| topic |config
    //将每条消息的 KV 取出
    val adClickData= kafkaDS.map (
      kafkaData=>{
        val data=kafkaData.value()
        val datas=data.split(" ")
        AdClickData(datas(0),datas(1),datas(2),datas(3),datas(4))
      })
    val reduceDS=adClickData.map(
      data=>{
        val sdf=new SimpleDateFormat("yyyy-MM-dd")
        val day=sdf.format(new Date(data.ts.toLong))
        ((day,data.area,data.city,data.ad),1)
      }
    ).reduceByKey(_+_)
    reduceDS.foreachRDD(
      rdd=>{
        rdd.foreachPartition(
          iter=>{
            val conn=JDBCUtil.getConnection
            val sql=conn.prepareStatement(
              """
                |insert into area_city_ad_count(dt,area,city,adid,count)
                |values(?,?,?,?,?)
                |on duplicate key update count=count + ?
                |""".stripMargin)
            iter.foreach{
              case ((day,area,city,ad),sum)=>
                sql.setString(1,day)
                sql.setString(2,area)
                sql.setString(3,city)
                sql.setString(4,ad)
                sql.setInt(5,sum)
                sql.setInt(6,sum)
                sql.executeUpdate()
            }
            sql.close()
            conn.close()
          }
        )
      }
    )
    //TODO 开始任务
    ssc.start()
    ssc.awaitTermination()
  }
  //广告点击数据
  case class AdClickData(ts:String,area:String,city:String,user:String,ad:String)
}
