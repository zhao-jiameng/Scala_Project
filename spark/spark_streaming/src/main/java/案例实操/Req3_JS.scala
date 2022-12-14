package 案例实操

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.io.{File, FileWriter, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable.ListBuffer

object Req3_JS {
  def main(args: Array[String]): Unit = {
    //TODO 创建环境对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("kafak")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    //TODO 逻辑处理
    //定义 Kafka 参数
    val kafkaPara: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG ->
        "hadoop101:9092,hadoop102:9092,hadoop103:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "zjm", //消费者组
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
    val adClickData = kafkaDS.map(
      kafkaData => {
        val data = kafkaData.value()
        val datas = data.split(" ")
        AdClickData(datas(0), datas(1), datas(2), datas(3), datas(4))
      })
    //最近一分钟，十秒计算一次
    val reduceDS = adClickData.map(
      data => {
        val ts = data.ts.toLong
        val newTS = ts / 10000 * 10000
        (newTS, 1)
      }).reduceByKeyAndWindow((x: Int, y: Int) => {
      x + y
    }, Seconds(60), Seconds(10))
    //输出到JS文件
    reduceDS.foreachRDD(
      rdd => {
        val list = ListBuffer[String]()
        val datas = rdd.sortByKey(true).collect()
        datas.foreach {
          case (time, cnt) =>
            val timeString = new SimpleDateFormat("mm:ss").format(new Date(time.toLong))
            list.append(s""" { "xtime":"$timeString", "yval":"$cnt" }""")
        }
        //输出文件
        val out = new PrintWriter(new FileWriter(new File("H:\\Scala程序\\spark\\datas\\adclick\\adclick.json")))
        out.println("[" + list.mkString(",") + "]")
        out.flush()
        out.close()

      }
    )
    //TODO 开始任务
    ssc.start()
    ssc.awaitTermination()
  }

  //广告点击数据
  case class AdClickData(ts: String, area: String, city: String, user: String, ad: String)
}
