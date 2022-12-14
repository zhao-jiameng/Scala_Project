package 案例实操

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import java.util.Properties
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random


object MockData {
  def main(args: Array[String]): Unit = {

    /**
     * 生成模拟数据,发送到Kafka
     * 模拟的数据
     * 格式 ：timestamp area city userid adid
     *       时间戳    地区   城市  用户   广告
     * Application=>Kafka=>SparkStreaming=>Analysis
     */
    // 创建配置对象
    val prop = new Properties()
    // 添加配置
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop101:9092")
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    // 根据配置创建 Kafka 生产者
    val producer = new KafkaProducer[String, String](prop)
    while (true){
      mockdata().foreach(
        data=>{
          //向Kafka中生成数据
          val record=new ProducerRecord[String,String]("zjmnew",data)
          producer.send(record)
        }
      )
      Thread.sleep(2000)
    }

  }
  def mockdata(): mutable.Seq[String] ={
    val list=ListBuffer[String]()
    val areaList=ListBuffer[String]("华北","华东","华南")
    val cityList=ListBuffer[String]("北京","太原","长治")
    for(_ <- 1 to new Random().nextInt(50)){
      val area=areaList(new Random().nextInt(3))
      val city=cityList(new Random().nextInt(3))
      val userid=new Random().nextInt(6)+1
      val adid=new Random().nextInt(6)+1
      list.append(s"${System.currentTimeMillis()} $area $city $userid $adid")
    }
    list
  }
}
