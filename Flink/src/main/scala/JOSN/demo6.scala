package JOSN

import java.util.Properties

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.kafka.clients.consumer.ConsumerConfig



object demo6 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    env.setParallelism(1)
    //设置时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //?
    env.setRestartStrategy(RestartStrategies.noRestart())

//    kafka的相关配置
  val prop = new Properties()
  prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"master:9092")
  prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"aaa")
  prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest")
    val inputStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("new_topic", new SimpleStringSchema(), prop))
//val inputStream: DataStream[String] = env.readTextFile("src\\main\\scala\\new_ShanxiProv\\1.json")
inputStream.map(json => {
  val str1: String = JSON.parseObject(json).getString("province")
  val str2: String = JSON.parseObject(json).getString("totalprice")
  (str1,str2)
}).print()

    env.execute()
  }
}