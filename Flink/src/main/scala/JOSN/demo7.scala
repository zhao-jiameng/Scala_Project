package JOSN

import java.util.Properties

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

object demo7 {
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
//    prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"master:9092")
//    prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"aaa")
//    prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest")
//    val inputStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("new_topic", new SimpleStringSchema(), prop))
    val inputStream: DataStream[String] = env.readTextFile("src\\main\\scala\\new_ShanxiProv\\2.json")
     inputStream.map(json => {
       val data: String = JSON.parseObject(json).getString("data")
       JSON.parseObject(data).getString("time")
        }).print()


    env.execute()
  }
}
