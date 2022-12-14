package api.sink

import api.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

/**
 *
 * @PROJECT_NAME: Flink
 * @PACKAGE_NAME: api.sink
 * @author: 赵嘉盟-HONOR
 * @data: 2022-08-22 19:14
 * @DESCRIPTION
 *
 */
object KafkaSink {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    val inputPath="H:\\Scala程序\\Flink\\src\\main\\resources\\source.txt"
    val stream=env.readTextFile(inputPath)
    //先转换样例类（简单转换操作）
    val dataStream=stream
      .map(data=>{
        val arr=data.split(",")
        SensorReading(arr(0),arr(1).toLong,arr(2).toDouble).toString
      })
    dataStream.addSink(new FlinkKafkaProducer011[String]("master:9092","sinktest",new SimpleStringSchema()) )
    env.execute("kafka sink")
  }

}
