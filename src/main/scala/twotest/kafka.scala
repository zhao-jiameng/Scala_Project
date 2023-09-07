package twotest
import java.lang.{Runnable, Thread}
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
/**
 *
 * @PROJECT_NAME: BIgData
 * @PACKAGE_NAME: twotest
 * @author: 赵嘉盟-HONOR
 * @data: 2023-08-28 14:42
 * @DESCRIPTION
 *
 */
object kafka extends Runnable {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "192.168.23.60:9092")
    val changeStream = env.addSource(new FlinkKafkaConsumer[String]("ChangeRecord", new SimpleStringSchema(), properties)).print()
    new Thread(kafka).start()
    env.execute("kafka")
  }

  override def run(): Unit = {
    var count=0
    while(true){
      print("程序运行"+count+"分钟\n")
      Thread.sleep(1000)
      count+=1
    }


  }
}
