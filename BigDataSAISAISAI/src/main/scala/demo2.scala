import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
 *
 * @PROJECT_NAME: BigDataSAISAISAI
 * @PACKAGE_NAME:
 * @author: 赵嘉盟-HONOR
 * @data: 2023-02-14 13:25
 * @DESCRIPTION
 *
 */
object demo2 {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","192.168.23.51:9092")
    val data = env.addSource(new FlinkKafkaConsumer011[String]("order", new SimpleStringSchema(), properties))
    data.print()
    env.execute("job2")
  }
}
