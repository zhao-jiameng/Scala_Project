import java.util.Properties

import com.alibaba.fastjson.JSON
import demo1.order_info
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
 *
 * @PROJECT_NAME: BigDataSAISAISAI
 * @PACKAGE_NAME:
 * @author: 赵嘉盟-HONOR
 * @data: 2023-02-15 15:23
 * @DESCRIPTION
 *
 */
object josn {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val qv_order=new OutputTag[order_info]("qx")
    val th_order=new OutputTag[order_info]("th")
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","192.168.23.51:9092")

    val inputStream = env.readTextFile("src/main/resources/response.json")
    inputStream.map(data=>{
      val datas = JSON.parseObject(data).getString("data")
      val datas2 = datas.split("},")
      //val datas3=datas2+"}"
      while (true){
        var i=0
        datas2(i)+"}"
        i+=1
      }
    }).print()
    env.execute("job3")
  }
}
