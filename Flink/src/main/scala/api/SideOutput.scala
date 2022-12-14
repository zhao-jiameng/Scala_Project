package api

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 *
 * @PROJECT_NAME: Flink
 * @PACKAGE_NAME: api
 * @author: 赵嘉盟-HONOR
 * @data: 2022-10-25 21:24
 * @DESCRIPTION
 *
 */
object SideOutput {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    val inputStream=env.socketTextStream("localhost",7777)
    val dataStream=inputStream
      .map( data=>{
        val arr=data.split(",")
        SensorReading(arr(0),arr(1).toLong,arr(2).toDouble)
      })
    val highTempStream=dataStream
      .process( new SplitTempProcessor(30.0))

    highTempStream.print("high")
    highTempStream.getSideOutput(new OutputTag[(String,Long,Double)]("low")).print("low")

    env.execute("side output")
  }

}

//实现自定义ProcessFunction，实现分流
class SplitTempProcessor(threshold:Double)  extends ProcessFunction[SensorReading,SensorReading] {
  override def processElement(i: SensorReading, context: ProcessFunction[SensorReading, SensorReading]#Context, collector: Collector[SensorReading]): Unit = {
    //判断条件
    if (i.temperature > threshold) {
      //主输出
      collector.collect(i)
    } else {
      //侧输出流
      context.output(new OutputTag[(String, Long, Double)]("low"), (i.id, i.timestamp, i.temperature))
    }
  }
}
