package api.sink

import api.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._

/**
 *
 * @PROJECT_NAME: Flink
 * @PACKAGE_NAME: api.sink
 * @author: 赵嘉盟-HONOR
 * @data: 2022-08-22 17:09
 * @DESCRIPTION
 *
 */
object FileSink {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    val inputPath="H:\\Scala程序\\Flink\\src\\main\\resources\\source.txt"
    val stream=env.readTextFile(inputPath)
    //先转换样例类（简单转换操作）
    val dataStream=stream
      .map(data=>{
        val arr=data.split(",")
        SensorReading(arr(0),arr(1).toLong,arr(2).toDouble)
      })

    dataStream.writeAsText("H:\\Scala程序\\Flink\\src\\main\\resources\\sourceout.txt")
    dataStream.addSink(
      StreamingFileSink.forRowFormat(
        new Path("H:\\Scala程序\\Flink\\src\\main\\resources\\sourceout.txt"),
        new SimpleStringEncoder[SensorReading]()
      ).build()
    )

    env.execute("file sink")
  }

}
