package 大赛

import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 *
 * @PROJECT_NAME: Flink
 * @PACKAGE_NAME: 大赛
 * @author: 赵嘉盟-HONOR
 * @data: 2022-11-09 16:08
 * @DESCRIPTION
 *
 */
object demo1 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputStream = env.readTextFile("src/main/resources/reader.txt")
    val timeStream = inputStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[String](Time.milliseconds(5000)) {
      override def extractTimestamp(t: String): Long = {
        val datas = t.split(",")
        val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val create_time = format.parse(datas(10)).getTime
        var operate_time = format.parse(datas(11)).getTime
        if (operate_time isNaN) operate_time = create_time
        val max = if (create_time > operate_time) create_time else operate_time
        max
      }
      })
    val outputStream = new OutputTag[String]("tag")

    val resultStream = timeStream.process(new ProcessFunction[String, String] {
      override def processElement(i: String, context: ProcessFunction[String, String]#Context, collector: Collector[String]): Unit = {
        val arr = i.split(",")
        if (arr(4) == "1003") {
          context.output(outputStream, i)
        }
        collector.collect(i)

      }
    })
    //resultStream.filter()
    resultStream.getSideOutput(outputStream).timeWindowAll(Time.minutes(1)).process(new ProcessAllWindowFunction[String,String,TimeWindow] {
      override def process(context: Context, elements: Iterable[String], out: Collector[String]): Unit = {

      }
    })
    env.execute()
  }
}
