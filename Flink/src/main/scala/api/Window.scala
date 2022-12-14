package api

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingProcessingTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 *
 * @PROJECT_NAME: Flink
 * @PACKAGE_NAME: api
 * @author: 赵嘉盟-HONOR
 * @data: 2022-10-24 1:46
 * @DESCRIPTION
 *
 */
object Window {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
//    val inputPath="H:\\Scala程序\\Flink\\src\\main\\resources\\source.txt"
//    val stream=env.readTextFile(inputPath)
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)   //添加事件时间语义，不添加默认Processing time（操作时间）
    env.getConfig.setAutoWatermarkInterval(500)   //设置水平线生成周期
    val inputStream=env.socketTextStream("localhost",7777)
    //先转换样例类（简单转换操作）
    val dataStream=inputStream
      .map(data=>{
        val arr=data.split(",")
        SensorReading(arr(0),arr(1).toLong,arr(2).toDouble)
      })
      //.assignAscendingTimestamps(_.timestamp*1000l)    //升序数据提取时间戳方法，直接使用事件时间戳
      .assignTimestampsAndWatermarks( new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(3)) {   //最大乱序程度
        override def extractTimestamp(t: SensorReading): Long = t.timestamp*1000l   //提取时间戳
      })

    //每十五秒统计一下窗口内各个传感器温度的最小值,以及最新的时间戳
    val resultStream=dataStream
      .map( data=>(data.id,data.temperature,data.timestamp))
      .keyBy(_._1)
      //.window( TumblingEventTimeWindows.of(Time.days(1),Time.hours(-8)))   //滚动时间窗口
      //.window( SlidingProcessingTimeWindows.of(Time.seconds(15),Time.seconds(10)))  //滑动时间窗口
      //.window( EventTimeSessionWindows.withGap(Time.seconds(10)))   //绘画窗口
      .timeWindow(Time.seconds(15))   //简写方法
      .allowedLateness(Time.minutes(1)) //允许处理迟到数据
      .sideOutputLateData(new OutputTag[(String, Double, Long)]("late"))    //写进侧输出流
      //.minBy(1)
      .reduce(
        (curRes,newData)=>(curRes._1,curRes._2.min(newData._2),newData._3))

    resultStream.getSideOutput(new OutputTag[(String, Double, Long)]("late")).print("late")   //获取测输出流
    resultStream.print("resulta")
    env.execute("window")
  }
}
