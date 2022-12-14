package api

import org.apache.flink.api.common.functions.{ReduceFunction, RichMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

/**
 *
 * @PROJECT_NAME: Flink
 * @PACKAGE_NAME: api
 * @author: 赵嘉盟-HONOR
 * @data: 2022-08-19 13:31
 * @DESCRIPTION
 *
 */
object Transform {
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
      // .filter(new MyFilter)   //自定義函數
    //分组聚合，输出最小值
    val aggStream=dataStream
      //.keyBy(1)
      .keyBy("id")
      //.min("temperature")           //只有求的字段的最小值，前面的随第一个输入
      //.minBy("temperature")          //最小值的全部字段
      //.reduce(MyReduceFunction)        //最新的时间和最小的温度
      .reduce((curState,newData)=>
      SensorReading(curState.id,newData.timestamp,curState.temperature.min(newData.temperature)))
    //aggStream.print()
    //多流转换（分流）
    val splitStream=dataStream
      .split(data=>{
        if(data.temperature>30.0) Seq("high") else Seq("low")
      })
    val highTempStream=splitStream.select("high")
    val lowTempStream=splitStream.select("low")
    val allTempStream=splitStream.select("high","low")

    //highTempStream.print("high")

    //合流,connect
    val warningStream=highTempStream.map(data=>(data.id,data.temperature))
    val connectedStreams=warningStream.connect(lowTempStream)
    //用coMap对数据进行分别处理
    val coMapResultStream=connectedStreams
      .map(
        warningData=>(warningData._1,warningData._2,"warning"),
        lowTempData=>(lowTempData.id,"healthy")
      )
    //coMapResultStream.print("coMap")

    //union
    val unionStream=highTempStream.union(lowTempStream)
    env.execute("transform")
  }
}
class MyReduceFunction extends ReduceFunction[SensorReading] {
  override def reduce(t: SensorReading, t1: SensorReading): SensorReading =
    SensorReading(t.id,t1.timestamp,t.temperature.min(t1.temperature))
}
//富函数：可以获取到运行时的上下文，还有一些生命周期
class MyRichMapper extends  RichMapFunction[SensorReading,String] {
  override def open(parameters: Configuration): Unit = {
    //做一些初始化操作，比如数据库连接
    //getRuntimeContext
  }
  override def map(in: SensorReading): String = in.id+" temperature"

  override def close(): Unit = {
    //做一些收尾工作，比如关闭连接，或者清空状态
  }
}