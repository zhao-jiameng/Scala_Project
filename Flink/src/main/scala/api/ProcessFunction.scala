package api


import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
/**
 *
 * @PROJECT_NAME: Flink
 * @PACKAGE_NAME: api
 * @author: 赵嘉盟-HONOR
 * @data: 2022-10-25 1:52
 * @DESCRIPTION
 *
 */
object ProcessFunction {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    val inputStream=env.socketTextStream("localhost",7777)
    val dataStream=inputStream
      .map( data=>{
        val arr=data.split(",")
        SensorReading(arr(0),arr(1).toLong,arr(2).toDouble)
      })
//      .keyBy(_.id)
//      .process( new MyKeyedProcessFunction)

    //连续十秒温度上升报警
      val warningStream=dataStream
        .keyBy(_.id)
        .process( new TempIncreWarning(10000L))
    warningStream.print()
    env.execute("processFunction")
  }


}
//连续十秒温度上升报警
class TempIncreWarning(interval:Long) extends  KeyedProcessFunction[String,SensorReading,String]{
  //定义状态:保存上一个温度值，保存定时器注册时间戳
  lazy val lastTempState=getRuntimeContext.getState(new ValueStateDescriptor[Double]("last_temp",classOf[Double]))
  lazy val timerTsState=getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer_ts",classOf[Long]))

  override def processElement(i: SensorReading, context: KeyedProcessFunction[String, SensorReading, String]#Context, collector: Collector[String]): Unit = {
    //先取出状态
    val lastTemp=lastTempState.value()
    val timerTs=timerTsState.value()
    //更新温度值
    lastTempState.update(i.temperature)
    //判断当前温度值和上次温度进行比较
    if(i.temperature>lastTemp && timerTs==0L){
      //如果温度上升，且没有定时器，那么注册当前时间10s后的定时器
      val ts=context.timerService().currentProcessingTime()+interval
      context.timerService().registerProcessingTimeTimer(ts)
      timerTsState.update(ts)
    }else if(i.temperature<lastTemp){
      //如果温度下降，删除定时器
      context.timerService().deleteProcessingTimeTimer(timerTs)
      timerTsState.clear()
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
      out.collect("传感器"+ctx.getCurrentKey+"的温度连续"+interval/1000+"秒连续上升")
      timerTsState.clear()
  }
}


//keyedProcessFunction功能测试
class MyKeyedProcessFunction extends KeyedProcessFunction[String,SensorReading,String]{
  override def processElement(i: SensorReading, context: KeyedProcessFunction[String, SensorReading, String]#Context, collector: Collector[String]): Unit = {
    context.getCurrentKey   //获取key
    context.timestamp()     //获取数据时间戳
    context.timerService().currentWatermark() //获取watermark
    context.timerService().registerEventTimeTimer(context.timestamp()+60000L) //注册定时器
    context.timerService().deleteEventTimeTimer(context.timestamp())          //删除定时器
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit ={}
    //定义定时器操作
}
