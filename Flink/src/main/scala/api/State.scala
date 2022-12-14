package api

import java.util
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.StateTtlConfig.RocksdbCompactFilterCleanupStrategy
import org.apache.flink.api.common.state.{ListStateDescriptor, MapStateDescriptor, ReducingStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 *
 * @PROJECT_NAME: Flink
 * @PACKAGE_NAME: api
 * @author: 赵嘉盟-HONOR
 * @data: 2022-10-24 21:06
 * @DESCRIPTION
 *
 */
object State {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //状态后端配置
    //env.setStateBackend( new MemoryStateBackend())         //内存状态后端
    //env.setStateBackend( new FsStateBackend("保存地址",异步快照))    //文件状态后端
    //env.setStateBackend ( new RocksDBStateBackend())

    //检查点配置
    env.enableCheckpointing(1000L)   //启用检查点
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)    //配置检查点算法
    env.getCheckpointConfig.setCheckpointTimeout(60000L)     //设置超时时间
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(5)  //最多允许几个检查点同时工作
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500) //两个检查点最小间隔（与max冲突）
    env.getCheckpointConfig.setPreferCheckpointForRecovery(true)    //是否只用检查点（默认false）
    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(3)  //最大失败次数

    //重启策略配置
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,10000L))   //重启三次，每次间隔10s
    env.setRestartStrategy(RestartStrategies.failureRateRestart(5,Time.of(5,TimeUnit.MINUTES),Time.of(10,TimeUnit.SECONDS)))//五分钟内重启五次，每次间隔10s

    val inputStream=env.socketTextStream("localhost",7777)
    val dataStream=inputStream
      .map( data=>{
        val arr=data.split(",")
        SensorReading(arr(0),arr(1).toLong,arr(2).toDouble)
      })


    //需求：对于温度传感器温度值跳变，超过十度报警
    val alertState=dataStream
      .keyBy(_.id)
      //.flatMap( new TempChangeAlert(10.0))
      .flatMapWithState[(String,Double,Double),Double]{
        case (data:SensorReading,None) =>( List.empty ,Some(data.temperature))    //第一次没有状态的初始化
        case (data,lastTemp:Some[Double])=>{
          val diff=(data.temperature-lastTemp.get).abs
          if(diff>10.0)
            (List((data.id,lastTemp.get,data.temperature)),Some(data.temperature))  //超过阈值报警
          else
            (List.empty,Some(data.temperature))                                     //没有超过阈值更新状态
        }
      }
    alertState.print()

    env.execute("state")
  }
}






//实现自定义RichFlatMapFunction
class TempChangeAlert(threshold :Double) extends RichFlatMapFunction[SensorReading,(String,Double,Double)]{
  //定义状态保存上一次的温度值
  lazy val lastTempState=getRuntimeContext.getState( new ValueStateDescriptor[Double]("last_temp",classOf[Double]))
  var first = false
  override def flatMap(in: SensorReading, collector: Collector[(String, Double, Double)]): Unit = {
    //判断是否第一次运行
    if(first==false){
      first=true
      lastTempState.update(in.temperature)
    }
    //获取上一次的温度值
    val lastTemp=lastTempState.value()
    //跟最新的温度值作比较
    val diff=(in.temperature-lastTemp).abs

    if (diff > threshold){
      collector.collect((in.id,lastTemp,in.temperature))
    }
    //更新状态
    lastTempState.update(in.temperature)

  }
}




//keyed satte 测试：必须定义在RichFunction中，因为需要运行时上下文
class MyRichMapper1 extends  RichMapFunction[SensorReading,String]{
  var valueState:ValueState[Double]=_
  lazy val listState=getRuntimeContext.getListState( new ListStateDescriptor[Int]("list",classOf[Int])) //listState简易定义方法
  lazy val mapState=getRuntimeContext.getMapState(new MapStateDescriptor[String,Double]("map",classOf[String],classOf[Double]))
  lazy val reduceState=getRuntimeContext.getReducingState(new ReducingStateDescriptor[SensorReading]("reduce",new MyReduceFunction,classOf[SensorReading]))

  override def open(parameters: Configuration): Unit = {
    valueState=getRuntimeContext.getState( new ValueStateDescriptor[Double]("value",classOf[Double]))
  }

  override def map(in: SensorReading): String = {
    //状态的读写
    val myV=valueState.value()  //读取
    valueState.update(in.temperature)      //更改

    //listState方法
    listState.add(2)
    val list = new util.ArrayList[Int]()
    list.add(3)
    list.add(4)
    listState.addAll(list)  //追加
    listState.update(list)  //覆盖
    listState.get()         //获取

    //map方法
    mapState.contains("sensor_1") //判断存在
    mapState.get("sensor_1")      //获取value
    mapState.put("sensor_1",1.2)  //修改value

    //reduce方法
    reduceState.get() //聚合后的值
    reduceState.add(in) //聚合新输入的值
    in.id
  }
}