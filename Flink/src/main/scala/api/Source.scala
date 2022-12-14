package api

import java.util.Properties

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import scala.util.Random

//定义样例类（传感器）
case class SensorReading(id:String,timestamp:Long,temperature:Double)
object Source {
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val env=StreamExecutionEnvironment.getExecutionEnvironment

    //1、从集合读取
    val dataList=List(
      SensorReading("sensor_1", 1547718199, 35.8),
      SensorReading("sensor_6", 1547718201, 15.4),
      SensorReading("sensor_7", 1547718202, 6.7),
      SensorReading("sensor_10", 1547718205, 38.1)
    )
    val stream1=env.fromCollection(dataList)
    //env.fromElements():随便数据类型
    stream1.print()

    //2、从文件读取
    val inputPath="H:\\Scala程序\\Flink\\src\\main\\resources\\source.txt"
    val stream2=env.readTextFile(inputPath)
    stream2.print()

    //3、从kafka读取
    val properties=new Properties()
    properties.setProperty("bootstrap.servers","hadoop101:9092")
    properties.setProperty("group.id","consumer-group")
    val stream3=env.addSource(new FlinkKafkaConsumer011[String]("sensor",new SimpleStringSchema(),properties))
    stream3.print()

    //4、自定义Source
    val stream4=env.addSource( new MySensorSource())
    stream4.print()
    //执行
    env.execute("source")
  }

}
//自定义MySensor
class MySensorSource() extends SourceFunction[SensorReading]{
  //定义一个标志位flag，用来表示数据源是否正常运行发出数据
  var running=true
  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    //定义一个随机数发生器
    val rand=new Random()
    //随机生成一组传感器的初始温度(id,temp)
    var curTemp=1.to(10).map(i=>("sensor_"+i,rand.nextDouble()*100))
    //定义无限循环，不停产生数据，除非被cancel
    while (running){
      //在上次温度基础上微调，更新温度
      curTemp=curTemp.map(
        data=>(data._1,data._2+rand.nextGaussian())
      )
      //获取当前时间戳，加入到数据中,调用collect发出数据
      val curTime=System.currentTimeMillis()
      curTemp.foreach(
        data=> sourceContext.collect(SensorReading(data._1,curTime,data._2))
      )
      //间隔100ms
      Thread.sleep(100)
    }
  }

  override def cancel(): Unit = running=false
}
//自定義一個函數類
class MyFilter extends FilterFunction[SensorReading] {
  override def filter(t: SensorReading): Boolean = {
    t.id.startsWith("sensor_1")
  }
}
