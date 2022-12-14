package api.sink

import api.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
 *
 * @PROJECT_NAME: Flink
 * @PACKAGE_NAME: api.sink
 * @author: 赵嘉盟-HONOR
 * @data: 2022-10-15 21:45
 * @DESCRIPTION
 *
 */
object RedisSink {
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

      //定义一个FlinkJedisConfigBase
      val conf=new FlinkJedisPoolConfig.Builder()
              .setHost("192.168.174.200")
              .setPort(6379)
              .build()
      dataStream.addSink(new RedisSink[SensorReading](conf,new MyRedisMapper))

      env.execute("redis sink")
  }

}
//定义一个RedisMapper
class MyRedisMapper extends  RedisMapper[SensorReading]{
  //定义保存数据写入redis的命令: HSET 表名 key value
  override def getCommandDescription: _root_.org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription =  new RedisCommandDescription(RedisCommand.HSET,"sensor_temp")
  //将id指定为key
  override def getKeyFromData(t: _root_.api.SensorReading): _root_.java.lang.String = t.id
  //将温度值指定为value
  override def getValueFromData(t: _root_.api.SensorReading): _root_.java.lang.String =t.temperature.toString


}