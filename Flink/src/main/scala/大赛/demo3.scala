package 大赛

import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer
case class user_info(id:String,creatTime:String,operateTime:String,status:String,money:Double,name:String)
object demo3 {
  def main(args: Array[String]): Unit = {
    //设置执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    env.setParallelism(1)
    //设置事件时间的类型
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //通过文件读取数据
    val inputStream: DataStream[String] = env.readTextFile("src\\main\\resources\\reader.txt")

    //标记两个标签,用来测输出流
    //标记 取消订单 状态为1003
    val cancelrate = new OutputTag[user_info]("cancelrate")
    //标记 不考虑订单状态
    val top3itemconsumption = new OutputTag[user_info]("top3itemconsumption")

    //处理时间
    val mapStream: DataStream[user_info] = inputStream.map(data => {
      val arr: Array[String] = data.split(",")
      user_info(arr(5), arr(10), arr(11), arr(4),arr(3).toDouble,arr(1))
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[user_info](Time.seconds(5)) {
      override def extractTimestamp(t: user_info) = {
        val creat_time = strToLongTime(t.creatTime)
        val operateTime: Long = strToLongTime(t.operateTime)
        if (creat_time > operateTime)
          creat_time
        else
          operateTime
      }
    })

    //测输出流(两条)

    val tagStream: DataStream[user_info] = mapStream.process(new ProcessFunction[user_info, user_info] {
      override def processElement(i: user_info, context: ProcessFunction[user_info, user_info]#Context, collector: Collector[user_info]) = {
        if ("1003".equals(i.status)) {
          context.output(cancelrate, i)
        }
        context.output(top3itemconsumption, i)
        //返回所有结果
        collector.collect(i)
      }
    })
    //TODO:1使用Flink消费Kafka中的数据，实时统计商城中消费额前2的用户
    // （需要考虑订单状态，若有取消订单、申请退回、退回完成则不计入订单消费额，其他的相加），
    // 将key设置成top2userconsumption存入Redis中（value使用String数据格式，value
    // 为前2的用户信息并且外层用[]包裹，其中按排序依次存放为该用户id:用户名称:消费总额，用逗号分割，
    // 其中用户名称为user_info表中的name字段，可从MySQL中获取）。
    // 使用redis cli以get key方式获取top2userconsumption值，将结果截图粘贴至对应报告中，
    // 需两次截图，第一次截图和第二次截图间隔1分钟以上，第一次截图放前面，第二次截图放后面（如有中文，需在redis-cli中展示中文）；
    // 示例如下：
    // top2userconsumption：[1:张三:10020,42:李四:4540]
    //主流
    var info=""
    var id1 = ""
    var id2 = ""
    var money1 = ""
    var money2 = ""
    var name1 = ""
    var name2 = ""
    var tag = 0
    val dataStream: DataStream[(String, Double, String)] = tagStream.filter(data => {
      data.status != "1003" && data.status != "1005" && data.status != "1006"
    }).map(data => {
      (data.id, data.money, data.name)
    })
    val listBuffer = new ListBuffer[(String, Double, String)]
    val resultMap: DataStream[String] = dataStream.process(new ProcessFunction[(String, Double, String), String] {
      override def processElement(i: (String, Double, String), context: ProcessFunction[(String, Double, String), String]#Context, collector: Collector[String]) = {
        listBuffer.append((i._1, i._2, i._3))
        //打印输出 添加buffer桶的元素
//                println(listBuffer)
        val top2: ListBuffer[(String, Double, String)] = listBuffer.sortBy(_._2).reverse.take(2)
        //按照价格排序
//                print(top2)
        top2.foreach(a => {
          if (tag== 0) {
            id1 = a._1
            name1 = a._3
            money1 = a._2.toString
            tag += 1
          } else {
            id2 = a._1
            name2 = a._3
            money2 = a._2.toString
            tag -= 1
          }
        })
        val info = s"[${id2}:${name2}:${money2},${id1}:${name1}:${money1}]"
        collector.collect(info)
      }
    })

    val builder = new FlinkJedisPoolConfig.Builder().setHost("192.168.174.200").setPort(6379).build()
    resultMap.addSink(new RedisSink[String](builder,new RedisMapper[String] {
      override def getCommandDescription = new RedisCommandDescription(RedisCommand.SET)

      override def getKeyFromData(data: String) = "top2consumption"

      override def getValueFromData(data: String) = data
    }))
    //TODO:2、在任务1进行的同时，使用侧边流，计算每分钟内状态为取消订单占所有订单的占比，
    // 将key设置成cancelrate存入Redis中，value存放取消订单的占比（为百分比，保留百分比后的一位小数，四舍五入，例如12.1%）
    // 。使用redis cli以get key方式获取cancelrate值，将结果截图粘贴至对应报告中，需两次截图，
    // 第一次截图和第二次截图间隔1分钟以上，第一次截图放前面，第二次截图放后面；

    //测输出流
      var count=0
      var size=0.0
    val result2: DataStream[Double] = tagStream.timeWindowAll(Time.minutes(1)).process(new ProcessAllWindowFunction[user_info, Double, TimeWindow] {
      override def process(context: Context, elements: Iterable[user_info], out: Collector[Double]): Unit = {
        elements.foreach(data => {
          if ("1003".equals(data.status)) {
            count = count + 1
          }
        })
        size+=elements.toList.size
        val str = (count / size).formatted("%.3f").toDouble * 100
        out.collect(str)
      }
    })

    result2.addSink(new RedisSink[Double](builder,new RedisMapper[Double] {
      override def getCommandDescription = new RedisCommandDescription(RedisCommand.SET)

      override def getKeyFromData(data: Double)  = "cancelrate_2"

      override def getValueFromData(data: Double) =  data+"%"
    }))

    env.execute()

  }
  def strToLongTime(date:String) :Long={
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    format.parse(date).getTime
  }
}
