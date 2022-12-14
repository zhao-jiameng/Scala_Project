package 大赛

import java.text.SimpleDateFormat
import java.util.Date

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
case class OrderInfo(id:String,creatTime:String,operateTime:String,status:String)
object demo2 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val inputStream: DataStream[String] = env.readTextFile("src\\main\\resources\\reader.txt")

    //设置两个分流标签
    // refund toDO 代表申请退回订单(1005)
    // cancelrate toDO 代表取消订单(1003)
    val refund = new OutputTag[OrderInfo]("refund")
    val cancelrate = new OutputTag[OrderInfo]("cancelrate")

    //统计每分钟申请的订单数量 首先对时间进行处理
    //TODO:create_time 或 operate_time 取两者中值较大者作为EventTime，若operate_time为空值或无此属性，则使用create_time填充
    val mapStream: DataStream[OrderInfo] = inputStream.map(data => {
      val arr: Array[String] = data.split(",")
      OrderInfo(arr(0), arr(10), arr(11), arr(4))
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderInfo](Time.seconds(5)) {
      override def extractTimestamp(t: OrderInfo) = {
        //调用时间格式化函数
        val create_time = strToLong(t.creatTime)
        val operate_time = strToLong(t.operateTime)
        if (create_time > operate_time) create_time else operate_time
      }
    })

    //定义了两条测输出流,代表申请退回订单(1005),代表取消订单(1003)
    val tagStream: DataStream[OrderInfo] = mapStream.process(new ProcessFunction[OrderInfo, OrderInfo] {
      override def processElement(i: OrderInfo, context: ProcessFunction[OrderInfo, OrderInfo]#Context, collector: Collector[OrderInfo]) = {
        if ("1005".equals(i.status)) context.output(refund, i) else if ("1003".equals(i.status)) context.output(cancelrate, i)
        collector.collect(i)
      }
    })

    //主流
    //TODO:1、使用Flink消费Kafka中的数据，统计商城实时订单数量（需要考虑订单状态，若有取消订单、申请退回、退回完
    // 成则不计入订单数量，其他状态则累加），将key设置成totalcount存入Redis中。使用redis cli以get key方式获取
    // totalcount值，将结果截图粘贴至对应报告中，需两次截图，第一次截图和第二次截图间隔1分钟以上，第一次截图放前面，
    // 第二次截图放后面；

/*
    val resultMap: DataStream[] = tagStream.filter(data => {
    val arr: Array[String] = data.split(",")
    arr(4) != "1003" && arr(4) != "1005" && arr(4) != "1006"
    }).map(data => {
      ("totalcount", 1)
    }).keyBy(_._1).sum(1)

    */
    val resultMap: DataStream[(String, Int)] = tagStream.filter(data => {
      data.status != "1003" && data.status != "1005" && data.status != "1006"
    }).map(data =>
      ("totalcount", 1)
    ).keyBy(_._1).sum(1)

    val builder=new FlinkJedisPoolConfig.Builder().setHost("192.168.25.102").setPort(6379).build()
    //存放到redis中
    resultMap.addSink(new RedisSink[(String, Int)](builder,new RedisMapper[(String, Int)] {
      override def getCommandDescription = new RedisCommandDescription(RedisCommand.SET)

      override def getKeyFromData(data: (String, Int))= data._1

      override def getValueFromData(data: (String, Int)) = data._2.toString
  }))

    //todo:2、在任务1进行的同时，使用侧边流，统计每分钟申请退回订单的数量，
    // 将key设置成refundcountminute存入Redis中。使用redis cli以get key
    // 方式获取refundcountminute值，将结果截图粘贴至对应报告中，需两次截图，
    // 第一次截图和第二次截图间隔1分钟以上，第一次截图放前面，第二次截图放后面；
    val result2: DataStream[(Int, String, String, Int)] = tagStream.getSideOutput(refund)
      .timeWindowAll(Time.minutes(1))
      .process(new ProcessAllWindowFunction[OrderInfo, (Int, String, String, Int), TimeWindow] {
        override def process(context: Context, elements: Iterable[OrderInfo], out: Collector[(Int, String, String, Int)]): Unit = {
          val list: List[OrderInfo] = elements.toList
          out.collect(list.size, longToStr(context.window.getStart), longToStr(context.window.getEnd), 1)
        }
      }).keyBy(_._4).sum(0)
    result2.addSink(new RedisSink[(Int, String, String, Int)](builder,new RedisMapper[(Int, String, String, Int)] {
      override def getCommandDescription = new RedisCommandDescription(RedisCommand.SET)

      override def getKeyFromData(data: (Int, String, String, Int)) = "refundcountminute"

      override def getValueFromData(data: (Int, String, String, Int)) = data._1.toString
    }))

    //todo:3、在任务1进行的同时，使用侧边流，计算每分钟内状态为取消订单占所有订单的占比，将key设置成cancelrate存入Redis中，
    // value存放取消订单的占比（为百分比，保留百分比后的一位小数，四舍五入，例如12.1%）。使用redis cli以get key方式获取cancelrate值，
    // 将结果截图粘贴至对应报告中，需两次截图，第一次截图和第二次截图间隔1分钟以上，第一次截图放前面，第二次截图放后面。
    //错误代码 todo:tagStream 是经过时间处理(mapStream)的 如果经过tagStream再打一个标签 则最终剩下的只有数据1003，无法进行订单总量计算
    //toDO:虽然 tagStream 和 mapStream 的效果相同，采取 侧边流处理
/*    tagStream.getSideOutput(cancelrate).timeWindowAll(Time.minutes(1)).process(new ProcessAllWindowFunction[orderInfo,(Double,Int),TimeWindow] {
      override def process(context: Context, elements: Iterable[orderInfo], out: Collector[(Double, Int)]): Unit = {
        println(elements)
      }
    })*/

    var count = 0
    var list = 0.0
    val result3: DataStream[Double] = tagStream.timeWindowAll(Time.minutes(1)).process(new ProcessAllWindowFunction[OrderInfo, (Double), TimeWindow] {
      override def process(context: Context, elements: Iterable[OrderInfo], out: Collector[(Double)]): Unit = {
        elements.foreach(data => {
          if ("1003".equals(data.status)) {
            count = count + 1
          }
        })
        list+=elements.toList.size
        val str = (count / list).formatted("%.3f").toDouble * 100
        out.collect(str)
      }
    })
//    result3.addSink(new RedisSink[Double](builder,new RedisMapper[Double] {
//      override def getCommandDescription = new RedisCommandDescription(RedisCommand.SET)
//
//      override def getKeyFromData(data: Double) = "cancelrate"
//
//      override def getValueFromData(data: Double) = data+"%"
//    }))

    env.execute()
}
    def strToLong(date:String):Long={
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    format.parse(date).getTime
    }

    def longToStr(date:Long) :String={
      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      format.format(new Date(date))
    }
}
