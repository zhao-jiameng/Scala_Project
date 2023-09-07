package onetest

import java.nio.charset.StandardCharsets
import java.sql.{DriverManager, PreparedStatement}
import java.util.Properties

import onetest.demo1.strToLong
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.util.Collector
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Table}

import scala.collection.mutable
/**
 *
 * @PROJECT_NAME: 国赛
 * @PACKAGE_NAME: onetest
 * @author: 赵嘉盟-HONOR
 * @data: 2023-06-03 20:58
 * @DESCRIPTION
 *
 */
object demo3 {
  case class order_info(id: String, status: String, create_time: String, option_time: String, sun: Double)
  case class order_info2(id: String, name: String, create_time: String, option_time: String)

  def main(args: Array[String]): Unit = {
    //TODO 准备环境：
    /**
     * 编写Scala代码，使用Flink消费Kafka中Topic为order的数据并进行相应的数据统计计算
     * （订单信息对应表结构order_info,订单详细信息对应表结构order_detail（来源类型和来源编号这两个字段不考虑，所以在实时数据中不会出现）
     * 同时计算中使用order_info或order_detail表中create_time或operate_time取两者中值较大者作为EventTime
     * 若operate_time为空值或无此列，则使用create_time填充，允许数据延迟5s
     * 订单状态order_status分别为1001:创建订单、1002:支付订单、1003:取消订单、1004:完成订单、1005:申请退回、1006:退回完成
     * 另外对于数据结果展示时，不要采用例如：1.9786518E7的科学计数法）。
     */
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","192.168.23.51:9092")
    //val inputStream = env.readTextFile("src/main/resources/order_info.txt")
    val inputStream=env.addSource(new FlinkKafkaConsumer[String]("order",new SimpleStringSchema(),properties))
    val timeStream = inputStream.map(data => {
      val datas = data.split(",")
      order_info(datas(0), datas(4), datas(10), datas(11), datas(3).toDouble)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[order_info](Time.seconds(5)) {
      override def extractTimestamp(t: order_info): Long = {
        var create_Time = strToLong(t.create_time)
        var option_Time = strToLong(t.option_time)
        if (option_Time isNaN) return create_Time
        if (create_Time >= option_Time) create_Time else option_Time
      }
    })
    //TODO 任务一：
    /**
     * 1、使用Flink消费Kafka中的数据，实时统计商城中消费额前2的用户（需要考虑订单状态，若有取消订单、申请退回、退回完成则不计入订单消费额，其他的相加）
     * 将key设置成top2userconsumption存入Redis中（value使用String数据格式，value为前2的用户信息并且外层用[]包裹
     * 其中按排序依次存放为该用户id:用户名称:消费总额，用逗号分割，其中用户名称为user_info表中的name字段，可从MySQL中获取）
     * 使用redis cli以get key方式获取top2userconsumption值，将结果截图粘贴至客户端桌面【Release\任务D提交结果.docx】中对应的任务序号下
     * 需两次截图，第一次截图和第二次截图间隔1分钟以上，第一次截图放前面，第二次截图放后面（如有中文，需在redis-cli中展示中文）；
     *  示例如下：
     *  top2userconsumption：[1:张三:10020,42:李四:4540]
     */
    val oneStream = timeStream.filter(data => data.status != "1003" && data.status != "1005" && data.status != "1006")
      .keyBy(_.id).sum(4) //.print()
    val mysqlStream = env.addSource(new MysqlSource()).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[order_info2](Time.seconds(5)) {
      override def extractTimestamp(t: order_info2): Long = {
        var create_Time = strToLong(t.create_time)
        var option_Time = strToLong(t.option_time)
        if (option_Time isNaN) return create_Time
        if (create_Time >= option_Time) create_Time else option_Time
      }
    })
    oneStream.print("one")
    mysqlStream.print("mysql")
    val map = mutable.Map((1, "a") -> 0.1)
    val conf = new FlinkJedisPoolConfig.Builder().setPort(6379).setHost("192.168.23.51").build()
    oneStream.join(mysqlStream)
      .where(_.id)
      .equalTo(_.id)
      .window(SlidingEventTimeWindows.of(Time.seconds(5),Time.seconds(1)))
      .apply((e1, e2) => {
        (e1.id, e2.name, e1.sun)
      }).map(data => {
      map += ((data._1.toInt, data._2) -> data._3)
      map.toList.sortBy(_._2).reverse.take(2)
    }) //.print("top2userconsumption")
      .addSink(new RedisSink[List[((Int, String), Double)]](conf, new RedisMapper[List[((Int, String), Double)]] {
        override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET)
        override def getKeyFromData(t: List[((Int, String), Double)]): String = "top2userconsumption"
        override def getValueFromData(t: List[((Int, String), Double)]): String = "[" + t(0)._1._1 + ":" + t(0)._1._2 + ":" + t(0)._2 + "," + t(1)._1._1 + ":" + t(1)._1._2 + ":" + t(1)._2 + "]"
      }))
    //TODO 任务二：
    /**
     * 2、在任务1进行的同时，使用侧边流，计算每分钟内状态为取消订单占所有订单的占比，将key设置成cancelrate存入Redis中
     * value存放取消订单的占比（为百分比，保留百分比后的一位小数，四舍五入，例如12.1%）
     * 使用redis cli以get key方式获取cancelrate值，将结果截图粘贴至客户端桌面【Release\任务D提交结果.docx】中对应的任务序号下
     * 需两次截图，第一次截图和第二次截图间隔1分钟以上，第一次截图放前面，第二次截图放后面；
     */
    var count = 0
    var list = 0.0
    val twoStream = timeStream.timeWindowAll(Time.seconds(1))
      .process(new ProcessAllWindowFunction[order_info, String, TimeWindow] {
        override def process(context: Context, elements: Iterable[order_info], out: Collector[String]): Unit = {
          elements.foreach(data => {
            if (data.status == "1003") count += 1
            list += 1
          })
          val str = (count / list * 100).formatted("%2.1f")
          out.collect(str)
        }
      })
    twoStream.print("two")
    twoStream.addSink(new RedisSink[String](conf, new RedisMapper[String] {
      override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET)
      override def getKeyFromData(t: String): String = "cancelrate"
      override def getValueFromData(t: String): String = t + "%"
    }))
    //TODO 任务三：
    /**
     * 3、在任务1进行的同时，使用侧边流，监控order_status字段为取消订单的数据,将数据存入HBase数据库(namespace)shtd_result的order_info表中
     * rowkey为id的值，然后在Linux的HBase shell命令行中查询列consignee，并查询出任意5条，将执行结果截图粘贴至客户端桌面【Release\模块D提交结果.docx】中对应的任务序号下。
     *  表空间为：shtd_result，表为order_info，列族为：info
     *  scan "shtd_result:order_info",{COLUMNS=>'info:consignee',FORMATTER=>'toString'}
     */
    inputStream.filter(data => data.split(",")(4) == "1003").addSink(new HBaseSinkFunction)
    env.execute("demo3")
  }

  class MysqlSource() extends SourceFunction[order_info2] {
    var conn: java.sql.Connection = _
    var select: PreparedStatement = _
    var running = true

    override def run(sourceContext: SourceFunction.SourceContext[order_info2]): Unit = {
      conn = DriverManager.getConnection("jdbc:mysql://192.168.23.51:3306/shtd_store?characterEncoding=UTF-8", "root", "root")
      //select = conn.prepareStatement("select id,name,create_time.isnull(operate_time,create_time) from user_info")
      select = conn.prepareStatement("select id,name,create_time,ifnull(operate_time,create_time) from user_info")
      val rs = select.executeQuery()
      while (rs.next()) {
        sourceContext.collect(order_info2(
          rs.getInt(1).toString,
          rs.getString(2),
          rs.getString(3),
          rs.getString(4)))
      }
    }

    override def cancel(): Unit = {
      select.close()
      conn.close()
      running = false
    }
  }

  class HBaseSinkFunction extends RichSinkFunction[String] {
    var connection: org.apache.hadoop.hbase.client.Connection = _
    var hbTable: Table = _

    override def open(parameters: Configuration): Unit = {
      val configuration = HBaseConfiguration.create()
      configuration.set("hbase.zookeeper.quorum", "192.168.23.51,192.168.23.60,192.168.23.69")
      connection = ConnectionFactory.createConnection(configuration)
      hbTable = connection.getTable(TableName.valueOf("shtd_result:order_info"))
    }

    override def invoke(value: String): Unit = {
      val datas = value.split(",")
      val put = new Put(datas(0).getBytes())
      put.addColumn("info".getBytes, "id".getBytes(), datas(0).getBytes())
      put.addColumn("info".getBytes, "consignee".getBytes(), datas(1).getBytes())
      put.addColumn("info".getBytes, "consignee_tel".getBytes(), datas(2).getBytes())
      put.addColumn("info".getBytes, "final_total_amount".getBytes(), datas(3).getBytes())
      put.addColumn("info".getBytes, "order_status".getBytes(), datas(4).getBytes())
      put.addColumn("info".getBytes, "user_id".getBytes(), datas(5).getBytes())
      put.addColumn("info".getBytes, "delivery_address".getBytes(), datas(6).getBytes())
      put.addColumn("info".getBytes, "order_comment".getBytes(), datas(7).getBytes())
      put.addColumn("info".getBytes, "out_trade_no".getBytes(), datas(8).getBytes())
      put.addColumn("info".getBytes, "trade_body".getBytes(), datas(9).getBytes())
      put.addColumn("info".getBytes, "create_time".getBytes(), datas(10).getBytes())
      put.addColumn("info".getBytes, "operate_time".getBytes(), datas(11).getBytes())
      put.addColumn("info".getBytes, "expire_time".getBytes(), datas(12).getBytes())
      put.addColumn("info".getBytes, "tracking_no".getBytes(), datas(13).getBytes())
      put.addColumn("info".getBytes, "parent_order_id".getBytes(), datas(14).getBytes())
      put.addColumn("info".getBytes, "img_url".getBytes(), datas(15).getBytes())
      put.addColumn("info".getBytes, "province_id".getBytes(), datas(16).getBytes())
      put.addColumn("info".getBytes, "benefit_reduce_amount".getBytes(), datas(17).getBytes())
      hbTable.put(put);
    }

    override def close(): Unit = {
      hbTable.close()
      connection.close()
    }
  }
}


