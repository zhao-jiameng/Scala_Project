package 案例实操

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import 案例实操.util.JDBCUtil

import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable.ListBuffer

object Req1_BlackLIst {
  def main(args: Array[String]): Unit = {
    //TODO 创建环境对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("kafak")
    val ssc=new StreamingContext(sparkConf,Seconds(3))
    //TODO 逻辑处理
    //定义 Kafka 参数
    val kafkaPara: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG ->
        "hadoop101:9092,hadoop102:9092,hadoop103:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "zjm",    //消费者组
      "key.deserializer" ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" ->
        "org.apache.kafka.common.serialization.StringDeserializer"
    )
    //读取 Kafka 数据创建 DStream
    val kafkaDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](ssc,
      LocationStrategies.PreferConsistent, //生产者策略
      ConsumerStrategies.Subscribe[String, String](Set("zjmnew"), kafkaPara)) //消费者策略| topic |config
    //将每条消息的 KV 取出
    val adClickData= kafkaDS.map (
      kafkaData=>{
        val data=kafkaData.value()
        val datas=data.split(" ")
        AdClickData(datas(0),datas(1),datas(2),datas(3),datas(4))
      })
    //TODO 周期性获取黑名单列表
    val ds=adClickData.transform(
      rdd=>{
        //通过JDBC获取
          val blacklist=ListBuffer[String]()

          val conn=JDBCUtil.getConnection
          val sql=conn.prepareStatement("select userid from black_list")
          val rs=sql.executeQuery()
          while (rs.next()){
            blacklist.append(rs.getString(1))
          }
          rs.close()
          sql.close()
          conn.close()
        //TODO 判断点击用户是否在黑名单中
        val filterRDD=rdd.filter(
          data=> !blacklist.contains(data.user)
        )
        //TODO 如果不在，统计数量（采集周期）
        filterRDD.map(
          data=>{
            val sdf=new SimpleDateFormat("yyyy-MM-dd")
            val day=sdf.format(new Date(data.ts.toLong))
            ((day,data.user,data.ad),1)
          }
        ).reduceByKey(_+_)
      }
    )
    //TODO 判断统计数量是否超过点击阈值
    ds.foreachRDD(
      rdd=>{
        rdd.foreach{
          case  ((day,user,ad),count)=>
            if (count>=30){
              //TODO 超过拉进黑名单
              val conn=JDBCUtil.getConnection
              val sql=conn.prepareStatement(
                """
                  |insert into black_list (userid) values (?)
                  |on DUPLICATE KEY
                  |UPDATE userid = ?
                  |""".stripMargin)
              sql.setString(1,user)
              sql.setString(2,user)
              sql.executeUpdate()
              sql.close()
              conn.close()
            }else{
              //TODO 没有超过，将当天广告点击数量更新
              val conn=JDBCUtil.getConnection
              val sql=conn.prepareStatement(
                """
                  |select *
                  |from user_ad_count
                  |where dt=? and userid=? and adid=?
                  |""".stripMargin)
              sql.setString(1,day)
              sql.setString(2,user)
              sql.setString(3,ad)
              //查询统计表数据
              val rs=sql.executeQuery()
              if (rs.next()){
                //存在，更新
                val pastat=conn.prepareStatement(
                  """
                    |update user_ad_count
                    |set count=count + ?
                    |where dt=? and userid=? and adid=?
                    |""".stripMargin)
                pastat.setInt(1,count)
                pastat.setString(2,day)
                pastat.setString(3,user)
                pastat.setString(4,ad)
                pastat.executeUpdate()
                pastat.close()
                //TODO 判断更新数据是否超过阈值，超过拉进黑名单
                val pd=conn.prepareStatement(
                  """
                    |select *
                    |from user_ad_count
                    |where dt=? and userid=? and adid=? and count>=30
                    |""".stripMargin)
                pd.setString(1,day)
                pd.setString(2,user)
                pd.setString(3,ad)
                val rs = pd.executeQuery()
                if (rs.next()){
                  val sql=conn.prepareStatement(
                    """
                      |insert into black_list (userid) values (?)
                      |on DUPLICATE KEY
                      |UPDATE userid = ?
                      |""".stripMargin)
                  sql.setString(1,user)
                  sql.setString(2,user)
                  sql.executeUpdate()
                }
              }else{
                //不存在，新建
                val pastat=conn.prepareStatement(
                  """
                    |insert into user_ad_count (dt,userid,adid,count) value(?,?,?,?)
                    |""".stripMargin)
                pastat.setString(1,day)
                pastat.setString(2,user)
                pastat.setString(3,ad)
                pastat.setInt(4,count)
                pastat.executeUpdate()
                pastat.close()
              }
              rs.close()
              sql.close()
              conn.close()

            }
        }
      }
    )
    //TODO 开始任务
    ssc.start()
    ssc.awaitTermination()
  }
  //广告点击数据
  case class AdClickData(ts:String,area:String,city:String,user:String,ad:String)
}
