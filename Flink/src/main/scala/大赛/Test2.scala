package 大赛

import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector


/**
 *
 * @PROJECT_NAME: Flink
 * @PACKAGE_NAME: 大赛
 * @author: 赵嘉盟-HONOR
 * @data: 2022-11-08 13:00
 * @DESCRIPTION
 *
 */
object Test2 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val inputStream = env.readTextFile("reader.txt")
    val dataStream = inputStream.map(data => {
      val datas = data.split(",")
      for (i <- 0 to datas.length-1) {
       if(datas(i)==""){
         datas(i)="null"
     }
        order_info(datas(1).toLong, datas(2), datas(3), datas(4).toDouble, datas(5), datas(6).toLong, datas(7), datas(8), datas(9), datas(10),
        datas(11), datas(12), datas(13), datas(14), datas(15).toLong, datas(16), datas(17).toInt, datas(18).toDouble, datas(19).toDouble, datas(20).toDouble)
     }
    })
    dataStream.print()
//    val timeStream = dataStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[order_info](Time.milliseconds(5000)) {
//      override def extractTimestamp(t: order_info): Long = {
//        val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//        val create_time = format.parse(t.create_time).getTime
//        var operate_time = format.parse(t.operate_time).getTime
//        if (operate_time isNaN) operate_time = create_time
//        val max = if (create_time > operate_time) create_time else operate_time
//        max
//      }
      //    }).print()
      //timeStream.process(new SplitTempProcessor())
      //    timeStream.process(new ProcessFunction[order_info, DataStream[(String, Double)]] {
      //      override def processElement(i: order_info, context: ProcessFunction[order_info,  DataStream[(String, Double)]]#Context, collector: Collector[ DataStream[(String, Double)]]): Unit = {
      //        if(i.order_status!="1003"||i.order_status!="1005"||i.order_status!="1006" ){
      //          val value: DataStream[(String, Double)] = timeStream.map(data => {
      //            ("tump", i.final_total_amount)
      //          }).keyBy(1).sum(1)
      //          collector.collect(value)
      //        }else if(i.order_status=="1005"){
      //
      //        }else if(i.order_status=="1003"){
      //
      //        }
      //
      //      }
      //    }
      //    timeStream.print()
      env.execute("test2")
    }

}
case class order_info(id:Long,consignee:String,consignee_tel:String,final_total_amount:Double,order_status:String,user_id:Long,delivery_address:String,order_comment:String,
                        out_trade_no:String ,trade_body:String ,create_time:String, operate_time:String, expire_time:String, tracking_no:String, parent_order_id:Long,
                      img_url:String,province_id:Int, benefit_reduce_amount:Double,original_total_amount:Double, feight_fee:Double) extends Serializable

class SplitTempProcessor() extends ProcessFunction[order_info,String] {
  override def processElement(i: order_info, context: ProcessFunction[order_info, String]#Context, collector: Collector[String]): Unit = {
    if(i.order_status!="1003"||i.order_status!="1005"||i.order_status!="1006" ){
      var sum:Double = 0.0;
      sum+=i.final_total_amount
      print(sum)

    }else if(i.order_status=="1005"){

    }else if(i.order_status=="1003"){

    }
  }
}