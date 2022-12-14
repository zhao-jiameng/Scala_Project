import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object State__Window {
  def main(args: Array[String]): Unit = {
    //TODO 创建环境对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("window")
    val ssc=new StreamingContext(sparkConf,Seconds(3))

    //TODO 逻辑处理
    val datas = ssc.socketTextStream("localhost", 9999)
    //窗口的范围应该是采集周期的整倍数
    //默认滑动一个采集周期
    //第二个参数为滑动的步长
    val windowDS = datas.map((_, 1)).window(Seconds(6),Seconds(6))
    windowDS.reduceByKey(_+_).print()
    //TODO 开始任务
    ssc.start()
    ssc.awaitTermination()
  }
}
