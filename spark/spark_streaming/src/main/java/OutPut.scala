import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object OutPut {
  def main(args: Array[String]): Unit = {
    //TODO 创建环境对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("output")
    val ssc=new StreamingContext(sparkConf,Seconds(3))
    ssc.checkpoint("cp")
    //TODO 逻辑处理
    val datas = ssc.socketTextStream("localhost", 9999)
    //reduceByKeyAndWindow:窗口范围大，滑动幅度小，可以使用，无需重复计算，提升性能
    val windowDS = datas.map((_, 1)).reduceByKeyAndWindow(
      (_+_),
      (x,y)=>x-y,
      Seconds(6),
      Seconds(6))
    //如果没有输出操作，会提示错误
    //
    //windowDS.print()
    //TODO 开始任务
    ssc.start()
    ssc.awaitTermination()
  }
}
