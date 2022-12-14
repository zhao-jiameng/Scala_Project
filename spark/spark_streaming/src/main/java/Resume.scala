import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

object Resume {
  def main(args: Array[String]): Unit = {
    //TODO 创建环境对象
    //设置检查点,创建环境对象
    val ssc = StreamingContext.getActiveOrCreate("cp", () => {
      val sparkConf = new SparkConf().setMaster("local[*]").setAppName("resume")
      val ssc = new StreamingContext(sparkConf, Seconds(3))
      ssc.checkpoint("cp")
      //TODO 逻辑处理
      val datas = ssc.socketTextStream("localhost", 9999)
      //reduceByKeyAndWindow:窗口范围大，滑动幅度小，可以使用，无需重复计算，提升性能
      val windowDS = datas.map((_, 1)).reduceByKeyAndWindow(
        (_ + _),
        (x, y) => x - y,
        Seconds(6),
        Seconds(6))
      windowDS.print()
      ssc
    })
    ssc.checkpoint("cp")
    //TODO 开始任务
    ssc.start()
    ssc.awaitTermination()  //block 阻塞


  }
}
