import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object State_Join {
  def main(args: Array[String]): Unit = {
    //TODO 创建环境对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("state")
    val ssc=new StreamingContext(sparkConf,Seconds(3))
    //设置缓存区目录(检查点目录 )
    ssc.checkpoint("cp")
    //TODO 逻辑处理
    val data9999 = ssc.socketTextStream("localhost", 9999)
    val data8888 = ssc.socketTextStream("localhost", 8888)
    val map9999 = data9999.map((_, 1))
    val map8888 = data8888.map((_, "a"))
    val joinDS = map9999.join(map8888)
    joinDS.print()
    //TODO 开始任务
    ssc.start()
    ssc.awaitTermination()
  }
}
