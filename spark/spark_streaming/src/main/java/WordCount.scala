import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    //TODO 创建环境对象
    //StreamingContext创建是需要两个参数
    //第一个代表环境配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("wordcount")
    //第二个代表批量处理的周期（采集周期）
    val ssc=new StreamingContext(sparkConf,Seconds(3))
    //TODO 逻辑处理
    //获取端口数据
    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" "))
    val wordToOne=words.map((_,1))
    val wordToCount=wordToOne.reduceByKey(_+_)
    wordToCount.print()

    //TODO 开始任务
    //由于采集器是长期执行的任务，所以不能直接关闭
    //如果main方法执行完毕，应用程序也会自动结束，所以不能让main执行完毕
    //ssc.stop()
    //1、启动采集器
    ssc.start()
    //2、等待采集器关闭
    ssc.awaitTermination()
  }
}
