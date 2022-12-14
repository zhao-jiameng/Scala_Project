import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random

object DIY {
  def main(args: Array[String]): Unit = {
    //TODO 创建环境对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("diy")
    val ssc=new StreamingContext(sparkConf,Seconds(3))
    //TODO 逻辑处理
    val value = ssc.receiverStream(new MyReceiver)
    value.print()
    //TODO 开始任务
    ssc.start()

    ssc.awaitTermination()
  }
  //自定义数据采集器
  class MyReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY){
    private var flg =true
    override def onStart(): Unit = {
      new Thread(new Runnable {
        override def run(): Unit = {
          while (flg){
            val message = new Random().nextInt(10).toString
            store(message)
            Thread.sleep(500)
          }
        }
      }).start()
    }

    override def onStop(): Unit = flg=false
  }
}
