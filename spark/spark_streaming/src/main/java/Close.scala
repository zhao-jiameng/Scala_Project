import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

object Close {
  def main(args: Array[String]): Unit = {
    //TODO 创建环境对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("close")
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
    windowDS.print()
    //TODO 开始任务
    ssc.start()
    //想要关闭采集器，需要新的线程
    //而且要在第三方程序中增加关闭状态
    new Thread(new Runnable {
      override def run(): Unit = {
        //优雅的关闭
        //计算节点不在接受新的数据，而是将现有数据处理完，然后 关闭
        //Mysql:Table(stopSpark)=>Row=>data
        //Redis:Data(K-V)
        //ZK   :/stopSpark
        //HDFS :/stopSpark
        while (true){
          if(true){
            //获取活动状态
            val state = ssc.getState()
            if (state== StreamingContextState.ACTIVE){
              ssc.stop(true,true)
            }
          }
        Thread.sleep(5000)
        }
      System.exit(0)
      }
    })
    ssc.awaitTermination()  //block 阻塞


  }
}
