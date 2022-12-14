import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object State_Transform {
  def main(args: Array[String]): Unit = {
    //TODO 创建环境对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("state")
    val ssc=new StreamingContext(sparkConf,Seconds(3))
    //设置缓存区目录(检查点目录 )
    ssc.checkpoint("cp")
    //TODO 逻辑处理
    val datas = ssc.socketTextStream("localhost", 9999)
    //transform方法可以将底层RDD获取到后进行操作，适合需要代码周期性执行使用
    //Code:Driver端
    val newDS: DStream[String] = datas.transform(
      rdd => {
        //Code：Driver端（周期性执行）
        rdd.map(
          data => {
            //Code:Executor端
            data
          }
        )
      }
    )
    //Code:Driver端
    val newDS1:DStream[String]=datas.map(
      data=>{
        //Code:Executor端
        data
      }
    )
    //TODO 开始任务
    ssc.start()
    ssc.awaitTermination()
  }
}
