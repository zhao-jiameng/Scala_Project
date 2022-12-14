import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object State {
  def main(args: Array[String]): Unit = {
    //TODO 创建环境对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("state")
    val ssc=new StreamingContext(sparkConf,Seconds(3))
    //设置缓存区目录(检查点目录 )
    ssc.checkpoint("cp")
    //TODO 逻辑处理
    val datas = ssc.socketTextStream("localhost", 9999)
    //无状态数据操作
    //datas.map((_,1)).reduceByKey(_+_).print()
    //updateStateByKet:根据key对数据的状态进行更新
    //第一个值表示相同key的value数据
    //第二个值表示缓存区相同的key的value数据
    datas.map((_,1)).updateStateByKey(
      (seq:Seq[Int],buff:Option[Int])=>{
        val newCount=buff.getOrElse(0)+seq.sum
        Option(newCount)
      }
    ).print()
    //TODO 开始任务
    ssc.start()
    ssc.awaitTermination()
  }
}
