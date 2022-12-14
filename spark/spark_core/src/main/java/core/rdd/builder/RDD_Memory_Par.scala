package core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object RDD_Memory_Par {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    sparkConf.set("spark.default.parallelism","5")//配置默认分区数
    val sc=new SparkContext(sparkConf)
    //TODO 创建RDD
    //RDD的并行度&分区
    val rdd=sc.makeRDD(//第二个参数为分区数，默认处理器最大核数，可配置
      List(1,2,3,4)
    )
    rdd.saveAsTextFile("output")
    //TODO 关闭环境
    sc.stop()
  }

}
