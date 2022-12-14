package core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object RDD_Memory {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc=new SparkContext(sparkConf)
    //TODO 创建RDD
    //从内存中创建RDD
    val seq=Seq(1,2,3,4,5)
    //parallelize:并行
    //val rdd=sc.parallelize(seq)
    val rdd=sc.makeRDD(seq)//底层调用rdd的parallelize
    rdd.collect().foreach(println)
    //TODO 关闭环境
    sc.stop()
  }

}
