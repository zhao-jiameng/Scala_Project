package core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object RDD_File_Par {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc=new SparkContext(sparkConf)
    //TODO 创建RDD
    val rdd = sc.textFile("hdfs://hadoop101:8020/test.txt",2)
    rdd.saveAsTextFile("output")
    //TODO 关闭环境
    sc.stop()
  }

}
