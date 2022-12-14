package core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object RDD_File {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc=new SparkContext(sparkConf)
    //TODO 创建RDD
    //从文件中创建RDD
    val rdd = sc.textFile("hdfs://hadoop101:8020/test.txt")//以行为单位读取数据。可以路径，目录，通配符
    val value = sc.wholeTextFiles("datas")                  //以文件读取，结果为元组
    rdd.collect().foreach(println)
    //TODO 关闭环境
    sc.stop()
  }

}
