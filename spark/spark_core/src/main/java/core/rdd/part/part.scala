package core.rdd.part

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object part {
  def main(args: Array[String]): Unit = {

    val sparkConf=new SparkConf().setMaster("local").setAppName("Part")
    val sc = new SparkContext(sparkConf)
    val value = sc.makeRDD(List(
      ("nba","xxxxxxxx"),
      ("cba","xxxxxxxx"),
      ("wnba","xxxxxxxx"),
      ("nba","xxxxxxxx"),
    ),3)
    value.partitionBy(new MyPartitioner).saveAsTextFile("out")

    sc.stop()
  }

  /**
   * 自定义分区器
   * 1、继承Partitioner
   * 2、重写方法
   */
  class MyPartitioner extends Partitioner {
    //分区数量
    override def numPartitions: Int = 3
    //根据数据的key值返回数据所在的分区索引（从0开始）
    override def getPartition(key: Any): Int = {
      key match {
        case "nba"=>0
        case "rnba"=>1
        case _=>2
      }
    }
  }
}
