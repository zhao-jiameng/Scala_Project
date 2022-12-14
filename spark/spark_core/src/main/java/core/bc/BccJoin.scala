package core.bc

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object BccJoin {
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setMaster("local").setAppName("BccJoin")
    val sc = new SparkContext(sparkConf)
    val value = sc.makeRDD(List(
      ("a",1),("b",2),("c",3)
    ))
    val value2 = sc.makeRDD(List(("a",1),("b",2),("c",3)))
    //join会导致数据量几何增长，并会影响shuffle的性能，不推荐使用
    val joinRDD=value.join(value2)
    joinRDD.collect().foreach(println)

    sc.stop()
  }
}
