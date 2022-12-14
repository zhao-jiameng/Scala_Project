package core.rdd.io

import org.apache.spark.{SparkConf, SparkContext}

object Load {
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setMaster("local").setAppName("Load")
    val sc = new SparkContext(sparkConf)

    val value = sc.textFile("out1")
    println(value.collect().mkString(","))

    val value2 = sc.objectFile[(String,Int)]("out2")
    println(value2.collect().mkString(","))

    val value3 = sc.sequenceFile[String,Int]("out1")
    println(value2.collect().mkString(","))



    sc.stop()
  }
}
