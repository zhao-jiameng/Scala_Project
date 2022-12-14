package core.rdd.operator.transfrom

import org.apache.spark.{SparkConf, SparkContext}

object pairValue {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("pair")
    val sc=new SparkContext(sparkConf)
    val rdd1 = sc.makeRDD(List(1,2,3,4))
    val rdd2 = sc.makeRDD(List(3,4,5,6))
    // TODO 转换算子-双Value类型-交集
    val value = rdd1.intersection(rdd2)
    println(value.collect().mkString(","))
    // TODO 转换算子-双Value类型-并集
    val value2 = rdd1.union(rdd2)
    println(value2.collect().mkString(","))
    // TODO 转换算子-双Value类型-差集
    val value3 = rdd1.subtract(rdd2)
    println(value3.collect().mkString(","))
    // TODO 转换算子-双Value类型-拉链
    val value4 = rdd1.zip(rdd2)
    println(value4.collect().mkString(","))


    sc.stop()
  }
}
