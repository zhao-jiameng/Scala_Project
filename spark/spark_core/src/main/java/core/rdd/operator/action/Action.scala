package core.rdd.operator.action

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Action {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("action")
    val sc=new SparkContext(sparkConf)
    val rdd=sc.makeRDD(List(1,2,3,4),2)

    // TODO 行动算子-reduce
    val i = rdd.reduce(_ + _)
    //println(s" = ${i}")

    // TODO 行动算子-collect
    val ints = rdd.collect()
    //println(ints.mkString(","))

    // TODO 行动算子-count
    val l = rdd.count()
    //println(l)

    // TODO 行动算子-first
    val i1 = rdd.first()
    //println(i1)

    // TODO 行动算子-take
    val ints1 = rdd.take(3)
    //println(ints1.mkString(","))

    // TODO 行动算子-takeOrdered
    val ints2 = rdd.takeOrdered(3)
    //println(ints2.mkString(","))

    // TODO 行动算子-aggregate
    val i2 = rdd.aggregate(10)(_ + _, _ + _)
    //println(i2)

    // TODO 行动算子-fold
    val i3 = rdd.fold(10)(_ + _)
    //println(i3)

    // TODO 行动算子-countByValue
    val intToLong = rdd.countByValue()
    //println(intToLong)

    // TODO 行动算子-countByKey
    val rddd=sc.makeRDD(List(
      ("a",1), ("a",1), ("a",1), ("a",1)
    ))
    val stringToLong = rddd.countByKey()
    //println(stringToLong)

    // TODO 行动算子-save
    rdd.saveAsTextFile("output")
    rdd.saveAsObjectFile("output1")
    rddd.saveAsSequenceFile("output2")  //必须为键值类型

    // TODO 行动算子-foreach
    //Driver端内存集合的循环遍历方法
    rdd.collect().foreach(println)
    println("******************")
    //Executor端内存数据打印
    rdd.foreach(println)

    sc.stop()
  }
}
