package core.acc

import org.apache.spark.{SparkConf, SparkContext}

object Acc {
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setMaster("local").setAppName("Acc")
    val sc = new SparkContext(sparkConf)
    val value = sc.makeRDD(List(1,2,3,4))
    var sum=0
    value.foreach(num=> sum+=num)
    println("sum="+sum)
    sc.stop()
  }
}
