package core.acc

import org.apache.spark.{SparkConf, SparkContext}

object SumAcc {
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setMaster("local").setAppName("SumAcc")
    val sc = new SparkContext(sparkConf)
    val value = sc.makeRDD(List(1,2,3,4))
    val sumACC=sc.longAccumulator("sum")
    sc.doubleAccumulator      //浮点
    sc.collectionAccumulator  //集合
    value.foreach(num=> sumACC.add(num))
    println("sum="+sumACC.value )
    sc.stop()
  }
}
