package core.acc

import org.apache.spark.{SparkConf, SparkContext}

object SumAccEx {
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setMaster("local").setAppName("SumAccEx")
    val sc = new SparkContext(sparkConf)
    val value = sc.makeRDD(List(1,2,3,4))
    val sumACC=sc.longAccumulator("sum")
    sc.doubleAccumulator      //浮点
    sc.collectionAccumulator  //集合

    value.map(num=> sumACC.add(num))
    //少加：转换算子调用累加器，没有行动算子，就不会执行
    println("sum="+sumACC.value )

    value.map(num=> sumACC.add(num)).collect()
    value.map(num=> sumACC.add(num)).collect()
    //多加：转换算子调用累加器，多个行动算子，就会重复执行
    println("sum="+sumACC.value )

    sc.stop()
  }
}
