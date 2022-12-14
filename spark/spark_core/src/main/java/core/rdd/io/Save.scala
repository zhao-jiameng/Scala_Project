package core.rdd.io

import org.apache.spark.{SparkConf, SparkContext}

object Save {
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setMaster("local").setAppName("Save")
    val sc = new SparkContext(sparkConf)
    val value = sc.makeRDD(List(("hallo word",1),("hallo spark",1)))

    value.saveAsTextFile("out1")
    value.saveAsObjectFile("out2")
    value.saveAsSequenceFile("out3")


    sc.stop()
  }
}
