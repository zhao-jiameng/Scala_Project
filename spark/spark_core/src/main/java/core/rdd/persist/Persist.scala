package core.rdd.persist

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Persist {
  def main(args: Array[String]): Unit = {

    val sparkConf=new SparkConf().setMaster("local").setAppName("Persist")
    val sc = new SparkContext(sparkConf)
    val value = sc.makeRDD(List("hallo word","hallo spark"))
    val value1 = value.flatMap(_.split(" "))
    val value2 = value1.groupBy(word => word)
    value2.cache()
    value2.persist(StorageLevel.NONE)
    val value3 = value2.map {
      case (word, list) => (word, list.size)
    }

    println(value3)

    sc.stop()
  }

}
