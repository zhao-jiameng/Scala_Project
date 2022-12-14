package core.bc

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object BccMap {
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setMaster("local").setAppName("BccMap")
    val sc = new SparkContext(sparkConf)
    val value = sc.makeRDD(List(
      ("a",1),("b",2),("c",3)
    ))
    //val value2 = sc.makeRDD(List(("a",1),("b",2),("c",3)))
    //join会导致数据量几何增长，并会影响shuffle的性能，不推荐使用
    //val joinRDD=value.join(value2)
    //joinRDD.collect().foreach(println)
    val map=mutable.Map(("a",1),("b",2),("c",3))
    val bc = sc.broadcast(map)
    value.map{
      case (w,c)=>{
        val i =map.getOrElse(w,0)
        (w,(c,i))
      }
    }.collect().foreach(println)



    sc.stop()
  }
}
