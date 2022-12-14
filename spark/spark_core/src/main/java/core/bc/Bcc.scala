package core.bc

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Bcc {
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setMaster("local").setAppName("Bcc")
    val sc = new SparkContext(sparkConf)
    val value = sc.makeRDD(List(
      ("a",1),("b",2),("c",3)
    ))

    val map=mutable.Map(("a",1),("b",2),("c",3))
    //封装广播变量
    val bc = sc.broadcast(map)

    value.map{
      case (w,c)=>{
        //获取广播变量
        val i =bc.value.getOrElse(w,0)
        (w,(c,i))
      }
    }.collect().foreach(println)

    sc.stop()
  }
}
