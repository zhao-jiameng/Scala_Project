package core.acc

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable


object AccWordCount2 {
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setMaster("local").setAppName("AccWordCount2")
    val sc = new SparkContext(sparkConf)
    val value = sc.makeRDD(List("hello","spark","hello"))
    val wcAcc=new MyAccumulator()
    sc.register(wcAcc, "wordCountAcc")
    value.foreach(word=> wcAcc.add(word))
    println(wcAcc.value)
    sc.stop()
  }
  class MyAccumulator extends AccumulatorV2[String,mutable.Map[String,Long]]{
    val wcMap = mutable.Map[String, Long]()
    override def isZero: Boolean = wcMap.isEmpty//判断知否为初始状态
    override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = new MyAccumulator()//复制一个新的累加器
    override def reset(): Unit = wcMap.clear()//重置累加器
    override def add(word: String): Unit ={   //获取累加器需要计算的值
      val newcount=wcMap.getOrElse(word,0L)+1L
      wcMap.update(word,newcount)
    }
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {//Driver合并多个累加器
      val map1=this.wcMap
      val map2=other.value
      map2.foreach {
        case (word, count) => {
          val newCount = map1.getOrElse(word, 0L) + count
          wcMap.update(word, newCount)
        }
      }
    }

    override def value: mutable.Map[String, Long] = wcMap //获取累加器结果
  }
}
