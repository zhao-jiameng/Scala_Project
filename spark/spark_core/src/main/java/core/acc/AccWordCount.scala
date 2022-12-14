package core.acc

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable



object AccWordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setMaster("local").setAppName("AccWordCount")
    val sc = new SparkContext(sparkConf)
    val value = sc.makeRDD(List("hello","spark","hello"))
    //累加器：WordCount
    //创建累加器对象
    val wcAcc=new MyAccumulator()
    //向Spark进行注册
    sc.register(wcAcc, "wordCountAcc")
    value.foreach(
      word=>{
        //数据的累加（使用累加器）
        wcAcc.add(word)
      }
    )
    //获取累加器结果
    println(wcAcc.value)


    sc.stop()
  }

  /**
   * 自定义数据累加器
   * 1、继承AccumulatorV2。定义泛型
   *  IN：累加器输入的数据类型
   *  OUT：返回的数据类型
   * 2、重写方法
   */

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
