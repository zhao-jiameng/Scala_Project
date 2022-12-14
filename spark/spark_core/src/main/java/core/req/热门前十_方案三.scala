package core.req


import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable


object 热门前十_方案三 {
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setMaster("local").setAppName("热门前十_方案三")
    val sc = new SparkContext(sparkConf)
    //读取原始数据
    val RDD = sc.textFile("datas/user_visit_action.txt")
    val acc=new HotCategoryAccumulator
    sc.register(acc, "name")
    //将数据转换结构
    RDD.foreach(
      action => {
        val datas = action.split("_")
        if (datas(6) != "-1") {
          acc.add((datas(6)),"click")
        } else if (datas(8) != "null") {
          val ids = datas(8).split(",")
          ids.foreach(id=>acc.add(id,"order"))
        } else if (datas(10) != "null") {
          val ids = datas(10).split(",")
          ids.foreach(id=>acc.add(id,"pay"))
        } else {
          Nil
        }
      }
    )
    val categories = acc.value.map(_._2)
    val sort = categories.toList.sortWith(
      (l, r) => if (l.clickCnt > r.clickCnt) true else if (l.clickCnt == r.clickCnt) {
        if (l.orderCnt > r.orderCnt) true else if (l.orderCnt == r.orderCnt) {
          l.payCnt > r.payCnt
        } else false
      } else false
    )
    sort.take(10).foreach(println)


    sc.stop()
  }

}
case class HotCategory(cid:String,var clickCnt:Int,var orderCnt:Int,var payCnt:Int)
/**
 * 自定义累加器
 *  IN：（品类id，行为类型）
 *  OUT：mutable.Map[String,HotCategory]
 */
class HotCategoryAccumulator extends AccumulatorV2[(String,String),mutable.Map[String,HotCategory]]{
  private val hcMap = mutable.Map[String,HotCategory]()
  override def isZero: Boolean = hcMap.isEmpty

  override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] = new HotCategoryAccumulator

  override def reset(): Unit = hcMap.clear()

  override def add(v: (String, String)): Unit = {
    val cid=v._1
    val actionType=v._2
    val category = hcMap.getOrElse(cid, HotCategory(cid, 0, 0, 0))
    if(actionType=="click") category.clickCnt+=1
    else if(actionType=="order") category.orderCnt+=1
    else if(actionType=="pay") category.payCnt+=1
    hcMap.update(cid,category)
  }


  override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]): Unit = {
    val map1=this.hcMap
    val map2=other.value
    map2.foreach{
      case (cid,hc)=>
        val category = map1.getOrElse(cid, HotCategory(cid, 0, 0, 0))
        category.clickCnt+=hc.clickCnt
        category.orderCnt+=hc.orderCnt
        category.payCnt+=hc.payCnt
        map1.update(cid,category)
    }

  }

  override def value: mutable.Map[String, HotCategory] = hcMap
}

