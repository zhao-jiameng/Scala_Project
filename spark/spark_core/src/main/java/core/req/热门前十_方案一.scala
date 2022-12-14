package core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}



object 热门前十_方案一 {
  //Q:RDD重复使用
  //Q:cogroup性能可能较低
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setMaster("local").setAppName("热门前十_方案一")
    val sc = new SparkContext(sparkConf)
    //读取原始数据
    val RDD = sc.textFile("datas/user_visit_action.txt")
    RDD.cache()
    //分别统计每个品类点击的次数，下单的次数和支付的次数：（品类，点击总数） （品类，下单总数） （品类， 支付总数）
    val clickRDD = RDD.filter(
      action => {
        val datas = action.split("_")
        datas(6) != "-1"
      }
    )
    val value = clickRDD.map(
      action => {
        val datas = action.split("_")
        (datas(6), 1)
      }
    ).reduceByKey(_ + _)
    //下单的次数（品类，下单总数）
    val orderRDD = RDD.filter(
      action => {
        val datas = action.split("_")
        datas(8) != "null"
      }
    )
    val value1 = orderRDD.flatMap(
      action => {
        val datas = action.split("_")
        val cid = datas(8)
        val cids = cid.split(",")
        cids.map((_, 1))
      }
    ).reduceByKey(_ + _)
    //支付的次数（品类， 支付总数）
    val payRDD = RDD.filter(
      action => {
        val datas = action.split("_")
        datas(10) != "null"
      }
    )
    val value2 = payRDD.flatMap(
      action => {
        val datas = action.split("_")
        val cid = datas(10)
        val cids = cid.split(",")
        cids.map((_, 1))
      }
    ).reduceByKey(_ + _)

    //将品类结合，进行排序，并取前十
    val value3: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] = value.cogroup(value1, value2)
    val analysisRDD = value3.mapValues {
      case (click, order, pay) =>
        var clickCnt = 0
        val iterator = click.iterator
        if (iterator.hasNext) clickCnt = iterator.next()

        var orderCnt = 0
        val iterator2 = order.iterator
        if (iterator2.hasNext) orderCnt = iterator2.next()

        var payCnt = 0
        val iterator3 = pay.iterator
        if (iterator3.hasNext) payCnt = iterator3.next()

        (clickCnt, orderCnt, payCnt)
    }
    val resultRDD = analysisRDD.sortBy(_._2, false).take(10)

    resultRDD.foreach(println)
    sc.stop()
  }

}
