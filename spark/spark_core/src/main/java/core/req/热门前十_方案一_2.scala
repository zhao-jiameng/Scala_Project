package core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object 热门前十_方案一_2 {
  //Q:存在大量shuffle操作（reduceByKey）
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setMaster("local").setAppName("热门前十_方案一_2")
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

    //将品类转换加0，聚合进行排序，并取前十
    val rdd1 = value.map {
      case (cid, cnt) => (cid, (cnt,0,0))
    }
    val rdd2 = value1.map {
      case (cid, cnt) => (cid, (0,cnt,0))
    }
    val rdd3 = value2.map {
      case (cid, cnt) => (cid, (0,0,cnt))
    }
    //将三个数据源组合
    val soruceRDD = rdd1.union(rdd2).union(rdd3)
    val analysisRDD=soruceRDD.reduceByKey(
      (t1,t2)=>(t1._1+t2._1,t1._2+t2._2,t1._3+t2._3)
    )
    val resultRDD = analysisRDD.sortBy(_._2, false).take(10)

    resultRDD.foreach(println)
    sc.stop()
  }

}
