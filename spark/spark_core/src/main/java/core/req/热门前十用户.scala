package core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object 热门前十用户 {
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setMaster("local").setAppName("热门前十用户")
    val sc = new SparkContext(sparkConf)
    //读取原始数据
    val RDD = sc.textFile("datas/user_visit_action.txt")
    RDD.cache()
    //获取前十商品
    val top10Ids = top10Category(RDD)
    //过滤原始数据，保留点击和前十品类ID
    val filterRdd = RDD.filter(
      action => {
        val datas = action.split("_")
        if (datas(6) != "-1") top10Ids.contains(datas(6)) else false
      }
    )
    //根据品类id和用户id进行点击量统计
    val reduceRDD = filterRdd.map(
      action => {
        val datas = action.split("_")
        ((datas(6), datas(2)), 1)
      }
    ).reduceByKey(_ + _)
    //将统计结果进行结构转换
    val mapRDD=reduceRDD.map{
      case ((cid,sid),sum)=>(cid,(sid,sum))
    }
    //相同品类进行分组
    val groupRDD = mapRDD.groupByKey()
    //将分组数据进行点击量排序，取前十
    val resultRDD=groupRDD.mapValues(
      iter=>iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(10)
    )
    resultRDD.collect().foreach(println)
    sc.stop()
  }
  def top10Category(RDD:RDD[String])={//将数据转换结构
    val flatRDD = RDD.flatMap(
      action => {
        val datas = action.split("_")
        if (datas(6) != "-1") {
          List((datas(6), (1, 0, 0)))
        } else if (datas(8) != "null") {
          val ids = datas(8).split(",")
          ids.map(id => (id, (0, 1, 0)))
        } else if (datas(10) != "null") {
          val ids = datas(10).split(",")
          ids.map(id => (id, (0, 0, 1)))
        } else {
          Nil
        }
      }
    )
    //分组聚合
    val analysisRDD=flatRDD.reduceByKey(
      (t1,t2)=>(t1._1+t2._1,t1._2+t2._2,t1._3+t2._3)
    )
    //降序取十
    analysisRDD.sortBy(_._2, false).take(10).map(_._1)
  }
}
