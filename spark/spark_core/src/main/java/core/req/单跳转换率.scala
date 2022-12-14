package core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object 单跳转换率 {
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setMaster("local").setAppName("单跳转换率")
    val sc = new SparkContext(sparkConf)
    //读取原始数据
    val RDD = sc.textFile("datas/user_visit_action.txt")
    RDD.cache()
    val fenzizhi = fenzi(RDD)
    val fenmuzhi = fenmu(RDD)
    fenzizhi.foreach{
      case ((pid1,pid2),sum)=>
        val i = fenmuzhi.getOrElse(pid1, 0L)
        println(s"页面${pid1}跳转到${pid2}的页面转换率为："+(sum.toDouble/i))
    }

    sc.stop()
  }
  def fenzi(RDD:RDD[String])={//将数据转换结构
    val flatRDD = RDD.flatMap(
      action => {
        val datas = action.split("_")
        List((datas(1),datas(3),datas(4)))  //用户id，页面id，时间
      }
    )
    val groupRDD = flatRDD.groupBy(_._1)
    val mvRDD = groupRDD.mapValues(
      iter => {
        val tuples = iter.toList.sortBy(_._3)
        val flowIds = tuples.map(_._2)
        flowIds.zip(flowIds.tail).map(t => (t, 1))
      }
    )
    val value = mvRDD.map(_._2)
    value.flatMap(list => list).reduceByKey(_ + _)

  }
  def fenmu(RDD:RDD[String])={//计算分母
    val fenmu = RDD.map(
      action => {
        val datas = action.split("_")
        (datas(3), 1L)
      }
    ).reduceByKey(_ + _).collect()
    fenmu.toMap
    }

}
