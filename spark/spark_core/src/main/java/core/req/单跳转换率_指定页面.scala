package core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object 单跳转换率_指定页面 {
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setMaster("local").setAppName("单跳转换率_指定页面")
    val sc = new SparkContext(sparkConf)
    //读取原始数据
    val RDD = sc.textFile("datas/user_visit_action.txt")
    RDD.cache()
    val fenzizhi = fenzi(RDD)
    val fenmuzhi = fenmu(RDD)
//    fenzizhi.foreach{
//      case ((pid1,pid2),sum)=>
//
//        val i = fenmuzhi.getOrElse(pid1, 0L)
//        println(s"页面${a(0)._1}跳转到${a(0)._2}的页面转换率为："+(sum.toDouble/i))
//    }

    sc.stop()
  }
  def fenzi(RDD:RDD[String])={//将数据转换结构
    val ids=List[Long](1,2,3,4,5,6,7,23,40,12)
    val okids:List[(Long, Long)] = ids.zip(ids.tail)
    val okk=(1,2)

    val flatRDD = RDD.flatMap(
      action => {
        val datas = action.split("_")
        List((datas(1).toLong,datas(3).toLong,datas(4)))  //用户id，页面id，时间
      }
    )
    val groupRDD = flatRDD.groupBy(_._1)
    val mvRDD =groupRDD.mapValues(
      iter => {
        val tuples = iter.toList.sortBy(_._3)
        val flowIds = tuples.map(_._2)
        val tuples1 = flowIds.zip(flowIds.tail)
        tuples1
      }
    )
    val value= mvRDD.filter(t => okids.contains(okk))
    value.foreach(println)
    //val value1 = value.map(t => (t, 1)).map(a => (a._1._2, a._2)).reduceByKey(_ + _)
    //val value1=value.map(a=>(a._1,a._2,1L)).map(
     // A=>{
       // val list2:List[(Long,Long),Long]=A._2 :: (A._3) :: Nil
        //list2
    //  }).flatMap(l=>l)//.reduceByKey(_+_)
    //value1
  }


  def fenmu(RDD:RDD[String])={//计算分母
    val ids=List(1,2,3,4,5,6,7,23,40,12)
    val fenmu = RDD.map(
      action => {
        val datas = action.split("_")
        val tuple = datas(3)
        val str = tuple.filter(datas => ids.init.contains(datas))
        (str,1L)
      }
    ).reduceByKey(_ + _).collect()
    fenmu.toMap
    }

}
