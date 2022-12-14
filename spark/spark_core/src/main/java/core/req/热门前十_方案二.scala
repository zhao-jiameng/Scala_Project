package core.req

import org.apache.spark.{SparkConf, SparkContext}


object 热门前十_方案二 {
  //Q:依旧有一次shuffle操作
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setMaster("local").setAppName("热门前十_方案二")
    val sc = new SparkContext(sparkConf)
    //读取原始数据
    val RDD = sc.textFile("datas/user_visit_action.txt")
    //将数据转换结构
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
    val resultRDD = analysisRDD.sortBy(_._2, false).take(10)

    resultRDD.foreach(println)

    sc.stop()
  }

}
