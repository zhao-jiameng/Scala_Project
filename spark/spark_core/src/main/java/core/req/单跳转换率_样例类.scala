package core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object 单跳转换率_样例类 {
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setMaster("local").setAppName("单跳转换率_样例类")
    val sc = new SparkContext(sparkConf)
    //读取原始数据
    val RDD = sc.textFile("datas/user_visit_action.txt")

    val actionDataRDD = RDD.map(
      action => {
        val datas = action.split("_")
        UserVisitAction(
          datas(0),
          datas(1).toLong,
          datas(2),
          datas(3).toLong,
          datas(4),
          datas(5),
          datas(6).toLong,
          datas(7).toLong,
          datas(8),
          datas(9),
          datas(10),
          datas(11),
          datas(12).toLong
        )
      }
    )
    //指定统计
    val ids=List[Long](1,2,3,4,5,6,7)
    val ok=ids.zip(ids.tail)
    actionDataRDD.cache()
    //计算分母
    val fenmu = actionDataRDD.filter(action=>ids.init.contains(action.page_id)).map(
      action => {
        (action.page_id, 1L)
      }
    ).reduceByKey(_ + _).collect().toMap
    //计算分子
    val groupRDD = actionDataRDD.groupBy(_.session_id)
      val mvRDD = groupRDD.mapValues(
        iter => {
          val tuples = iter.toList.sortBy(_.action_time)
          val flowIds = tuples.map(_.page_id)
          val tuples1 = flowIds.zip(flowIds.tail)
          tuples1.filter(t=>ok.contains(t)).map(t => (t, 1))
        }
      )
      val fenzi = mvRDD.map(_._2).flatMap(list => list).reduceByKey(_ + _)


    fenzi.foreach{
      case ((pid1,pid2),sum)=>
        val i = fenmu.getOrElse(pid1, 0L)
        println(s"页面${pid1}跳转到${pid2}的页面转换率为："+(sum.toDouble/i))
    }

    sc.stop()
  }

//用户访问动作表
case class UserVisitAction(
       date: String,//用户点击行为的日期
       user_id: Long,//用户的 ID
       session_id: String,//Session 的 ID
       page_id: Long,//某个页面的 ID
       action_time: String,//动作的时间点
       search_keyword: String,//用户搜索的关键词
       click_category_id: Long,//某一个商品品类的 ID
       click_product_id: Long,//某一个商品的 ID
       order_category_ids: String,//一次订单中所有品类的 ID 集合
       order_product_ids: String,//一次订单中所有商品的 ID 集合
       pay_category_ids: String,//一次支付中所有品类的 ID 集合
       pay_product_ids: String,//一次支付中所有商品的 ID 集合
       city_id: Long//城市 id
)
}