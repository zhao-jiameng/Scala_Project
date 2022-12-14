package core.rdd.operator.transfrom

import org.apache.spark.{SparkConf, SparkContext}

/**
 *agent.log：时间戳， 省份， 城市， 用户，广告， 中间字段使用空格分隔。
 *统计出每一个省份每个广告被点击数量排行的 Top3
 *
 */
object 案例实操 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("alsc")
    val sc = new SparkContext(sparkConf)
    val value = sc.textFile("datas/agent.log")
    val value1 = value.map(
      data => {
        val strings = data.split(" ")
        ((strings(1), strings(4)), 1)
      }
    )
    val value2 = value1.reduceByKey(_+_)
    val value3 = value2.map {
      case ((x, y), z) => {
        (x, (y, z))
      }
    }
    val value4 = value3.groupByKey()
    val value5 = value4.mapValues(
      iter =>
        iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
    )
    value5.collect().foreach(println)
  }

}
