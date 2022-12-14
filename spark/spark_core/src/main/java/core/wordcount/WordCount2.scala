package core.wordcount

import org.apache.log4j.{Logger,Level}
import org.apache.spark.{SparkConf, SparkContext}

object WordCount2 {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    //TODO 建立和Spark框架的连接
    val sparkConf=new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    //TODO 执行业务逻辑
      //读取文件
      val value = sc.textFile("datas/1.txt")
      //分词
      val value1 = value.flatMap(_.split(" "))
      //加1后缀
      val wordToOne = value1.map(word => (word, 1))
      //单词进行分组统计
      val value2 = wordToOne.groupBy(word => word._1)
      //分组数据转换
      val value3 = value2.map {
        case (word, list) => {
          list.reduce(
            (t1, t2) => (t1._1, t1._2 + t2._2)
          )
        }
      }
      //将转换结果采集到控制台
      value3.foreach(print)
    //TODO 关闭连接
    sc.stop()
  }

}
