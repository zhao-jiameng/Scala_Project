package core.wordcount

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    //TODO 建立和Spark框架的连接
    val sparkConf=new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    //TODO 执行业务逻辑
      //读取文件
      val value = sc.textFile("datas")
      //分词
      val value1 = value.flatMap(_.split(" "))
      //单词进行分组统计
      val value2 = value1.groupBy(word => word)
      //分组数据转换
      val value3 = value2.map {
        case (word, list) => (word, list.size)
      }
      //将转换结果采集到控制台
      println(value3)
    //TODO 关闭连接
    sc.stop()
  }

}
