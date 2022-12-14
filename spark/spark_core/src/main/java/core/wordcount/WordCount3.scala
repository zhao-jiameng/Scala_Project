package core.wordcount

import org.apache.spark.{SparkConf, SparkContext}

object WordCount3 {
  def main(args: Array[String]): Unit = {
    //TODO 建立和Spark框架的连接
    val sparkConf=new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    //TODO 执行业务逻辑
      //读取文件
      val value = sc.textFile("datas")
      //分词
      val value1 = value.flatMap(_.split(" "))
      //加1后缀
      val wordToOne = value1.map(word => (word, 1))
      //spark框架提供了更多的功能，可以将分组和聚合使用一个方法实现
      //reduceKey：相同的key的数据，可以对value进行聚合
      val \ = wordToOne.reduceByKey(_ + _)
      //将转换结果采集到控制台
      val tuples = \.collect()
      tuples.foreach(print)
    //TODO 关闭连接
    sc.stop()
  }

}
