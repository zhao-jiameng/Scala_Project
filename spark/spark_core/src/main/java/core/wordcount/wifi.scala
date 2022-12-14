package core.wordcount

import org.apache.spark.{SparkConf, SparkContext}

object wifi {
  def main(args: Array[String]): Unit = {
    //TODO 建立和Spark框架的连接
    val sparkConf=new SparkConf().setMaster("local[*]").setAppName("wifi")
    val sc = new SparkContext(sparkConf)
    //TODO 执行业务逻辑
      //读取文件
      val value = sc.textFile("G:\\Python\\wifi密码本.txt",1)
      //分词
      val value1 = value.flatMap(_.split("\t"))
      val value2 = value1.filter(_.size >= 8)
      value2.saveAsTextFile("G:\\out")



    //TODO 关闭连接
    sc.stop()
  }

}
