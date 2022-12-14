package sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object UDF {
  def main(args: Array[String]): Unit = {
    // TODO 创建SparkSQL的运行环境
    val sparkConf=new SparkConf().setMaster("local[*]").setAppName("UDF")
    val spark=SparkSession.builder().config(sparkConf).getOrCreate()

    //TODO 执行逻辑操作

    val df = spark.read.json("datas/user.json")
    df.show()
    df.createOrReplaceTempView("user")
    spark.udf.register("prefixName",(name:String)=>"Name:"+name)
    spark.sql("select age,prefixName(name) from user").show

    
    //TODO 关闭环境
    spark.close()
  }

}
