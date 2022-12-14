package sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Test {
  def main(args: Array[String]): Unit = {
    // TODO 创建SparkSQL的运行环境
    val sparkConf=new SparkConf().setMaster("local[*]").setAppName("Test")
    val spark=SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()

    //TODO 执行逻辑操作
    spark.sql("use test")
    spark.sql(
      """
        |
        |""".stripMargin)
    //TODO 关闭环境
    spark.close()
  }
}
