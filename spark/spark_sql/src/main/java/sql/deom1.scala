package sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @PROJECT_NAME: spark
 * @PACKAGE_NAME: sql
 * @author: 赵嘉盟-HONOR
 * @data: 2022-11-20 11:47
 * @DESCRIPTION
 *
 */
object deom1 {
  val sparkConf=new SparkConf().setMaster("192.168.174.200").setAppName("WordCount")
  private val sparkSession: SparkSession = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","E:\\Hadoop\\windows依赖\\hadoop-2.7.1")
    sparkSession.sql(
      """
        |show tables
        |""".stripMargin)
      .show()
  }

}
