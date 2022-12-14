package sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object Hive {
  def main(args: Array[String]): Unit = {
    // TODO 创建SparkSQL的运行环境
    val sparkConf=new SparkConf().setMaster("local[*]").setAppName("Hive")
    val spark=SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()

    //TODO 执行逻辑操作
    //连接外置Hive读取数据
    //1、拷贝Hive-size-xml文件到resources下
    //2、启用Hive的支持
    //3、增加对应驱动（包含mysql）
    spark.sql("show tables").show
    //TODO 关闭环境
    spark.close()
  }
}
