package sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SaveMode, SparkSession}

object JDBC {
  def main(args: Array[String]): Unit = {
    // TODO 创建SparkSQL的运行环境
    val sparkConf=new SparkConf().setMaster("local[*]").setAppName("JDBC")
    val spark=SparkSession.builder().config(sparkConf).getOrCreate()

    //TODO 执行逻辑操作
    //从mysql读取数据
    val df=spark.read
      .format ("jdbc")
      .option ("url", "jdbc:mysql://hadoop101:3306/spark-sql")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123123")
      .option("dbtable", "user")
      .load()
      df.show

    //保存数据
    df.write
      .format ("jdbc")
      .option ("url", "jdbc:mysql://hadoop101:3306/spark-sql")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123123")
      .option("dbtable", "user1")
      .mode(SaveMode.Append)
      .save

    //TODO 关闭环境
    spark.close()
  }
}
