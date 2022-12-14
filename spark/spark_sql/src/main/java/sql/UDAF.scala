package sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}

object UDAF {
  def main(args: Array[String]): Unit = {
    // TODO 创建SparkSQL的运行环境
    val sparkConf=new SparkConf().setMaster("local[*]").setAppName("UDAF")
    val spark=SparkSession.builder().config(sparkConf).getOrCreate()

    //TODO 执行逻辑操作

    val df = spark.read.json("datas/user.json")
    df.show()
    df.createOrReplaceTempView("user")
    spark.udf.register("ageAvg",new MyAvgUDAF)
    spark.sql("select ageAvg(age) from user").show

    
    //TODO 关闭环境
    spark.close()
  }

  /**
   * 自定义聚合类：计算年龄平均值
   * 弱类型
   */
  class MyAvgUDAF extends UserDefinedAggregateFunction{
    //输入数据的结构:Int
    override def inputSchema: StructType = {
      StructType(
        Array(
          StructField("age",LongType)
        )
      )
    }
    //缓冲区数据类型:Buffer
    override def bufferSchema: StructType = {
      StructType(
        Array(
          StructField("total",LongType) ,
          StructField("count",LongType)
        )
      )
    }
    //结果数据类型：Out
    override def dataType: DataType = LongType
    //函数稳定性
    override def deterministic: Boolean = true
    //缓冲区初始化
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      //buffer.update(0,0L)
      buffer(0)=0L
      buffer(1)=0L
    }
    //根据输入的值更新缓冲区数据
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer.update(0,buffer.getLong(0)+input.getLong(0))
      buffer.update(1,buffer.getLong(1)+1)
    }
    //缓冲区数据合并
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1.update(0,buffer1.getLong(0)+buffer2.getLong(0))
      buffer1.update(1,buffer1.getLong(1)+buffer2.getLong(1))
    }
    //计算
    override def evaluate(buffer: Row): Any = buffer.getLong(0)/buffer.getLong(1)
  }
}
