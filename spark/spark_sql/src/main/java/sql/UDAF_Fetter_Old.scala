package sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}

object UDAF_Fetter_Old {
  def main(args: Array[String]): Unit = {
    // TODO 创建SparkSQL的运行环境
    val sparkConf=new SparkConf().setMaster("local[*]").setAppName("UDAF_Fetter_Old")
    val spark=SparkSession.builder().config(sparkConf).getOrCreate()

    //TODO 执行逻辑操作

    val df = spark.read.json("datas/user.json")
    df.show()
    //早期版本中，spark中使用强类型UDAF操作，强类型聚合函数使用DSL语法操作
    import spark.implicits._
    val ds = df.as[User]
    //将UDAF函数转换为查询的列对象
    val udafCol = new MyAvgUDAF().toColumn

    ds.select(udafCol).show
    
    //TODO 关闭环境
    spark.close()
  }

  /**
   * 自定义聚合类：计算年龄平均值
   * 强类型3.0
   */
  case class User(name:String,age:Long)
  case class Buff(var total:Long,var count:Long)
  class MyAvgUDAF extends Aggregator[User,Buff,Long]{
    //z & zero : 初始化或者零值
    //缓冲区的初始化
    override def zero: Buff =Buff(0L,0L)
    //根据输入的数据更新缓冲区的数据
    override def reduce(b: Buff, in: User): Buff = {
      b.total=b.total+in.age
      b.count=b.count+1
      b
    }
    //合并缓冲区
    override def merge(b1: Buff, b2: Buff): Buff = {
      b1.total=b1.total+b2.total
      b1.count=b1.count+b2.count
      b1
    }
    //计算结果
    override def finish(reduction: Buff): Long = reduction.total/reduction.count
    //缓冲区编码操作
    override def bufferEncoder: Encoder[Buff] = Encoders.product
    //输出的编码操作
    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }
}
