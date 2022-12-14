package sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object Test_sx {
  def main(args: Array[String]): Unit = {
    // TODO 创建SparkSQL的运行环境
    val sparkConf=new SparkConf().setMaster("local[*]").setAppName("Test_sx")
    val spark=SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()

    //TODO 执行逻辑操作
    spark.sql("use test")
    //查询基本数据
    spark.sql(
      """
        |select
        | a.*,
        | p.product_name,
        | c.city_name,
        |from user_visit_action a
        |join product_info on a.click_product_id=p.product_id
        |join city_info on a.city_id =c.city_id
        |where a.click_product_id>-1
        |""".stripMargin).createOrReplaceTempView("t1")

    //根据区域，商品进行聚合
    spark.udf.register("cityRemark",functions.udaf(new cityRemarkUDAF()))
    spark.sql(
      """
        |select
        | area,
        | product_name,
        | count(*) as clickCnt,
        | cityRemark(city_name) as city_remark
        |from t1 group by area,product_name
        |""".stripMargin).createOrReplaceTempView("t2")

    //区域内点击数量进行排行
    spark.sql(
      """
        |select
        | *,
        | rank() over(partition by area order by clickCnt desc ) as rank
        |from t2
        |""".stripMargin).createOrReplaceTempView("t3")

    //取前三
    spark.sql(
      """
        |select
        | *
        |from t3 where rank<=3
        |""".stripMargin).show(false)


    //TODO 关闭环境
    spark.close()
  }
  case class Buffer(var total:Long,var cityMap:mutable.Map[String,Long])
  class cityRemarkUDAF extends Aggregator[String,Buffer,String] {
    override def zero: Buffer = Buffer(0,mutable.Map[String,Long]())

    override def reduce(b: Buffer, a: String): Buffer = {
      b.total+=1
      val newcount=b.cityMap.getOrElse(a,0L)+1
      b.cityMap.update(a,newcount)
      b
    }

    override def merge(b1: Buffer, b2: Buffer): Buffer ={
      b1.total+=b2.total
      val map1=b1.cityMap
      val map2=b2.cityMap
//      两个map合并
//      b1.cityMap=map1.foldLeft(map2){
//        case (map,(city,cnt))=>
//          val newcount=map.getOrElse(city,0L)+cnt
//          map.update(city,newcount)
//          map
//      }
      map2.foreach{
        case (city,cnt)=>
          val newcount=map1.getOrElse(city,0L)+cnt
          map1.update(city,newcount)

      }
      b1.cityMap=map1
      b1
    }

    override def finish(reduction: Buffer): String = {
      val remarkList=ListBuffer[String]()

      val totalCnt=reduction.total
      val cityMap=reduction.cityMap

      //降序排列
      val cityCntList=cityMap.toList.sortWith{ (l,r)=> l._2>r._2}.take(2)
      //百分比
      val hasMore=cityMap.size>2
      var rsum = 0L
      cityCntList.foreach{
        case (city,cnt)=>
          val r=cnt*100/totalCnt
          remarkList.append(s"${city} ${r}%")
          rsum += r
      }
      if (hasMore) remarkList.append(s"其他 ${100-rsum}%")
      remarkList.mkString(",")
    }

    override def bufferEncoder: Encoder[Buffer] =Encoders.product

    override def outputEncoder: Encoder[String] = Encoders.STRING
  }
}
