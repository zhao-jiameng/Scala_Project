package core.rdd.operator.transfrom

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object keyValue {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("KeyValue")
    val sc=new SparkContext(sparkConf)
    val rdd=sc.makeRDD(List(1,2,3,4))
    val mapRDD=rdd.map((_,1))
    // TODO 转换算子-keyValue-partitionBy
    mapRDD.partitionBy(new HashPartitioner(2))//.saveAsTextFile("output")

    // TODO 转换算子-keyValue-reduceByKey
    val rdd1=sc.makeRDD(List(("a",1),("a",2),("b",1),("a",3)),2)
    val value = rdd1.reduceByKey(_ + _)
    //value.collect().foreach(println)

    // TODO 转换算子-keyValue-groupByKey
    val groupRDD=rdd1.groupByKey()
    //groupRDD.collect().foreach(println)
    // TODO 转换算子-keyValue-aggregateByKey
    //aggregateByKey 算子是函数柯里化，存在两个参数列表
    // 1. 第一个参数列表中的参数表示初始值
    // 2. 第二个参数列表中含有两个参数
      //2.1 第一个参数表示分区内的计算规则
      //2.2 第二个参数表示分区间的计算规则
    //最终的结果应该和初始值的类型保持一致
    val aggRDD=rdd1.aggregateByKey(0)(
      (x,y)=>math.max(x,y),
      (x,y)=> x+y
    )//.collect().foreach(println)
    val aggRDD1=rdd1.aggregateByKey((0,0))(
      (t,v)=> (t._1+v,t._2+1),
      (t1,t2)=>(t1._1+t2._1,t1._2+t2._2)
    )
    val value1 = aggRDD1.mapValues {
      case (num, cnt) => num / cnt
    }
    //value1.collect().foreach(println)

    // TODO 转换算子-keyValue-foldByKey
    //val foldRDD=rdd1.foldByKey(0)(_+_).collect().foreach(println)

    // TODO 转换算子-keyValue-
    val combineRDD1=rdd1.combineByKey(
      v =>(v,1),
      (t:(Int,Int),v)=> (t._1+v,t._2+1),
      (t1:(Int,Int),t2:(Int,Int))=>(t1._1+t2._1,t1._2+t2._2)
    )
    val value2 = combineRDD1.mapValues {
      case (num, cnt) => num / cnt
    }
    //value2.collect().foreach(println)

    // TODO 转换算子-keyValue-join
    val rddd = sc.makeRDD(Array((1, "a"),(2, "b"), (3, "c")))
    val rddd1 = sc.makeRDD(Array((1, 4), (2, 5), (3, 6)))
    //rddd.join (rddd1).collect().foreach(println)

    // TODO 转换算子-keyValue-leftOuterJoin
    val rdddd = sc.makeRDD(Array((1, "a"),(2, "b"), (3, "c"),(3,"d")))
    val rdddd1 = sc.makeRDD(Array((1, 4), (2, 5)))//, (3, 6)))
    //rdddd.leftOuterJoin(rdddd1).collect().foreach(println)

    // TODO 转换算子-keyValue-rightOuterJoin
    rdddd1.rightOuterJoin(rdddd).collect().foreach(println)

    // TODO 转换算子-keyValue-cogroup
    //cogroup:connect(连接)+group（分组）
    rdddd.cogroup(rdddd1).collect().foreach(println)

    sc.stop()
  }
}
