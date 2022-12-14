package core.rdd.operator.transfrom

import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat

object oneValue {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc=new SparkContext(sparkConf)
    val rdd=sc.textFile("datas/apache.log")
    // TODO 转换算子-map
    val mapRdd = rdd.map(data =>{
      val strings = data.split(" ")
      strings(6)
    })
    //mapRdd.collect().foreach(println)

    // TODO 转换算子-mapPartitions
    val mpRDD = rdd.mapPartitions(iter => List(iter.max).iterator)
    //mpRDD.collect().foreach(println)

    // TODO 转换算子-mapPartitionsWithIndex
    val mpiRDD = rdd.mapPartitionsWithIndex((index,iter) => {if (index==1) iter else Nil.iterator})
    //mpiRDD.collect().foreach(println)

    // TODO 转换算子-map
    val value = sc.makeRDD(List(List(1, 2),3, List(4, 5)))
    val flatRDD = value.flatMap {
      case list: List[_] => list
      case dat => List(dat)
    }
    //flatRDD.collect().foreach(println)

    // TODO 转换算子-glom
    val value2 = sc.makeRDD(List(1,2,4,5,6,7,8,9),2)
    val glomRDD = value2.glom()
    //glomRDD.collect().foreach(data=> println(data.mkString(",")))
    val maxRDD = glomRDD.map(array => array.max)
    //println(maxRDD.collect().sum)

    // TODO 转换算子-groupBy
    val groupRDD=value2.groupBy(_%2)
    val groupRDD2=rdd.groupBy(_.charAt(0))
    //groupRDD.collect().foreach(println)

    //提取日志时间访问量
    val timeRDD=rdd.map(
      line=>{
        val datas=line.split(" ")
        val time=datas(3)
//        val str = time.substring(11, 13)
//        (str,1)
        val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val date = sdf.parse(time)
        val sdf1 = new SimpleDateFormat("HH")
        val hour = sdf1.format(date)
        (hour,1)
      }
    ).groupBy(_._1).map{
      case (hour,iter)=> (hour,iter.size)
   }//.collect().foreach(println)

    // TODO 转换算子-filter
    val filterRDD = value2.filter(num => num % 2 != 0)
    //filterRDD.collect().foreach(println)

    rdd.filter(
      line=>{
        val strings = line.split(" ")
        val str = strings(3)
        str.startsWith("17/05/2015")
      }
    )//.collect().foreach(println)

    // TODO 转换算子-sample
    //println(value2.sample(false, 0.4, 1))

    // TODO 转换算子-distinct
    //println(value2.distinct())

    // TODO 转换算子-coalesce
    val newRDD = value2.coalesce(1)
    //默认情况下不会将分区打乱组合，可能会导致数据倾斜,可以传入第二个参数，进行shuffle操作
    val newRDD2 = value2.coalesce(1,true)
    //newRDD.saveAsTextFile("output")

    // TODO 转换算子-repartition
    val newRDD3 = value2.repartition(3)

    // TODO 转换算子-sortBy
    val sortRDD=value2.sortBy(num=>num)
    val sortRDD2=value2.sortBy(num=>num,false)//降序

    sc.stop()
  }
}
