package core.wordcount

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable


object WordCount4 {
  def main(args: Array[String]): Unit = {

    val sparkConf=new SparkConf().setMaster("local").setAppName("WordCount4")
    val sc = new SparkContext(sparkConf)
    wordCount1(sc)
    wordCount2(sc)
    wordCount3(sc)
    wordCount4(sc)
    wordCount5(sc)
    wordCount6(sc)

    sc.stop()
  }
  def wordCount1(sc:SparkContext):Unit={    //group
    val rdd = sc.makeRDD(List("hallo world", "hallo spark"))
    val words = rdd.flatMap(_.split(" "))
    val group = words.groupBy(word => word)
    val wordCount = group.mapValues(iter => iter.size)
    wordCount.collect().foreach(println)
  }
  def wordCount2(sc:SparkContext):Unit={    //groupByKey
    val rdd = sc.makeRDD(List("hallo world", "hallo spark"))
    val words = rdd.flatMap(_.split(" "))
    val map = words.map((_,1))
    val group = map.groupByKey()
    val wordCount = group.mapValues(iter => iter.size)
    wordCount.collect().foreach(println)
  }
  def wordCount3(sc:SparkContext):Unit={    //reduceByKey,aggregateByKey，foldByKey，combineByKey
    val rdd = sc.makeRDD(List("hallo world", "hallo spark"))
    val words = rdd.flatMap(_.split(" "))
    val map = words.map((_,1))
    val wordCount = map.reduceByKey(_+_)
    val wordCount2 = map.aggregateByKey(0)(_+_,_+_)
    val wordCount3 = map.foldByKey(0)(_+_)
    val wordCount4 = map.combineByKey(
      v=>v,
      (x:Int,y)=>x+y,
      (x:Int,y:Int)=>x+y
    )
    wordCount.collect().foreach(println)
    wordCount2.collect().foreach(println)
    wordCount3.collect().foreach(println)
    wordCount4.collect().foreach(println)
  }
  def wordCount4(sc:SparkContext):Unit= { //countByKey
    val rdd = sc.makeRDD(List("hallo world", "hallo spark"))
    val words = rdd.flatMap(_.split(" "))
    val map = words.map((_, 1))
    val wordCount = map.countByKey()
    println(wordCount)
  }
  def wordCount5(sc:SparkContext):Unit= { //countByValue
    val rdd = sc.makeRDD(List("hallo world", "hallo spark"))
    val words = rdd.flatMap(_.split(" "))
    val wordCount = words.countByValue()
    println(wordCount)
  }
  def wordCount6(sc:SparkContext):Unit= { //reduce，,aggregate，foldBy
    val rdd = sc.makeRDD(List("hallo world", "hallo spark"))
    val words = rdd.flatMap(_.split(" "))
    val mapWord=words.map(word=>mutable.Map[String,Int]((word,1)))
    val wordCount = mapWord.reduce(
      (map1, map2) => {
        map2.foreach {
          case (word, count) =>
            val newCount = map1.getOrElse(word, 0) + count
            map1.update(word, newCount)
        }
        map1
      }
    )
    println(wordCount)
  }
}
