package 集合

object WordCount {
  def main(args: Array[String]): Unit = {
    val strings=List("hello world",
      "hello java",
      "hello scala",
      "hello spark from scala")
    val flatmapList=strings.flatMap(_.split(" "))
    val groupMap=flatmapList.groupBy(word =>word)
    val countMap=groupMap.map(kv => (kv._1,kv._2.length))
    val sortMap=countMap.toList.sortWith(_._2>_._2).take(3)
    println(sortMap)
  }
}
