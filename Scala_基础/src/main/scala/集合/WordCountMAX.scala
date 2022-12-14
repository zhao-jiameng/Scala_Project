package 集合

object WordCountMAX {
  def main(args: Array[String]): Unit = {
    val tupleLIst=List(
      ("hello world",2),
      ("hello java",3),
      ("hello scala",4),
      ("hello spark from scala",2))
    //思路一：直接展开为普通版本
    val newStringList=tupleLIst.map(kv => (kv._1.trim+" ") * kv._2)
    val wordCountList=newStringList
      .flatMap(_.split(" "))
      .groupBy( word => word)
      .map(kv => (kv._1,kv._2.length))
      .toList
      .sortBy(_._2)(Ordering[Int].reverse)
      .take(3)

    //思路二：直接基于预统计的结果转换
    val tuples = tupleLIst.flatMap(                     //拆分统计好的结果
      tuple => {
        val strings = tuple._1.split(" ")
        strings.map(word => (word, tuple._2))
      }
    )
    val preCountMap = tuples.groupBy(_._1)             //按照单词进行分组
    val countMap = preCountMap.mapValues(_.map(_._2).sum) //叠加每个单词预统计的个数
    val countList=countMap.toList.sortWith(_._2>_._2).take(3)
  }
}
