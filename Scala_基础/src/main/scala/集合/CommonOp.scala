package 集合

object CommonOp {
  def main(args: Array[String]): Unit = {
    val list=List(1,2,3,4,5)
    val set=Set(1,2,3,4,5)
    //    （ 1 ）获取集合长度
    println(list.length)
    //    （ 2 ）获取集合大小
    println(list.size)
    println(set.size)
    //    （ 3 ）循环遍历
    for ( elmt <- list) println(elmt)
    set.foreach(println)
    //    （ 4 ）迭代器
    for (lis <-list.iterator) println(lis)
    //    （ 5 ）生成字符串
    println(list.mkString(","))
    //    （ 6 ）是否包含
    println(list.contains(1))
  }

}
