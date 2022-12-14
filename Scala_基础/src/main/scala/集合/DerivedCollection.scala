package 集合

object DerivedCollection {
  def main(args: Array[String]): Unit = {
    val list=List(1,2,3,4,5)
    val list2=List(1,2,3,4,5,6,7,8,9,10)
    val set=Set(1,2,3,4,5)
    val set2=Set(1,2,3,4,5,6,7,8,9,10)
    //    （ 1 ）获取集合的头
      list.head
    //    （ 2 ）获取集合的尾（不是头的就是尾）
      val tail = list.tail
    //    （ 3 ）集合最后一个数据
      list.last
    //    （ 4 ）集合初始数据（不包含最后一个）
      val init = list.init
    //    （ 5 ）反转
      list.reverse
    //    （ 6 ）取前（后）n 个元素
      list.take(3)
      list.takeRight(2)
    //    （ 7 ）去掉前（后） n 个元素
      list.drop(3)
      list.dropRight(2)
    //    （ 8 ）并集,set会去重
      val ints = list.union(list2)
      list ::: list2
      set ++ set2
    //    （ 9 ）交集
      val ints1 = list.intersect(list2)
    //    （ 10 ）差集
      val ints2 = list.diff(list2)
      val ints3 = list2.diff(list)
    //    （ 11 ）拉链
      val tuples = list.zip(list2)
      val tuples1 = list2.zip(list)
    //    （ 12 ）滑窗（参数相同叫滚动窗口）
      for (elem <- list.sliding(3,2)) println(elem)
  }

}
