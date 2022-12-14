package 集合

object SimpleFunction {
  def main(args: Array[String]): Unit = {
    val list=List(1,2,3,4,5)
    val list2=List(('a',1),('b',2),('c',3),('d',1))
  //    （ 1 ）求和
    val sum = list.sum
  //    （ 2 ）求乘积
    list.product
  //    （ 3 ）最大值
    list.max
  //    （ 4 ）最小值
    list.min
    list2.minBy( tuples => tuples._2)
    list2.minBy( _._2 )
  //    （ 5 ）排序
    //sorted 对一个集合进行自然排序，通过传递隐式的 Ordering
    val sorted = list.sorted
    //sortBy 对一个属性或多个属性进行排序，通过它的类型
    list2.sortBy(a => a._2)
    val tuples = list2.sortBy(_._2)
    //逆序
    list.sorted.reverse
    list.sorted(Ordering[Int].reverse)
    list2.sortBy(_._2)(Ordering[Int].reverse)
    //比较器 sortWith：基于函数的排序，通过一个 comparator 函数，实现自定义排序的逻辑。
    list.sortWith((a,b)=>a<b)
    list.sortWith(_<_)
    list2.sortWith(_._2<_._2)

  }

}
