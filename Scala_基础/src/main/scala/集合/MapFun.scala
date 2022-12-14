package 集合

object MapFun {
  def main(args: Array[String]): Unit = {
    val list=List(1,2,3,4,5)
  //    （ 1 ）过滤:遍历一个集合并从中获取满足指定条件的元素组成一个新的集合
    val ints = list.filter(_ % 2 == 0) //过滤奇数
  //    （ 2 ）转化/映射（map）:将集合中的每一个元素映射到某一个函数
    val map = list.map(_ * 2)
  //    （ 3 ）扁平化（flatten）:打散
    val nestedList=List(List(1,2,3),List(4,5,6),List(7,8))
    val flatList=nestedList(0) ::: nestedList(1) ::: nestedList(2)
    val flatten = nestedList.flatten
  //    （ 4 ）扁平化+映射（flatMap） 相当于先进行 map 操作，在进行 flatten 操作集合中的每个元素的子元素映射到某个函数并返回新集合
    val strings=List("hello world","hello java","hello scala")
    val splitList=strings.map(_.split(" "))       //分词
    val flattenList=splitList.flatten                     //打散
    val flatmapList=strings.flatMap(_.split(" ")) //结合
  //    （ 5 ）分组(group):按照指定的规则对集合的元素进行分组
    val groupMap=list.groupBy(_%2)
    val groupMap2=list.groupBy(data => if (data % 2 == 0) "偶数" else "奇数")
    val groupMap3=flatmapList.groupBy(_.charAt(0))
  }

}
