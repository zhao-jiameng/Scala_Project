package 模式匹配

object MatchTupleExtend {
  def main(args: Array[String]): Unit = {
    //在变量声明匹配
    val (x,y)=(10,"hello")
    val List(first,second,_*)=List(23,45,78,96)
    val fir :: sec :: rest =List(23,56,89,96,32)
    //在for推导式匹配
    val list=List(("a",1),("b",2),("c",3))
      //for (elem <- list) println(elem._1+" "+elem._2)
       for ((word,count)<-list) println(word+" "+count)
       for ((word,_)<-list) println(word) //只考虑一个值
       for (("a",count)<-list) println(count)//指定某个位置的值
  }

}
