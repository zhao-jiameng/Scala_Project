package 集合

import scala.collection.mutable

object ReduceFun {
  def main(args: Array[String]): Unit = {
    val list=List(23,45,67)
    val map=Map( "a"->1,"b"->2,"c"->3,"d"->4 )
    val map2=mutable.Map( "a"->5,"b"->7,"c"->1,"d"->2,"e"->5)
  //    归约(Reduce) ：通过指定的逻辑将集合中的数据进行聚合，从而减少数据，最终获取结果
    list.reduce(_+_)
    list.reduceRight(_-_) //(23-(45-67))
  //    折叠(Fold )：化简的一种特殊情况
    list.fold(10)(_+_)
    list.foldRight(10)(_-_) //(23-(45-(67-10)))
    val map3=map.foldLeft(map2)(
      (mergedMap, kv) => {
        val key = kv._1
        val value = kv._2
        mergedMap(key)= mergedMap.getOrElse(key, 0) + value
        mergedMap
      }
    )

  }
}
