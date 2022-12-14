package 集合

import scala.collection.mutable

object MutableMap {
  def main(args: Array[String]): Unit = {
    val map=mutable.Map("a"->12,"b"->24)
    map.put("c",3)
    map += (("d",4))
    map +=(("d",12))
    map.toList
    println(map)
//    map.update("d",5)
//    println(map)
//    map.remove("c")
//    map -= "a"
//    println(map)
//    println("=====================")
//    val map2=mutable.Map("f"->55,"b"->66)
//    map ++= map2
//    println("map1:"+map)
//    println("map2"+map2)
//    val map3=map ++ map2
//    println(map3)
  }

}
