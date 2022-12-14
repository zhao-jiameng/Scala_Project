package 集合

object ImmutableMap {
  def main(args: Array[String]): Unit = {
    val map=Map("a"->23,"b"->56,"c"->99)
    //遍历元素
    map.foreach(println)
    //获取所有key或者value
    for (key <- map.keys){
      println(s"$key-->${map.get(key)}")
      println(s"$key-->${map.get(key).get}")

    }
    //获取某个key的value
    println(map.get("a").get)
    println(map.getOrElse("d",0))
    println(map("a"))
  }

}
