package 集合

object array {
  def main(args: Array[String]): Unit = {
    val arr = Array(25, 65, 78, 15, 78)
    val arr2 = new Array[Int](5)
    arr2(0) = 25

    //遍历
    for (i <- 0 until arr.length) println(arr(i))
    for (i <- arr.indices) println(arr(i))
    for (elmt <- arr) println(elmt)

    var iter = arr.iterator
    while (iter.hasNext) {
      println(iter.next())
    }

    arr.foreach((elmt: Int) => println(elmt))
    arr.foreach(println)

    println(arr.mkString("--"))

    //添加元素
    val newarr = arr.+:(30)
    val newarr2 = arr.:+(30)

    val newarr3 = arr :+ 30
    val newarr4 = 30 +: arr
    val newarr5=60 +: 80 +:66 +:arr :+ 88:+99
  }
}
