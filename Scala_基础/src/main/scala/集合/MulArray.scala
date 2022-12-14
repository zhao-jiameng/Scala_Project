package 集合

object MulArray {
  def main(args: Array[String]): Unit = {
    val arr: Array[Array[Int]] = Array.ofDim(3, 3)
    arr(0)(0)=1
//    for (i <- arr.indices; j <- arr(i).indices) println(arr(i)(j))
    for (i <- arr.indices; j <- arr(i).indices){
      arr(i)(j)=i+j
    }
    for (i <- arr.indices; j <- arr(i).indices){
      print(arr(i)(j)+"\t")
      if(j==arr(i).length-1) println()
    }
//    arr.foreach(_.foreach(println))
  }


}
