package 集合

object Immutableset {
  def main(args: Array[String]): Unit = {
    val set1=Set(23,45,64,78)
    val set2=set1.+(45)
    val set3=set1 + 55
    val set4=set1 ++ set2
    val set5=set1 - 23
    println(set5)
  }

}
