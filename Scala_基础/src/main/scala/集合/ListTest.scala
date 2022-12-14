package 集合

object ListTest {
  def main(args: Array[String]): Unit = {
    val list=List(23,45,67)
    val list1=list.::(23)
    println(list1)
    val list2=12 :: 45 :: 67 :: 78 :: Nil
    val list3=list ::: list2
    val list4=list ++ list1
  }
}
