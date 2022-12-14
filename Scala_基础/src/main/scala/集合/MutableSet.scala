package 集合

import scala.collection.mutable

object MutableSet {
  def main(args: Array[String]): Unit = {
    val set=mutable.Set(23,45,78,89)
    val set2=mutable.Set(99,88,55,66)
    set +=55
    set.add(55)
    set.remove(99)
    set ++= set2
    println(set)
    println(set2)

  }

}
