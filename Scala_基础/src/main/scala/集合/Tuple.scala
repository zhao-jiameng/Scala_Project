package 集合

object Tuple {
  def main(args: Array[String]): Unit = {
    val tuple=("hallo",100,'a',true)
    print(tuple._1)
    println(tuple.productElement(0))
    for (elem <- tuple.productIterator)
      println(elem)

    val mulTuple=(12,0.3,"haloo",(23,"scala"),23)
    println(mulTuple._4._2)
  }

}
