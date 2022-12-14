package 模式匹配

object PartialFunction {
  def main(args: Array[String]): Unit = {
    val positiveAbs : PartialFunction[Int,Int] = {
      case x if x>0 => x
    }
    val negativeAbs : PartialFunction[Int,Int]= { case x if x<0 => -x }
    val zeroAbs : PartialFunction[Int,Int]={ case 0 => 0 }
    def abs(x:Int):Int=(positiveAbs orElse negativeAbs orElse zeroAbs)(x)
  }
}
