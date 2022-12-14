package 模式匹配

object PatternMatchBase {
  def main(args: Array[String]): Unit = {
    val x=2
    val y=x match {
      case 1 =>"one"
      case 2 =>"two"
      case 3 =>"three"
      case _ =>"other"
    }

    val a=25
    val b=13
    def matchDualOp(op:Char)=op match {
      case '+' => a+b
      case '-' => a-b
      case '*' => a*b
      case '/' => a/b
      case '%' => a%b
      case _ => "非法运算符"
    }

    //模式守卫
    def abs(num:Int)=num match {
      case i if i>=10 =>i
      case i if i<0 => -i
    }

  }

}
