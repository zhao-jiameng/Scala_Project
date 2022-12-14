package 正则表达式

object Regex {
  def main(args: Array[String]): Unit = {
    //获取正则表达式对象
    val r = "s".r
    val rr="^\\d{11}$".r
    //准备对象
    val s="zbczbczbc"
    val ss="16634116494"
    //匹配规则
    val maybeString = r.findFirstIn(s)
    if (maybeString.isEmpty) println("没有找到") else println(maybeString.get)
    val maybeString1 = rr.findFirstIn(ss)
    if (maybeString1.isEmpty) println("没有找到") else println(maybeString1.get)
  }

}
