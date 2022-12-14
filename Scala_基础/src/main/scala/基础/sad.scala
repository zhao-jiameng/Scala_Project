package 基础

object sad {
  def main(args: Array[String]):Unit = {
    try {
      for (elem <- 1 to 10) {
        println(elem)
        if (elem == 5)
          throw new RuntimeException}
    }catch {
      case e =>
    }
    println("正常结束循环")
  }
}
