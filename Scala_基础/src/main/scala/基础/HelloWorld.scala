package 基础

object HelloWorld { //object：关键字，声明一个单例对象（伴生对象）
  def main(args: Array[String]): Unit = { //main方法：从外部可以直接调用执行的方法
    println("hallo world")
    System.out.println("hello scala")
  }

}
