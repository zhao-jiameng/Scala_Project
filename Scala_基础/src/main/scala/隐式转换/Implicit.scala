package 隐式转换

object Implicit {
  def main(args: Array[String]): Unit = {
    //隐式函数
      implicit def convert (num:Int): MyRichInt=new MyRichInt(num)
      println(12.myMax(15))
    //隐式类
    implicit class MyRichInt2(val self:Int){
      def myMax2(num:Int):Int= if (num<self) self else num
      def myMin2(num:Int):Int= if (num>self) self else num
    }
    println(12.myMin2(25))
    //隐式参数(会覆盖默认值)
    implicit val str:String="zjm"
    def sayHello(name:String="yy"):Unit=println("hello"+name)
    def Hello(implicit name:String):Unit=println("hello"+name)
    def sayHi()(implicit name:String):Unit=println("Hi"+name)
    sayHello()
    Hello
    sayHi
    sayHi()("name")
    //简便写法

    implicit val num:Int=23
    def hiAge():Unit=println("hi,"+implicitly[Int])
  }
}
//自定义类
class MyRichInt(val self:Int){
  def myMax(num:Int):Int= if (num<self) self else num
  def myMin(num:Int):Int= if (num>self) self else num
}
