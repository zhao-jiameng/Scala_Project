package 函数

object fun {
  def main(args: Array[String]): Unit = {
    def f (name:String) : Unit = {
      println(name)
    }
    def fun (fsd:String => Unit):Unit={
      fsd("zjm")
    }
    fun(f)
    fun(name => {println(name)})
    fun(println(_))
    fun( println )

    //实际案例，定义一个二元运算函数，操作数写死，操作有参数传入
    def fun2(fun:(Int,Int)=>Int):Int={
      fun(1,2)
    }
    val sum=(a:Int,b:Int)=>a+b
    val minus=(a:Int,b:Int)=>a-b
    println(fun2(sum))
    //简化
    println(fun2((a:Int,b:Int)=>a-b))
    println(fun2((a,b)=>b - a))
    println(fun2(-_+_))
  }

}
