package 函数

object fun高级 {
  def main(args: Array[String]): Unit = {
    def fun(n:Int):Int={
      print("函数被调用")
      n+1
    }
    //函数作为值传递
    val fi=fun _
    val f2:Int=>Int=fun
    println(fi)
    println(fi(25))

    //函数作为参数传递
    def fun2(op:(Int,Int)=>Int,a:Int,b:Int):Int={
      op(a,b)
    }
    def sum(a:Int,b:Int):Int={
      a+b
    }

    println(fun2(sum, 25, 25))
    println(fun2((a,b)=>a+b, 25, 25))
    println(fun2(_ + _, 25, 25))

    //将函数作为返回值
    def fun3():Int=>Unit ={
      def f6(a:Int):Unit={
        println("f6被调用"+a)
      }
      //f6 _
      f6
    }

    println(fun3()(1))
  }

}
