package 函数

object 练习 {
  def main(args: Array[String]): Unit = {
    def fun(i:Int,s: String,c: Char):Boolean={
      if (i==0 && s=="" && c=='0') false else true
    }
    println(fun(0, "", '0'))

    def func(i:Int): String=>(Char => Boolean)={
      def f1(s:String):Char => Boolean={
        def f2(c:Char):Boolean={
          if (i==0 && s=="" && c=='0') false else true
        }
        f2
      }
      f1
    }

    println(func(0)("")('0'))
    //匿名简写
    def func1(i:Int): String=>(Char => Boolean)={
      s=>c=>if (i==0 && s=="" && c=='0') false else true
        }
    println(func1(0)("")('0'))

    //柯里化
    def func2(i:Int)(s:String)(c:Char):Boolean ={
      if (i==0 && s=="" && c=='0') false else true
    }
    println(func2(0)("")('0'))
  }

}
