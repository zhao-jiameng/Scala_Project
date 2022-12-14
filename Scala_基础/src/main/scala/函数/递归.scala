package 函数

import scala.annotation.tailrec

object 递归 {
  def main(args: Array[String]): Unit = {
    def cj(n:Int):Int={
      if(n==0) return 1
      cj(n-1)*n
    }

    println(cj(5))
    def wdg(n:Int):Int={
      @tailrec
      def loop(n:Int,res:Int):Int={
        if (n==0) return res
        loop(n-1,res*n)
      }
      loop(n,1)
    }
    println(wdg(5))
  }

}
