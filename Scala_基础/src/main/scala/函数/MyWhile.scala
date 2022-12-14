package 函数

object MyWhile {
  def main(args: Array[String]): Unit = {
    def myWhile(n: =>Boolean):(=>Unit)=>Unit ={
      def doop(op: =>Unit):Unit={
        if (n){
          op
          myWhile(n)(op)
        }
      }
      doop _
    }
    var n=10
    myWhile(n>=1){
      println(n)
      n-=1
    }
    //使用lba表达式
    def myWhile2(n: =>Boolean):(=>Unit)=>Unit ={
      op=>{
        if (n){
          op
          myWhile2(n)(op)
        }
      }
    }
    //使用柯里化
    def myWhile3(n : =>Boolean)(op : =>Unit):Unit={
      if (n){
        op
        myWhile3(n)(op)
      }
    }
  }
}
