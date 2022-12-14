package 函数

object arr操作 {
  def main(args: Array[String]): Unit = {
    //对数组进行处理，将操作抽象出来，处理完毕后结果返回一个新的数组
    val arr:Array[Int]=Array(23,25,65,48)
    def fun(arr:Array[Int],op:Int=>Int):Array[Int]={
      for (i <- arr) yield op(i)
    }
    def addOne(arr:Int):Int={
      arr+1
    }

    val array:Array[Int] = fun(arr, addOne)
    println(array.mkString(","))
    val array2=fun(arr,i => i*2)
    val array3=fun(arr,_ * 2)
  }

}
