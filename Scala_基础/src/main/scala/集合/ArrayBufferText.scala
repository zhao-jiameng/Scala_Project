package 集合

import scala.collection.mutable.ArrayBuffer

object ArrayBufferText {
  def main(args: Array[String]): Unit = {
    val arr=new ArrayBuffer[Int]()
    val arr2=ArrayBuffer(23,56,78)

    //添加元素
    arr += 25
    66 +=: arr
    arr.append(55)
    arr.prepend(66)
    arr.insert(1,56,99,66)
    arr.insertAll(1,arr2)
    println(arr)
    //删除元素
    arr.remove(3)
    arr.remove(3,2)
    println(arr)
    arr -= 66
    println(arr)

    //数组转换
    val array1 = arr.toArray
    val buffer = array1.toBuffer

  }

}
