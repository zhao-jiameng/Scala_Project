package 基础

import scala.io.StdIn

/**
 *
 * @PROJECT_NAME: Scala_基础
 * @PACKAGE_NAME: 基础
 * @author: 赵嘉盟-HONOR
 * @data: 2022-09-20 13:51
 * @DESCRIPTION
 *
 */
object 快递 {
  def main(args: Array[String]): Unit = {
    /*
    某快递公司运费计算模块的功能描述如下：
      某快递公司货物快递费分段计价，收费标准规定如下：
        货物重量≤5kg，快递费3元/kg
        5kg<货物重量≤10kg，快递费3.5元/kg
        10kg<货物重量≤20kg，快递费4元/kg
        20kg<货物重量≤30kg，快递费4.5元/kg
        30kg<货物重量≤50kg，快递费5元/kg
        货物重量>50kg，拒收

        示例1：货物重量为4kg，则对应的快递费应这样计算：
        4×3=12（元）
        示例2：货物重量为9kg，则对应的快递费应这样计算：
        5×3+4×3.5=29（元）
        示例3：货物重量为12kg，则对应的快递费应这样计算：
        5×3+5×3.5+2×4=40.5（元）

        编程要求：输入货物重量，然后计算并输出对应的快递费。
     */
    println("请输入货物重量：")
    val z= StdIn.readDouble()
      if (z<=0)  println("错误输入")
      else if (z<=5)  println("快递费为："+ 3*z)
      else if (z<=10)  println("快递费为："+((z-5)*3.5+15.0))
      else if (z<=20)  println("快递费为："+((z-10)*4+32.5))
      else if (z<=30)  println("快递费为："+((z-20)*4.5+72.5))
      else if (z<=50)  println("快递费为："+((z-30)*5+117.5))
      else  println("拒收")
    }
}
