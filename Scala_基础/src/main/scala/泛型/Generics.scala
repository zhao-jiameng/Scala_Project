package 泛型

object Generics {
  def main(args: Array[String]): Unit = {
      val child:Parent=new Child
      //val childList:MyCollection[Parent]=new Child
      val childList:MyCollection2[Parent]=new MyCollection2[Child]
      val childList2:MyCollection3[SubChild]=new MyCollection3[Child]

    //上下限
    def test[A <: Child](a: A):Unit=println(a.getClass.getName)
    test(new SubChild)
  }

}
//定义继承关系
class Parent{}
class Child extends Parent{}
class SubChild extends Child{}
//定义带泛型的集合类型
class MyCollection[E]{}
class MyCollection2[+E]{}
class MyCollection3[-E]{}
