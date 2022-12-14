package 模式匹配

object MatchCaseClass {
  def main(args: Array[String]): Unit = {
    val student1 = Student1("zjm", 12)
    //针对对象实例的内容进行匹配
    val result = student1 match {
      case Student("zjm", 12)=> "YES"
      case _ => "else"
    }
  }
}
//定义样例类
case class Student1 (name:String,age:Int)
