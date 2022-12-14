package 模式匹配

object MatchObject {
  def main(args: Array[String]): Unit = {
    val student = new Student("zjm", 12)

    //针对对象实例的内容进行匹配
    val result = student match {
      case Student("zjm", 12)=> "YES"
      case _ => "else"
    }
  }
}
//定义类
class Student (val name:String,val age:Int)
//定义伴生对象
object Student{
  def unapply(student: Student):Option[(String,Int)]={
    if (student==null)  None else Some(student.name,student.age)
  }
}