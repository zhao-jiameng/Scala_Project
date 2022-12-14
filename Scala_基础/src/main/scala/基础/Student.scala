package 基础

class Student (name:String,age:Int){
  def printInfo():Unit={
    println(name+" "+age+" "+Student.school)
  }
}
//引入伴生对象
object Student{
  val school:String="山西工程"

  def main(args: Array[String]): Unit = {
    val zjm = new Student("赵嘉盟", 20)
    zjm.printInfo()
  }
}
