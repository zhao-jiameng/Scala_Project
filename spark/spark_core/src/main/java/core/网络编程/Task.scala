package core.网络编程

class Task extends Serializable {
  val datas = List(1, 2, 3, 4)
  //val logic=(num:Int)=>{ num * 2}
  val logic: (Int) => Int = _ * 2

  def compute(): List[Int] = {
    datas.map(logic)
  }
}
