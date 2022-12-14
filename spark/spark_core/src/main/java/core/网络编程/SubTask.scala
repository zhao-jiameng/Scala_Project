package core.网络编程

class SubTask extends Serializable {
  var datas: List[Int] = _
  var logic: (Int) => Int = _

  def compute(): List[Int] = {
    datas.map(logic)
  }
}
