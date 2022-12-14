package 集合

object Parallel {
  def main(args: Array[String]): Unit = {
    val strings = (1 to 100).map(x => Thread.currentThread.getName)
    println(strings)
    val strings2 = (1 to 100).par.map(x => Thread.currentThread.getId)
    println(strings2)
  }

}
