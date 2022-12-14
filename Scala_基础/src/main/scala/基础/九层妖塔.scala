package 基础

object 九层妖塔 {
  def main(args: Array[String]): Unit = {
    for(i <- 1 to 9 ; x=2*i-1 ; kg=9-i){
      println(" "*kg+"*"*x)
    }

    for{
      i <- 1 to 9
      x=2*i-1
      kg=9-i
    }{
      println(" "*kg+"*"*x)
    }
    for (x <- 1 to 17 by 2 ; kg=(17-x)/2){
      println(" "*kg+"*"*x)
    }
  }

}
