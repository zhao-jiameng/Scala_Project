package 集合

import scala.collection.immutable.Queue
import scala.collection.mutable

object QueueText {
  def main(args: Array[String]): Unit = {
    val queue = new  mutable.Queue[Int]
    queue.enqueue(1,2,3)
    val i = queue.dequeue()
    val queue2 =Queue(1,2,3)
    val ints = queue2.enqueue(1)
    val dequeue = queue2.dequeue

  }

}
