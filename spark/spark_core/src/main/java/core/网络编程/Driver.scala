package core.网络编程

import java.io.ObjectOutputStream
import java.net.Socket


object Driver {
  def main(args: Array[String]): Unit = {
    //连接服务器
    val socket1 = new Socket("localhost", 9999)
    val socket2 = new Socket("localhost", 8888)
    //创建流，发送消息
    val stream = socket1.getOutputStream
    val stream2 = socket2.getOutputStream

    //    stream.write(123)
    //    stream.flush()
    //    stream.close()
    val out1 = new ObjectOutputStream(stream)
    val out2 = new ObjectOutputStream(stream2)
    val task= new Task
    val subTask = new SubTask
    val subTask2 = new SubTask

    subTask.logic=task.logic
    subTask.datas=task.datas.take(2)
    out1.writeObject(subTask)
    out1.flush()
    out1.close()
    socket1.close()

    subTask2.logic=task.logic
    subTask2.datas=task.datas.takeRight(2)
    out2.writeObject(subTask2)
    out2.flush()
    out2.close()
    socket2 .close()
    println("客户端发送完毕")
  }
}
