package core.网络编程

import java.io.ObjectInputStream
import java.net.ServerSocket

object Executor {
  def main(args: Array[String]): Unit = {
    //启动服务器，接收数据
    val socket = new ServerSocket(9999)
    println("服务器已启动，等待客户端连接")
    //等待客户端连接
    val client = socket.accept()
    val in = client.getInputStream
    val stream = new ObjectInputStream(in)
    //处理接收数据
    //    val i = in.read()
    val task = stream.readObject().asInstanceOf[SubTask]
    val ints = task.compute()
    //    println("接收到客户端的数据:"+i)
    println("计算节点[9999]计算的结果为："+ints)
    //    in.close()
    stream.close()
    client.close()
    socket.close()
  }
}
