package core.framework.util

import org.apache.spark.SparkContext

object EnvUtil {
  //ThreadLocal可以对线程的内存进行控制，存储数据，共享数据
  private val scLocal =new ThreadLocal[SparkContext]

  def put(sc:SparkContext):Unit={
    scLocal.set(sc)
  }

  def take():SparkContext={
    scLocal.get()
  }

  def clear():Unit={
    scLocal.remove()
  }
}
