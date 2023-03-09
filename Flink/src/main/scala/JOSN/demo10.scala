package JOSN

import org.apache.flink.streaming.api.scala._

object demo10 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val data: DataStream[String] = env.readTextFile("src\\main\\resources\\json.txt")
    data.map(_.replace("},","}\n")).print()
    env.execute()
  }
}
