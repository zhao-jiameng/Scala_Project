package day01
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

/**
 *
 * @PROJECT_NAME: day01
 * @PACKAGE_NAME: day01
 * @author: 赵嘉盟-HONOR
 * @data: 2023-02-14 14:32
 * @DESCRIPTION
 *
 */
object demo1 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.addSource()

    env.execute()
  }
}
