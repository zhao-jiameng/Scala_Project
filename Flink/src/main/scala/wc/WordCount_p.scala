package wc

import org.apache.flink.api.scala._

//批处理WordCount
object WordCount_p {
  def main(args: Array[String]): Unit = {
    //创建一个批处理的执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //从文件中读取数据
    val inputPath:String="H:\\Scala程序\\Flink\\src\\main\\resources\\hello.txt"
    val inputdataSet:DataSet[String]=env.readTextFile(inputPath)
    //对数据进行转换处理统计
    val function: DataSet[(String,Int)] = inputdataSet
      .flatMap(_.split(" "))
      .map((_,1))
      .groupBy(0)   //以第一个元素为key进行分组
      .sum(1)     //对所有数据的第二个元素求和
    function.print()
  }

}
