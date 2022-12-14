package wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._


//流处理WordCount
object WordCount_l {
  def main(args: Array[String]): Unit = {
    //创建一个流处理的执行环境
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(8)     //设置并行度
    env.disableOperatorChaining()   //全局切断任务链

    //从外部命令中提取参数，作为socket的主机名和端口号
    val paramTool=ParameterTool.fromArgs(args)
    val host=paramTool.get("host")
    val port=paramTool.getInt("port")
    //接受一个socket文本流
    val inputDataStream=env.socketTextStream(host,port)
    //对数据进行转换处理统计
    val resultDataStream=inputDataStream
      .flatMap(_.split(" ")).slotSharingGroup("a")//共享组
      .filter(_.nonEmpty).disableChaining()     //断开合并任务链（前后都断开）
      .map((_,1)).startNewChain()               //开启一个新的任务链
      .keyBy(0)
      .sum(1)

    resultDataStream.print().setParallelism(1)    //单独设置并行度

    //启动任务执行
    env.execute("stream word count")
  }

}
