package onetest

import org.apache.flink.streaming.api.scala._
/**
 *
 * @PROJECT_NAME: 国赛
 * @PACKAGE_NAME: onetest
 * @author: 赵嘉盟-HONOR
 * @data: 2023-06-03 21:02
 * @DESCRIPTION
 *
 */
object demo9 {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境：
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //TODO 任务一：
    /**
     * 使用Flink消费Kafka中EnvironmentData主题的数据,监控各环境检测设备数据，当温度（Temperature字段）持续3分钟高于38度时记录为预警数据
     * 将结果存入HBase中的gyflinkresult:EnvTemperatureMonitor，key值为“env_temperature_monitor”
     * rowkey“设备id-系统时间”（如：123-2023-01-01 12:06:06.001），将结果截图粘贴至客户端桌面【Release\任务D提交结果.docx】中对应的任务序号下，需要Flink启动运行6分钟以后再截图；
        注：时间语义使用Processing Time。中文内容及格式必须为示例所示内容。同一设备3分钟只预警一次。
     * 字段	类型	中文含义
          rowkey	String	设备id-系统时间（如：123-2023-01-01 12:06:06.001）
          machine_id	String	设备id
          out_warning_time	String	预警生成时间
          预警信息生成时间格式：yyyy-MM-dd HH:mm:ss
     */
    //参考demo8-1
    //TODO 任务二：
    /**
     *使用Flink消费Kafka中ChangeRecord主题的数据，统计每3分钟各设备状态为“预警”且未处理的数据总数
     * 将结果存入MySQL数据库shtd_industry的threemin_warning_state_agg表中（追加写入，表结构如下）
     * 请将任务启动命令复制粘贴至客户端桌面【Release\任务D提交结果.docx】中对应的任务序号下
     * 启动且数据进入后按照设备id降序排序查询threemin_warning_state_agg表进行截图粘贴至客户端桌面【Release\任务D提交结果.docx】中对应的任务序号下
     * 第一次截图后等待3分钟再次查询并截图粘贴至客户端桌面【Release\任务D提交结果.docx】中对应的任务序号下；
     *
     * threemin_warning_state_agg表：
          字段	类型	中文含义
          change_machine_id	int	设备id
          totalwarning	int	未被处理预警总数
          window_end_time	varchar	窗口结束时间（yyyy-MM-dd HH:mm:ss）
     */
    //参考demo4-3
    //TODO 任务三：
    /**
     *使用Flink消费Kafka中ChangeRecord主题的数据，实时统计每个设备从其他状态转变为“运行”状态的总次数,将结果存入MySQL数据库shtd_industry的change_state_other_to_run_agg表中（表结构如下）。请将任务启动命令复制粘贴至客户端桌面【Release\任务D提交结果.docx】中对应的任务序号下，启动1分钟后根据change_machine_id降序查询change_state_other_to_run_agg表并截图粘贴至客户端桌面【Release\任务D提交结果.docx】中对应的任务序号下，启动2分钟后根据change_machine_id降序查询change_state_other_to_run_agg表并再次截图粘贴至客户端桌面【Release\任务D提交结果.docx】中对应的任务序号下。
     * change_state_other_to_run_agg表：
        字段	类型	中文含义
        change_machine_id	int	设备id
        last_machine_state	varchar	上一状态。即触发本次统计的最近一次非运行状态
        total_change_torun	int	从其他状态转为运行的总次数
        in_time	varchar	flink计算完成时间（yyyy-MM-dd HH:mm:ss）
     */
    //参考demo5-2
  }

}
