package onetest

/**
 *
 * @PROJECT_NAME: 国赛
 * @PACKAGE_NAME: onetest
 * @author: 赵嘉盟-HONOR
 * @data: 2023-06-03 21:02
 * @DESCRIPTION
 *
 */
object demo10 {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境：
    /**
     *工业大数据
     * 时间语义使用Processing Time。
     */
    //TODO 任务一：
    /**
     *1、使用Flink消费Kafka中ChangeRecord主题的数据，实时统计每个设备从其他状态转变为“运行”状态的总次数
     * 将结果存入MySQL数据库shtd_industry的change_state_other_to_run_agg表中（表结构如下）
     * 请将任务启动命令复制粘贴至客户端桌面【Release\任务D提交结果.docx】中对应的任务序号下，启动1分钟后根据change_machine_id降序查询change_state_other_to_run_agg表并截图
     * 启动2分钟后根据change_machine_id降序查询change_state_other_to_run_agg表并再次截图，将两次截图粘贴至客户端桌面【Release\任务D提交结果.docx】中对应的任务序号下；
     *
     * change_state_other_to_run_agg表：
          字段	                  类型	        中文含义
          change_machine_id	    int	        设备id
          last_machine_state	  varchar	    上一状态。即触发本次统计的最近一次非运行状态
          total_change_torun	  int	        从其他状态转为运行的总次数
          in_time	              varchar   	flink计算完成时间（yyyy-MM-dd HH:mm:ss）
     */
    //参考demo5-2
    //TODO 任务二：
    /**
     *2、使用Flink消费Kafka中ChangeRecord主题的数据，每隔1分钟输出最近3分钟的预警次数最多的设备，将结果存入Redis中
     * key值为“warning_last3min_everymin_out”，value值为“窗口结束时间，设备id”（窗口结束时间格式：yyyy-MM-dd HH:mm:ss）
     * 使用redis cli以HGETALL key方式获取warning_last3min_everymin_out值
     * 将结果截图粘贴至客户端桌面【Release\任务D提交结果.docx】中对应的任务序号下，需两次截图，第一次截图和第二次截图间隔1分钟以上，第一次截图放前面，第二次截图放后面；
     */
    //参考demo8-3
    //TODO 任务三：
    /**
     *3、使用Flink消费Kafka中EnvironmentData主题的数据,监控各环境检测设备数据，当温度（Temperature字段）持续3分钟高于38度时记录为预警数据，将结果存入Redis中
     * key值为“env_temperature_monitor”，value值为“设备id-预警信息生成时间，预警信息”（预警信息生成时间格式：yyyy-MM-dd HH:mm:ss）
     * 使用redis cli以HGETALL key方式获取env_temperature_monitor值，将结果截图粘贴至客户端桌面【Release\任务D提交结果.docx】中对应的任务序号下，需要Flink启动运行6分钟以后再截图。
     * 注：时间语义使用Processing Time。
        value示例：114-2022-01-01 14:12:19，设备114连续三分钟温度高于38度请及时处理！
        中文内容及格式必须为示例所示内容。
        同一设备3分钟只预警一次。
     */
    //参考demo8-1
  }
}
