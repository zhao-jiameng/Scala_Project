package twotest

import java.nio.charset.StandardCharsets
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Table}

/**
 *
 * @PROJECT_NAME: BIgData
 * @PACKAGE_NAME: twotest
 * @author: 赵嘉盟-HONOR
 * @data: 2023-08-28 18:03
 * @DESCRIPTION
 *
 */
object hbase {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "192.168.23.51:9092")
    val value1 = env.addSource(new FlinkKafkaConsumer[String]("order", new SimpleStringSchema(), properties))
    value1.addSink(new RichSinkFunction[String] {
        var connection: org.apache.hadoop.hbase.client.Connection = _
        var hbTable: Table = _

        override def open(parameters: Configuration): Unit = {
          val configuration = HBaseConfiguration.create()
          configuration.set("hbase.zookeeper.quorum", "192.168.23.51,192.168.23.60,192.168.23.69")
          configuration.set("hbase.zookeeper.property.clientPort", "2181")
          configuration.set("hbase.rootdir", "hdfs://192.168.23.51:8020/hbase")
          configuration.set("hbase.cluster.distributed", "true")
          configuration.set("hbase.unsafe.stream.capability.enforce", "false")
          connection = ConnectionFactory.createConnection(configuration)
          hbTable = connection.getTable(TableName.valueOf("shtd_result:order_info"))
        }

        override def invoke(value: String): Unit = {
          val data = value.split(",")
          val put = new Put(data(0).getBytes())
          put.addColumn("info".getBytes, "id".getBytes(), data(1).getBytes())
          put.addColumn("info".getBytes, "name".getBytes(), "张三".getBytes(StandardCharsets.UTF_8))
          put.addColumn("info".getBytes, "age".getBytes(), "18".getBytes())
          hbTable.put(put);
        }

        override def close(): Unit = {
          hbTable.close()
          connection.close()
        }
      })
    value1.print()
    env.execute("hbase")
  }

}
