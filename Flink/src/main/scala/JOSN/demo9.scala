package JOSN

import com.alibaba.fastjson.JSON
import org.apache.flink.streaming.api.scala._

object demo9 {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    var i=0;
    val inputStream: DataStream[String] = environment.readTextFile("src\\main\\scala\\new_ShanxiProv\\response.json")
      inputStream.map(
        json=>{
          //i+=1
          val str1: String = JSON.parseObject(json).getJSONArray("data").getJSONObject(1).getString("province")
          val str2: String = JSON.parseObject(json).getJSONArray("data").getJSONObject(1).getString("totalprice")
          (str1,str2.toDouble)
        }
      ).keyBy(_._1).sum(1).print()

environment.execute()

  }
}
