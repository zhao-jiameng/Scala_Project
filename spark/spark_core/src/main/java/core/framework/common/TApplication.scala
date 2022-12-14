package core.framework.common



import core.framework.util.EnvUtil
import org.apache.spark.{SparkConf, SparkContext}

trait TApplication {
  def start(master:String="local[*]",app:String="app")(op : =>Unit):Unit={
    //TODO 建立和Spark框架的连接
    val sparkConf=new SparkConf().setMaster(master).setAppName(app)
    val sc = new SparkContext(sparkConf)
    EnvUtil.put(sc)
    try{
      op
    }catch {
      case ex=>println(ex.getMessage)
    }

    //TODO 关闭连接
    sc.stop()
    EnvUtil.clear()
  }
}
