package core.framework.common

import core.framework.util.EnvUtil

trait TDao {
  def readFile(path:String)={
    //读取文件
   EnvUtil.take().textFile(path)
  }
}
