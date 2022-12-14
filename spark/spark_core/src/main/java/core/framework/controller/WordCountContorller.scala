package core.framework.controller

import core.framework.common.TContorller
import core.framework.service.WordCountService

/**
 * 控制层
 */
class WordCountContorller extends TContorller{
  private val wordCountService=new WordCountService()

  def dispatch():Unit={
    val value=wordCountService.dataAnalysis()
    //将转换结果采集到控制台
    value.foreach(print)
  }
}
