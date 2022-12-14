package core.framework.service


import core.framework.common.TService
import core.framework.dao.WordCountDao

/**
 * 服务层
 */
class WordCountService extends TService{
  private val wordCountDao=new WordCountDao()
  def dataAnalysis()={ //数据分析
    val value=wordCountDao.readFile("datas/1.txt")
    //TODO 执行业务逻辑
    //分词
    val value1 = value.flatMap(_.split(" "))
    //加1后缀
    val wordToOne = value1.map(word => (word, 1))
    //单词进行分组统计
    val value2 = wordToOne.groupBy(word => word._1)
    //分组数据转换
    val value3 = value2.map {
      case (word, list) => {
        list.reduce(
          (t1, t2) => (t1._1, t1._2 + t2._2)
        )
      }
    }
    value3
  }
}
