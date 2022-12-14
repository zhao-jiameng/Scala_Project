package 模式匹配

object MatchTypes {
  def main(args: Array[String]): Unit = {
     //匹配类型
    def describeType(x:Any):String=x match {
      case i: Int => "int"+i
      case s: String =>"String"+ s
      case list:List[Int] => "list"+list
      case array: Array[Int] => "Array"+array
      case a => "Something else:"+ a
    }
    //匹配数组
    for (arr <- List(
      Array(0),
      Array(0,1),
      Array(1,1,0),
      Array(1,1,1),
      Array(0,1,0),
      Array(0,0,0),
      Array("array",20,40)
    )){
      val result=arr match {
        case Array(0)=> "0"
        case Array(0,1)=>"array(0,1)"
        case Array(x,y)=>"array:"+x+","+y
        case Array(0,_*)=>"以0开始的数组"
        case Array(x,0,y)=>"中间元素为0的三元数组"
        case _ => "没匹配到的数组"
      }
    }

    //匹配列表同上（有一个特殊用法）
    val list=List(1,2,3,4,5)
    list match {
      case first :: second :: rest => println(s"first:$first,second:$second,rest:$rest")
      case _ =>println("something else")
    }

    //匹配元组
    for (tuple <- List(
      (0),
      (0,1),
      (1,1,0),
      (1,1,1),
      (0,1,0),
      (0,0,0),
      ("array",20,40)
    )){
      val result=tuple match {
        case (0)=> "0"
        case (0,1)=>"array(0,1)"
        case (x,y)=>"array:"+x+","+y
        case  (0,_)=>"以0开始的数组"
        case (x,0,y)=>"中间元素为0的三元数组"
        case _ => "没匹配到的数组"
      }
    }
  }

}
