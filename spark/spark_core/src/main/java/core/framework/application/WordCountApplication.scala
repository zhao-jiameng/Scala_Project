package core.framework.application

import core.framework.common.TApplication
import core.framework.controller.WordCountContorller


object WordCountApplication extends App with TApplication{
  start(){
    val contorller=new WordCountContorller()
    contorller.dispatch()
  }

}
