package com.nuctech.util

import com.nuctech.model.ISuggest
import com.nuctech.utils.JaxbUtil
import org.slf4j.LoggerFactory


/**
 * Created by shangjianping on 2015/12/1.
 */
object test{
   val log = LoggerFactory.getLogger(this.getClass.getName.stripSuffix("$"))
  def main(args:Array[String]){
   val jaxR = new JaxbUtil(classOf[ISuggest])
   val result:ISuggest = jaxR.fromXml("<iSuggest><path></path></iSuggest>");

    log.info("sada")
    test
 }
  def test(): Unit ={
    log.info("sada")
  }
}
