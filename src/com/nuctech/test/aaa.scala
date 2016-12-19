package com.nuctech.test

import java.util.Date

import com.nuctech.model.Args
import com.nuctech.utils.JaxbUtil

import scala.collection.mutable.ArrayBuffer

/**
 * Created by Administrator on 2015/9/1.
 */
object aaa {
  def main(args :Array[String]){


    val aa ="111"
    val c = new Integer(8)
    println(c.toString)

    //val bb = inspectImage(aa)
    print(new Date().getTime.toString)
  }

  def bytesToObjects():Iterator[String] ={
    var filename = "aadfasd"
    var ss = 10
    val array = new ArrayBuffer[String]
    array+=filename
    array.iterator
  }

  def inspectImage(id:String): Args = {
    //val path = new File(id)
    println("=========" + id)
    if (true) {
      //      for (file <- path.listFiles()) {
      //        if (file.getName.endsWith("xml")) {
      //          //print(file.getName())
      //        }
      //
      //        println("=========" + file.getName)
      //
      //      }
      var result = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n<ARGS>\n    <CUSTOMINFO>\n        <ENTRY>\n            <ENTRY_ITEMS>\n                <NAME>dfdfdfdf</NAME>\n            </ENTRY_ITEMS>\n            <ENTRY_ITEMS>\n                <NAME>dfdfdfdf</NAME>\n            </ENTRY_ITEMS>\n        </ENTRY>\n    </CUSTOMINFO>\n    <IMAGEINFO>\n        <GOODS_NUM>30</GOODS_NUM>\n        <IMAGEFOLDER>ddfdf</IMAGEFOLDER>\n        <IMAGEID>2012001</IMAGEID>\n    </IMAGEINFO>\n</ARGS>"
      val jaxb = new JaxbUtil(classOf[Args])
      val bean:Args = jaxb.fromXml(result)
      bean
    }
    else {
      println("========= not found " + id)
      var bean = new Args()
      bean
    }
  }
}
