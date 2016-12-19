package com.nuctech.test

import java.io.File
import java.util

import com.nuctech.hbase.HBaseStorage
import com.nuctech.model._
import com.nuctech.streaming.ImageInputDStream
import com.nuctech.utils.ParseXML
import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.flume.{SparkFlumeEvent, FlumeUtils}
import org.apache.spark.streaming.{StreamingContext, Milliseconds}

/**
 * Created by Administrator on 2015/9/10.
 */
object test {
  def main(args: Array[String]) {
    //val batchInterval = Milliseconds(5000)
    // Create the context and set the batch size
    //val sparkConf = new SparkConf().setAppName("FlumeSpark")
    //sparkConf.setMaster("local")
    //val sc = new SparkContext(sparkConf)

    var a= new ISuggest
    var b = new ISearchPic
    var c = new ICigDetect
    var d = new ILiquorDetect
    var list =new  util.ArrayList[Area]()
    var pics = new util.ArrayList[Pic]()
    var areas = new util.ArrayList[DetectArea]()
    for(i <- 1 to 5){
      var area = new Area
      area.setHscode("unknown")
      area.setSimilarity("0.4")
      area.setImageId(i.toString)
      area.setNo("5")
      list.add(area)
    }
    for(i <- 1 to 5){
      var pic = new Pic
      pic.setSimilarity("0.4")
      pic.setImageId(i.toString)
      pics.add(pic)
    }
    for(i <- 1 to 5){
      var detectArea = new DetectArea
      detectArea.setColor("red")
      detectArea.setCredibility("aa")
      val rect = new Rect
      rect.setBottom("bottom")
      rect.setTop("top")
      detectArea.setRect(rect)
      areas.add(detectArea)
    }


    a.setArea(list)
    b.setPicNum("5")
    b.setPic(pics)
    c.setAreaNum("5")
    c.setResult("0")
    c.setDetectArea(areas)
    d.setAreaNum("5")
    d.setResult("0")
    d.setDetectArea(areas)
    var arg = ParseXML.getArgs("E:\\nuctech\\86574FS01201106210047")
    if(arg==null){
      print("null")
    }

    //new HBaseStorage().convert("001",arg.getMeta,a,b,c,d)
  }

  def tofile(event:SparkFlumeEvent): String ={
    var array = event.event.getBody.array()
    val filename = "E:/test/test.jpg"
    FileUtils.writeByteArrayToFile(new File(filename),array)
    filename
  }
}
