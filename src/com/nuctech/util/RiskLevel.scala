package com.nuctech.util

import java.util

import com.nuctech.model._
import scala.collection.JavaConversions._
/**
 * Created by Administrator on 2015/9/30.
 */
object RiskLevel {
  def main(args:Array[String]){
    var a= new ISuggest
    var b= new Custominfo
    var c = new ICigDetect
    var d = new ILiquorDetect
    var list =new  util.ArrayList[Area]()
    var entrys =new  util.ArrayList[Entry]()
    var entry = new Entry
    var items = new util.ArrayList[EntryItems]()
    for(i <- 1 to 5){
      var area = new Area
      area.setHscode("unknown")
      area.setSimilarity("0.4")
      list.add(area)
    }
    for(i <- 1 to 5){
      var e = new EntryItems
      e.setHscode("244534"+i+"")
      items.add(e)
    }
    entry.setEntryItems(items)
    entrys.add(entry)
    b.setEntry(entrys)
    a.setArea(list)
    c.setAreaNum("5")
    val sss = getRiskLevel(a,b,c,d)
    println(sss)
  }

  def getUpdateFlag(iSuggest:ISuggest,custominfo:Custominfo,level:String): String = {
    var validAreaNum = 0
    var goodsCount = 0
    var flag = "0"
    if(level.equals("green")) {
      if (iSuggest != null && custominfo != null) {
        if (iSuggest.getArea != null) {
          for (pic <- iSuggest.getArea) {
            if (pic.getHscode() != null && !pic.getHscode().equals("")) {
              print(pic.getHscode)
              if (!pic.getHscode.equals("-1")) {
                validAreaNum += 1
              }
            }
          }
        }
        if (custominfo.getEntry != null) {
          for (entry <- custominfo.getEntry) {
            if (entry.getEntryItems != null) {
              for (item <- entry.getEntryItems) {
                if (item.getHscode != null && !item.getHscode.equals("")) {
                  goodsCount += 1
                }
              }
            }
          }
        }
      }
      if (validAreaNum == 1 && goodsCount == 1) {
        flag = "1"
      }
    }
    flag
  }

  def getRiskLevel(iSuggest:ISuggest,custominfo:Custominfo,iCigDetect:ICigDetect,iLiquorDetect:ILiquorDetect): String ={
    var level = ""
    var simlarity = 0.0
    var num=0
    var sugSet =Set[String]()


    if(iCigDetect!=null){
      if(iLiquorDetect.getResult() !=null){
        if(iCigDetect.getResult.equals("1") && !exsitCig(custominfo)){
          level="red"
        }
      }
    }
    if(iLiquorDetect!=null){
      if(iLiquorDetect.getResult() !=null){
        if(iLiquorDetect.getResult().equals("1") && !exsitLiq(custominfo)){
          level="red"
        }
      }
    }
    if(level.equals("")&&iSuggest!=null&&iSuggest.getArea!=null) {
      for(pic <- iSuggest.getArea){
        if(pic.getHscode() !=null && !pic.getHscode().equals("")) {
          print(pic.getHscode)
          sugSet += (pic.getHscode)
          if (!pic.getHscode.equals("-1") && !pic.getHscode.equals("unknown")) {
              simlarity += pic.getSimilarity.toDouble
              num += 1
          }
        }
      }
      sugSet.foreach(x=>println(x))
      var avgSimlarity =simlarity/num
      if ((sugSet.contains("unknown")) || avgSimlarity<0.5) {
        level = "yellow"
      } else {
        level = "green"
      }
    }
    level
  }

  def exsitCig(custominfo:Custominfo): Boolean ={
    var flag=false
    if(custominfo !=null) {
      if (custominfo.getEntry != null) {
        for (entry <- custominfo.getEntry) {
          if (entry.getEntryItems != null) {
            for (item <- entry.getEntryItems) {
              if (item.getHscode != null && !item.getHscode.equals("")) {
                if (item.getHscode.startsWith("24")) {
                  flag = true
                }
              }
            }
          }
        }
      }
    }
    flag
  }

  def exsitLiq(custominfo:Custominfo): Boolean ={
    var flag=false
    if(custominfo !=null) {
      if (custominfo.getEntry != null) {
        for (entry <- custominfo.getEntry) {
          if (entry.getEntryItems != null) {
            for (item <- entry.getEntryItems) {
              if (item.getHscode != null && !item.getHscode.equals("")) {
                if (item.getHscode.startsWith("2203") || item.getHscode.startsWith("2204") || item.getHscode.startsWith("2205") || item.getHscode.startsWith("2207") || item.getHscode.startsWith("2208")) {
                  flag = true
                }
              }
            }
          }
        }
      }
    }
    flag
  }

}
