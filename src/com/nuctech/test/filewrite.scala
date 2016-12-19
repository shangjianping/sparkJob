package com.nuctech.test

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

import com.nuctech.model.ISuggest
import com.nuctech.utils.ParseXML
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.flume.SparkFlumeEvent

/**
 * Created by Administrator on 2015/9/11.
 */
object filewrite {
  def main(args: Array[String]) {
//    val sparkConf = new SparkConf().setAppName("FlumeEventProcess")
//    sparkConf.setMaster("local")
//    val sc = new SparkContext(sparkConf)
//    val aa = sc.binaryFiles("file:///E:/test1/Image0002.JPG")
//
//    aa.saveAsObjectFile("hdfs://master.node:8020/tmp/test2/Image0002.JPG")
    //val file = new File("file:///E:/test1/Image0002.JPG")
  //val arg = ParseXML.getArgs("D:/test/unzip/86574MB01201401170057")
    val date = new SimpleDateFormat("yyyyMMdd HH:mm:ss:SSS").parse("20160125 19:54:21:701")
    var dateStr=""
    var timestamp="1453722861701"
    dateStr=new SimpleDateFormat("yyyyMMdd HH:mm:ss:SSS").format(timestamp.toLong)
    val dateStr2=new SimpleDateFormat("yyyyMMdd HH:mm:ss:SSS").format(date)
    val timestamp2=date.getTime.toString
    println(timestamp)
    println(timestamp2)
    println(dateStr)
    println(dateStr2)
    println(new Date())
  }
    def tofile(): String = {
      val aa = new ISuggest
      val filename = aa.getPath

      filename
    }
}
