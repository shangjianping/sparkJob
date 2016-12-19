package com.nuctech.test

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

import com.nuctech.algorithm.Algorithm
import com.nuctech.hbase.HBaseStorage
import com.nuctech.model._
import com.nuctech.util.RiskLevel
import com.nuctech.utils.{ImageUtils, ParseXML}
import net.lingala.zip4j.core.ZipFile
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.flume.{FlumeUtils, SparkFlumeEvent}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.slf4j.LoggerFactory

/**
 * Created by Administrator on 2015/9/15.
 */
object FlumeEventProcessTest {
  val log = LoggerFactory.getLogger(this.getClass.getName.stripSuffix("$"))
  def main(args: Array[String]) {

    val batchInterval = Milliseconds(2000)

    // Create the context and set the batch size
    val sparkConf = new SparkConf().setAppName("FlumeEventProcess")
    sparkConf.setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, batchInterval)

    val stream = FlumeUtils.createStream(ssc,args(0), args(1).toInt, StorageLevel.MEMORY_ONLY_SER_2)

    // Print out the count of events received from this server in each batch
    stream.count().map(cnt => "Received " + cnt + " flume events." ).print()
    val kvs = stream.map(flumeEvent=>tofile(flumeEvent,args(2)))
    val algorithmResult = kvs.map(kv=>algorithm(args(2),kv))
    val storage =new HBaseStorage(args(3),args(4))
    algorithmResult.foreachRDD(rdd=>{
      storage.save(rdd)
    })

    ssc.start()
    ssc.awaitTermination()
  }


  def algorithm(path :String,kv:(String,String)): InspectInfo ={
    val name = kv._1
    val dateStr = kv._2
    val arg = ParseXML.getArgs(path + "/unzip/"+name+"/")
    val icon = getIcon(path,name)
    val veriResult = null
    val cigResult = null
    val liqResult = null
    val mappingResult = null
    val wasteResult = null
    var updateFlag = "0"
    var level = ""
    var meta :Meta = null
    if(new File(path + "/bak/"+name+"/").exists()){
      FileUtils.deleteDirectory(new File(path + "/bak/"+name+"/"))
    }
    FileUtils.moveDirectoryToDirectory(new File(path + "/unzip/"+name+"/"),new File(path + "/bak/"),true)
    if(arg !=null){
      val custominfo = arg.getCustominfo
      level = RiskLevel.getRiskLevel(veriResult, custominfo,cigResult,liqResult)
      updateFlag = RiskLevel.getUpdateFlag(veriResult,custominfo,level)
      meta = arg.getMeta
    }
    if(meta ==null){
      meta = new Meta
    }
    meta.setRiskLevel(level)
    meta.setState(updateFlag)
    meta.setUploadDate(dateStr)
    val algorithmResult= new AlgorithmResult
    algorithmResult.setICigDetect(cigResult)
    algorithmResult.setISearchPic(mappingResult)
    algorithmResult.setILiquorDetect(liqResult)
    algorithmResult.setISuggest(veriResult)
    algorithmResult.setIWasteDetect(wasteResult)
    val inspectInfo = new InspectInfo
    inspectInfo.setId(name)
    inspectInfo.setMeta(meta)
    inspectInfo.setAlgorithmResult(algorithmResult)
    inspectInfo
  }

  def getIcon(path:String,name:String): Array[Byte] ={
    val iconPath=path + "/unzip/"+name+"/"+name+"_icon.jpg"
    val jpgPath=path + "/unzip/"+name+"/"+name+".jpg"
    var byteIcon:Array[Byte]=null
    if(!new File(iconPath).exists()){
      if(new File(jpgPath).exists()){
        ImageUtils.resize(iconPath,new File(jpgPath))
      }
    }
    if(new File(iconPath).exists()){
      byteIcon=FileUtils.readFileToByteArray(new File(iconPath))
    }
    byteIcon
  }

  def tofile(event:SparkFlumeEvent,parentPath:String): (String,String) ={
    var array = event.event.getBody.array()
    log.info("==========head:"+event.event.getHeaders)
    var dateStr=""
    if(event.event.getHeaders.containsKey("timestamp")) {
      val timestamp = event.event.getHeaders.get("timestamp").toString
      dateStr=new SimpleDateFormat("yyyyMMdd").format(timestamp.toLong)
    }else{
      dateStr=new SimpleDateFormat("yyyyMMdd").format(new Date)
    }
    var filename=""
    if(event.event.getHeaders.containsKey("basename")) {
      val basename = event.event.getHeaders.get("basename").toString
      val path = parentPath + "/zip/" + basename
      if(new File(path).exists()){
        log.info("============:"+path+" is exists")
      }
      log.info("==========" + path + "====size:" + array.length)
      FileUtils.writeByteArrayToFile(new File(path), array)
      val zipFile = new ZipFile(path)
      zipFile.extractAll(parentPath + "/unzip/")
      if (basename.contains(".")) {
        filename = basename.substring(0, basename.indexOf("."))
      } else {
        filename = basename.substring(0, basename.length)
      }
    }
    (filename,dateStr)
  }
}
