package com.nuctech.flume

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

import com.nuctech.algorithm.Algorithm
import com.nuctech.hbase.HBaseStorage
import com.nuctech.model._
import com.nuctech.util.RiskLevel
import com.nuctech.utils.{BmpReader, ImageUtils, ParseXML}
import net.lingala.zip4j.core.ZipFile
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.flume.{SparkFlumeEvent, FlumeUtils}
import org.apache.spark.streaming.{StreamingContext, Milliseconds}
import org.slf4j.LoggerFactory

/**
 * Created by Shangjianping on 2015/9/15.
 * args(0):flume hostnmae
 * args(1):flume port
 * args(2):flume recive file directory
 * args(3):zookeeper hostname
 * args(4):result tableName
 * args(5):batch interval
 * args(6):spark algorithm process thread mode
 * args(7):suspect tablename
 */
object FlumeEventProcess {
  val log = LoggerFactory.getLogger(this.getClass.getName.stripSuffix("$"))
  def main(args: Array[String]) {

    val batchInterval = Milliseconds(args(5).toInt)

    // Create the context and set the batch size
    val sparkConf = new SparkConf().setAppName("FlumeEventProcess")
    //sparkConf.setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, batchInterval)

    val stream = FlumeUtils.createPollingStream(ssc,args(0), args(1).toInt, StorageLevel.MEMORY_ONLY_SER_2)

    // Print out the count of events received from this server in each batch
    stream.count().map(cnt => "Received " + cnt + " flume events."++new SimpleDateFormat("yyyyMMdd:HH:mm:ss:SSS").format(new Date())).print()
    val kvs = stream.map(flumeEvent=>tofile(flumeEvent,args(2)))
    val algorithmResult = kvs.map(kv=>algorithm(args(2),args(6),kv))
    val hbaseStorage = new HBaseStorage(args(3),args(4))
    algorithmResult.foreachRDD(rdd=>{
      log.info("hbase save start:"+new SimpleDateFormat("yyyyMMdd:HH:mm:ss:SSS").format(new Date()))
      hbaseStorage.save(rdd)
      log.info("hbase save end:"+new SimpleDateFormat("yyyyMMdd:HH:mm:ss:SSS").format(new Date()))
    })
    ssc.start()
    ssc.awaitTermination()
  }


  def algorithm(path :String,thread:String,kv:(String,String)): InspectInfo ={
    log.info("algorithm function start:"+new SimpleDateFormat("yyyyMMdd:HH:mm:ss:SSS").format(new Date()))
    val name = kv._1
    val timestamp = kv._2
    val dateStr = new SimpleDateFormat("yyyyMMdd").format(timestamp.toLong)
    val arg = ParseXML.getArgs(path + "/unzip/"+name+"/")
    log.info("parseXML end:"+new SimpleDateFormat("yyyyMMdd:HH:mm:ss:SSS").format(new Date()))
    val icon = getIcon(path,name)
    var veriResult: ISuggest = null
    var cigResult: ICigDetect = null
    var liqResult: ILiquorDetect = null
    //var mappingResult:ISearchPic = null
    //var wasteResult:IWasteDetect = null


    if(thread.equals("multi")){
      val veriThread = new Thread(new Runnable {
        override def run() {
          log.info("veri Thread" + Thread.currentThread().getId)
          log.info("veriThread start:" + new SimpleDateFormat("yyyyMMdd:HH:mm:ss:SSS").format(new Date()))
          veriResult = Algorithm.verifylabelAnalyze(arg)
          log.info("veriThread end:" + new SimpleDateFormat("yyyyMMdd:HH:mm:ss:SSS").format(new Date()))
        }
      })
      val cigThread = new Thread(new Runnable {
        override def run() {
          log.info("cig Thread" + Thread.currentThread().getId)
          log.info("cigThread start:" + new SimpleDateFormat("yyyyMMdd:HH:mm:ss:SSS").format(new Date()))
          cigResult = Algorithm.wiCigDetect(arg)
          log.info("cigThread end:" + new SimpleDateFormat("yyyyMMdd:HH:mm:ss:SSS").format(new Date()))
        }
      })
      val liqThread = new Thread(new Runnable {
        override def run() {
          log.info("liq Thread" + Thread.currentThread().getId)
          log.info("liqThread start:" + new SimpleDateFormat("yyyyMMdd:HH:mm:ss:SSS").format(new Date()))
          liqResult = Algorithm.wiLiquorDetect(arg)
          log.info("liqThread end:" + new SimpleDateFormat("yyyyMMdd:HH:mm:ss:SSS").format(new Date()))
        }
      })
      //    val mappingThread = new Thread(new Runnable {
      //      override def run(){
      //        println("mapping Thread"+Thread.currentThread().getId)
      //        mappingResult = Algorithm.search(arg)
      //      }
      //    })
      //    val wasteThread = new Thread(new Runnable {
      //      override def run(){
      //        log.info("waste Thread"+Thread.currentThread().getId)
      //        log.info("wasteThread start:"+new SimpleDateFormat("yyyyMMdd:HH:mm:ss:SSS").format(new Date()))
      //        wasteResult = Algorithm.wiWasteDetect(arg)
      //        log.info("wasteThread end:"+new SimpleDateFormat("yyyyMMdd:HH:mm:ss:SSS").format(new Date()))
      //      }
      //    })
      val threads: Array[Thread] = Array(veriThread, cigThread, liqThread)
      log.info("algorithmstart start:" + new SimpleDateFormat("yyyyMMdd:HH:mm:ss:SSS").format(new Date()))
      for (thread <- threads) {
        thread.start()
      }
      log.info("algorithmstart end:" + new SimpleDateFormat("yyyyMMdd:HH:mm:ss:SSS").format(new Date()))
      for (thread <- threads) {
        thread.join()
      }
    }else{
      veriResult = Algorithm.verifylabelAnalyze(arg)
      cigResult = Algorithm.wiCigDetect(arg)
      liqResult = Algorithm.wiLiquorDetect(arg)
      //    var mappingResult:ISearchPic = null
      //    var wasteResult = Algorithm.wiWasteDetect(arg)
    }

    log.info("algorithm end:"+new SimpleDateFormat("yyyyMMdd:HH:mm:ss:SSS").format(new Date()))
    var updateFlag = "0"
    var level = ""
    var meta :Meta = null

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
    meta.setUploadTimestamp(new Date().getTime.toString)
    meta.setIcon(icon)
    val algorithmResult= new AlgorithmResult
    if(cigResult.getDetectArea!=null){
      cigResult.setAreaNum(cigResult.getDetectArea.size().toString)
      cigResult.setDetectAreaNum(cigResult.getDetectArea.size().toString())
    }
    if(liqResult.getDetectArea!=null){
      liqResult.setAreaNum(liqResult.getDetectArea.size().toString)
      liqResult.setDetectAreaNum(liqResult.getDetectArea.size().toString())
    }
    algorithmResult.setICigDetect(cigResult)
    //algorithmResult.setISearchPic(mappingResult)
    algorithmResult.setILiquorDetect(liqResult)
    algorithmResult.setISuggest(veriResult)
    //algorithmResult.setIWasteDetect(wasteResult)
    algorithmResult.setJpg(getJpg(path,name))
    algorithmResult.setSug(getSug(veriResult.getPath))
    val inspectInfo = new InspectInfo
    inspectInfo.setId(name)
    inspectInfo.setMeta(meta)
    inspectInfo.setAlgorithmResult(algorithmResult)
    log.info("algorithm function time:"+new SimpleDateFormat("yyyyMMdd:HH:mm:ss:SSS").format(new Date()))

    if(new File(path + "/bak/"+name+"/").exists()){
      FileUtils.deleteDirectory(new File(path + "/bak/"+name+"/"))
    }
    FileUtils.moveDirectoryToDirectory(new File(path + "/unzip/"+name+"/"),new File(path + "/bak/"),true)
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

  def getSug(path:String): Array[Byte] ={
    var byteSug:Array[Byte]=null
    if(new File(path).exists()){
      byteSug=BmpReader.bmpTojpg(path)
    }
    byteSug
  }

  def getJpg(path:String,name:String): Array[Byte] ={
    val jpgPath=path + "/unzip/"+name+"/"+name+".jpg"
    var byteJpg:Array[Byte]=null
    if(new File(jpgPath).exists()){
      byteJpg=FileUtils.readFileToByteArray(new File(jpgPath))
    }
    byteJpg
  }

  def tofile(event:SparkFlumeEvent,parentPath:String): (String,String) ={
    log.info("tofile start:"+new SimpleDateFormat("yyyyMMdd:HH:mm:ss:SSS").format(new Date()))
    var array = event.event.getBody.array()
    log.info("==========head:"+event.event.getHeaders)
    var timestamp=""
    if(event.event.getHeaders.containsKey("timestamp")) {
      timestamp = event.event.getHeaders.get("timestamp").toString
    }else{
      timestamp=new Date().getTime.toString
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
    log.info("tofile end:"+new SimpleDateFormat("yyyyMMdd:HH:mm:ss:SSS").format(new Date()))
    (filename,timestamp)
  }
}
