package com.nuctech.flume

import java.io.File

import net.lingala.zip4j.core.ZipFile
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.flume.{SparkFlumeEvent, FlumeUtils}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

/**
 * Created by Administrator on 2015/9/7.
 */
object FlumeSpark {
  def main(args: Array[String]) {
    val batchInterval = Milliseconds(5000)
    // Create the context and set the batch size
    val sparkConf = new SparkConf().setAppName("FlumeSpark")
    sparkConf.setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, batchInterval)




      val flumeStream = FlumeUtils.createStream(ssc, "192.168.25.143", 11111, StorageLevel.MEMORY_ONLY_SER_2)


    flumeStream.count().map(cnt => "Received " + cnt + " flume events." ).print()
    val names = flumeStream.map(x=>tofile(x,"E:/test"))
      names.foreachRDD(rdd=> {
        if (rdd.count() != 0) {
          println("rdd not is emty " + rdd.count() + ":")
          var i=0;
          while (i<rdd.collect().length) {
            var x = rdd.collect()(i)
            println("rdd not is emty " + rdd.count() + ":" + x)
            i += 1
          }
        }else{
          println("rdd  is emty ")
        }
      })

    ssc.start()

    ssc.awaitTermination()
  }

  def tofile(event:SparkFlumeEvent,parentPath:String): String ={
    var array = event.event.getBody.array()
    val filename = new String(System.currentTimeMillis().toString)
    val path = parentPath + "/zip/"+filename+".zip"

    print("=========="+path)
    FileUtils.writeByteArrayToFile(new File(path),array)
    val zipFile = new ZipFile(path)
    zipFile.extractAll(parentPath + "/zip/"+filename)
    var name=""
    if(new File(parentPath + "/zip/"+filename).listFiles().length==1){
      var scan = new File(parentPath + "/zip/"+filename).listFiles()(0)
      FileUtils.moveDirectoryToDirectory(scan, new File(parentPath + "/unzip/"), true)
      name=scan.getName
      FileUtils.deleteDirectory(new File(parentPath + "/zip/"+filename))
    }
    name
  }

}
