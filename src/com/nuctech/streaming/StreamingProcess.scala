package com.nuctech.streaming

import java.io._

import com.nuctech.hbase.HBaseStorage
import com.nuctech.model.Args
import com.nuctech.utils.JaxbUtil
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by Administrator on 2015/8/6.
 */
object StreamingProcess {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("InpectionImage")
    sparkConf.setMaster("local[2]")
    val batchInterval = Milliseconds(3000)
    val ssc = new StreamingContext(sparkConf, batchInterval)
    val lines= new ImageInputDStream[String](ssc,"127.0.0.1", 11111,bytesToImgs,StorageLevel.MEMORY_AND_DISK_SER)



    val result=lines.map(inspectImage)
    result.foreachRDD(rdd=>{
     // new HBaseStorage().save(rdd,"test2","master.node")
    })


    ssc.start()

    ssc.awaitTermination()

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
      var result="<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n<ARGS>\n    <CUSTOMINFO>\n        <ENTRY>\n            <ENTRY_ITEMS>\n                <NAME>dfdfdfdf</NAME>\n            </ENTRY_ITEMS>\n            <ENTRY_ITEMS>\n                <NAME>dfdfdfdf</NAME>\n            </ENTRY_ITEMS>\n        </ENTRY>\n    </CUSTOMINFO>\n    <IMAGEINFO>\n        <GOODS_NUM>30</GOODS_NUM>\n        <IMAGEFOLDER>ddfdf</IMAGEFOLDER>\n        <IMAGEID>2012001</IMAGEID>\n    </IMAGEINFO>\n</ARGS>"
      val jaxb = new JaxbUtil(classOf[Args])
      val bean:Args =jaxb.fromXml(result)
      bean
    }
    else{
      println("========= not found " + id)
      val bean:Args = new Args()
      bean
    }


  }

  def bytesToLines(inputStream: InputStream): Iterator[String] = {
    val dataInputStream = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"))
    new NextIterator[String] {
      protected override def getNext() = {
        val nextValue = dataInputStream.readLine()
        if (nextValue == null) {
          finished = true
        }
        nextValue
      }

      protected override def close() {
        dataInputStream.close()
      }
    }
  }

  def bytesToImg(inputStream: InputStream): Iterator[String] = {
    println("==============start recive")
    val filename = new String(System.currentTimeMillis().toString)
    val os = new FileOutputStream(new File("E:/test/"+filename+".jpg"));
    var bytesRead = 0;
    var buffer = new Array[Byte](1024)
    bytesRead = inputStream.read(buffer, 0, 1024)
    while (bytesRead != -1) {
      os.write(buffer, 0, bytesRead);
      bytesRead = inputStream.read(buffer, 0, 1024)
    }
    println("==============end recive")
    os.close();
    inputStream.close();
    val array = new ArrayBuffer[String]
    array+=filename
    array.iterator
  }

  def bytesToImgs(inputStream: InputStream): Iterator[String] = {
    println("==============start recive")


    var bytesRead = 0;

    var nameByte = new Array[Byte](21)
    bytesRead = inputStream.read(nameByte, 0, 21)
    val name = new String(nameByte)
    println("=============recive "+name)
    if(bytesRead != -1) {
      var lengthByte = new Array[Byte](4)
      bytesRead = inputStream.read(lengthByte, 0, 4)
      val length = byteToInt(lengthByte)
      println("=============recive "+length)
      val os = new FileOutputStream(new File("E:/test/" + name))
      var buffer = new Array[Byte](1024)
      //bytesRead = inputStream.read(buffer, 0, length)
     // bytesRead =inputStream.read(buffer)
      var sum=0
      bytesRead = inputStream.read(buffer, 0, 1024)
      while (bytesRead != -1&&sum < length) {
        os.write(buffer, 0, bytesRead)
        sum +=bytesRead
        bytesRead = inputStream.read(buffer, 0, 1024)

      }
      println("==========="+bytesRead+" size:"+sum)

      os.close();
      inputStream.close();
    }
    println("==============end recive")

    val array = new ArrayBuffer[String]
    array+=name
    array.iterator
  }

  def byteToInt(bytes:Array[Byte]):Int={
    var iOutcome = 0
    for(i <- 0 to bytes.length-1) {
      var bLoop = bytes(i);
      iOutcome += (bLoop & 0xFF) << (8 * i);
    }
    iOutcome
  }

  
}
