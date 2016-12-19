package com.nuctech.streaming

import java.io._

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by Administrator on 2015/8/27.
 */
object ImageRecive {
  def main(args: Array[String]){
    val batchInterval = Milliseconds(1000)
  val s = new SparkConf().setAppName("face").setMaster("local")
  val sc = new SparkContext(s)
  val ssc = new StreamingContext(sc,batchInterval)
  val img = new ImageInputDStream[String](ssc,"192.168.25.221", 11111,bytesToLines,StorageLevel.MEMORY_AND_DISK_SER)
   // val img= ssc.socketStream("192.111.111.224", 11111,bytesToImg,StorageLevel.MEMORY_AND_DISK_SER)
  //val imgMap = img.map(x => (new Text(System.currentTimeMillis().toString), x))





    img.foreachRDD(rdd=>{
      if(rdd.count()!=0){
        println("rdd not is emty")
        rdd.foreach(x=> {

          println("=========" + x)
          val path = new File(x);
          if (path.exists()) {
            for (file <- path.listFiles()) {
              if (file.getName.endsWith("xml")) {
                //print(file.getName())
              }
              println("=========" + file.getName)
            }
          }
        })
      }else{
        println("rdd  is emty")
      }
    })
  ssc.start()
  ssc.awaitTermination()
}

  def bytesToImg(inputStream: InputStream): Iterator[String] = {
    println("==============start recive")
    val filename = new String(System.currentTimeMillis().toString)
    val os = new FileOutputStream(new File("E:/test/"+filename+".jpg"));
    var bytesRead = 0;
    var buffer = new Array[Byte](1024);
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

  def bytesToLines1(inputStream: InputStream): Iterator[String] = {
    println("==============start recive")
    new NextIterator[String] {
      protected override def getNext() = {
        val filename = new String(System.currentTimeMillis().toString)
        val os = new FileOutputStream(new File("E:/test/"+filename+".jpg"))
        var bytesRead = 0
        var buffer = new Array[Byte](1024)
        bytesRead = inputStream .read(buffer, 0, 1024)
        while (bytesRead != -1) {
          os.write(buffer, 0, bytesRead)
          bytesRead = inputStream.read(buffer, 0, 1024)
        }
        println("==============end recive")
        os.close()
        inputStream.close()
        val nextValue = filename
        finished = true
        println("==============end "+nextValue)
        nextValue
      }

      protected override def close() {
        //inputStream.close()
      }
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
}
