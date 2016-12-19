package com.nuctech.flume

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

/**
 * Created by Administrator on 2015/9/7.
 */
object FlumeSparktest {
  def main(args: Array[String]) {
    val batchInterval = Milliseconds(10000)
    // Create the context and set the batch size
    val sparkConf = new SparkConf().setAppName("InpectionImage")
    val ssc = new StreamingContext(sparkConf, batchInterval)




    val flumeStream = FlumeUtils.createStream(ssc, "192.168.25.221", 11111, StorageLevel.MEMORY_ONLY)
    //flumeStream.count().map(cnt => "AAAAAA-Received " + cnt + " flume events." ).print()
    flumeStream.map(x => "receivedaaa: "+new String(x.event.getBody.array())).print()
    ssc.start()

    ssc.awaitTermination()
  }



}
