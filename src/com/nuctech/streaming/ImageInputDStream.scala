package com.nuctech.streaming

import java.io.InputStream
import java.net.Socket

import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver

import scala.reflect.ClassTag

/**
 * Created by Administrator on 2015/8/27.
 */


class ImageInputDStream[T: ClassTag](
                                       @transient ssc_ : StreamingContext,
                                       host: String,
                                       port: Int,
                                       bytesToObjects: InputStream => Iterator[T],
                                       storageLevel: StorageLevel
                                       ) extends ReceiverInputDStream[T](ssc_) {

  def getReceiver(): Receiver[T] = {
    new ImageRecevier(host, port, bytesToObjects, storageLevel)
  }
}


class ImageRecevier[T: ClassTag](  host: String,
                        port: Int,
                        bytesToObjects: InputStream => Iterator[T],
                        storageLevel: StorageLevel
                     ) extends Receiver[T](storageLevel) with Logging {

  def onStart() {
    // Start the thread that receives data over a connection
    new Thread("Socket Receiver") {
      setDaemon(true)
      override def run() { receive() }
    }.start()
  }

  def onStop() {
    // There is nothing much to do as the thread calling receive()
    // is designed to stop by itself isStopped() returns false
  }

  /** Create a socket connection and receive data until receiver is stopped */
  def receive() {
    var socket: Socket = null
    try {
      logInfo("Connecting to " + host + ":" + port)
      socket = new Socket(host, port)
      logInfo("Connected to " + host + ":" + port)
      val iterator = bytesToObjects(socket.getInputStream())
      while(!isStopped && iterator.hasNext) {
        store(iterator.next)
      }
      logInfo("Stopped receiving")
      restart("Retrying connecting to " + host + ":" + port)
    } catch {
      case e: java.net.ConnectException =>
        restart("Error connecting to " + host + ":" + port, e)
      case t: Throwable =>
        restart("Error receiving data", t)
    } finally {
      if (socket != null) {
        socket.close()
        logInfo("Closed socket to " + host + ":" + port)
      }
    }
  }
}
