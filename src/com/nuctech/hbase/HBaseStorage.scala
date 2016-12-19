package com.nuctech.hbase

import java.io.File
import java.lang.reflect.Field

import com.nuctech.model._
import com.nuctech.util.RiskLevel
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HColumnDescriptor, TableName, HTableDescriptor, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{HBaseAdmin, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._
class HBaseStorage(zkHost: String,tableName: String) extends Serializable {
  val log = LoggerFactory.getLogger(this.getClass.getName.stripSuffix("$"))
  def save(result: RDD[InspectInfo]): Unit = {
    val conf = HBaseStorage.getConfiguration(zkHost,tableName)
    val c = result.map(convert)
    c.saveAsNewAPIHadoopDataset(conf)
  }


  def saveState(result: RDD[(String, String)]): Unit = {
    val conf = HBaseStorage.getConfiguration(zkHost,tableName)
    val c = result.map(convert)
    c.saveAsNewAPIHadoopDataset(conf)
  }


  def convert(bean: (String, String)) = {
    val p = new Put(Bytes.toBytes(bean._1))
    if (bean._1 != null) {
      p.add(Bytes.toBytes("meta"), Bytes.toBytes("state"), Bytes.toBytes(bean._2))
    }
    (new ImmutableBytesWritable, p)
  }

  def convert(inspectInfo: InspectInfo) = {
    val p = new Put(Bytes.toBytes(inspectInfo.getId))
    addPut(inspectInfo.getMeta,p,"meta","")
    addPut(inspectInfo.getAlgorithmResult,p,"algorithmResult","")
    (new ImmutableBytesWritable, p)
  }

  def addPut(bean:Any,put:Put,family:String,prefix:String,suffix:String): Put ={
    if(bean !=null) {
      for (field <- bean.getClass.getDeclaredFields) {
        field.setAccessible(true)
        val property = field.get(bean)
        if (property != null) {
          property match {
            case s: String => {
              log.info(family + " : " + prefix + field.getName + suffix + "=" + s.toString)
              if(field.getName.equals("path")&&new File(s.toString).exists()){
                put.add(Bytes.toBytes(family),Bytes.toBytes(prefix+field.getName+suffix),FileUtils.readFileToByteArray(new File(s.toString)))
              }else{
                put.add(Bytes.toBytes(family),Bytes.toBytes(prefix+field.getName+suffix),Bytes.toBytes(s.toString))
              }
            }
            case list: java.util.List[_] => {
              var i = 1
              for (member <- list) {
                addPut(member, put, family, prefix+field.getName+"_", i.toString)
                i += 1
              }
            }
            case b: Array[Byte] => {
              put.add(Bytes.toBytes(family),Bytes.toBytes(prefix+field.getName+suffix),b)
            }
            case _ => addPut(property, put, family, prefix+field.getName+"_", suffix)
          }
        }
      }
    }
    put
  }

  def addPut(bean:Any,put:Put,family:String,prefix:String): Put ={
    if(bean !=null) {
      for (field <- bean.getClass.getDeclaredFields) {
        field.setAccessible(true)
        val property = field.get(bean)
        if (property != null) {
          property match {
            case s: String => {
              log.info(family + " : " + prefix + field.getName + "=" + s.toString)
              //if(field.getName.equals("path")&&new File(s.toString).exists()){
              //  put.add(Bytes.toBytes(family),Bytes.toBytes(prefix+field.getName),FileUtils.readFileToByteArray(new File(s.toString)))
              //}else{
                put.add(Bytes.toBytes(family),Bytes.toBytes(prefix+field.getName),Bytes.toBytes(s.toString))
              //}
            }
            case list: java.util.List[_] => {
              var i = 0
              for (member <- list) {
                addPut(member, put, family, prefix+field.getName+"[" +i.toString + "].")
                i += 1
              }
              put.add(Bytes.toBytes(family),Bytes.toBytes(prefix+field.getName+"Num"),Bytes.toBytes(i.toString))
            }
            case b: Array[Byte] => {
              put.add(Bytes.toBytes(family),Bytes.toBytes(prefix+field.getName),b)
            }
            case _ => addPut(property, put, family, prefix+field.getName+".")
          }
        }
      }
    }
    put
  }

}
object HBaseStorage {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("HbaseTest")
    sparkConf.setMaster("local")


    val sc = new SparkContext(sparkConf)
    val a = sc.parallelize(1 to 9, 3)
    val b = a.map(y => (y, "test5", y))
    val conf = HBaseConfiguration.create()


    conf.set(TableOutputFormat.OUTPUT_TABLE, "test2")
    conf.set("hbase.zookeeper.quorum", "master.node")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("zookeeper.znode.parent", "/hbase-unsecure")

    val job = Job.getInstance(conf)
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
   // val c = b.map(convert)

   // c.saveAsNewAPIHadoopDataset(job.getConfiguration)

    sc.stop()
  }

  def getConfiguration(zkHost: String,tableName: String ):Configuration={
    val conf = HBaseConfiguration.create()
    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    conf.set("hbase.zookeeper.quorum", zkHost)
    //conf.set("hbase.master", zkHost)
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("zookeeper.znode.parent", "/hbase-unsecure")
    conf.set("hbase.client.keyvalue.maxsize","20971520")
    tableExist(tableName,conf)
    val job = Job.getInstance(conf)
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    job.getConfiguration
  }

  def tableExist(tableName: String,conf: Configuration): Unit ={
    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    val admin = new HBaseAdmin(conf)
    if (!admin.isTableAvailable(tableName)) {
      val tableDesc = new HTableDescriptor(TableName.valueOf(tableName))
      tableDesc.addFamily(new HColumnDescriptor("meta"))
      tableDesc.addFamily(new HColumnDescriptor("algorithmResult"))
      admin.createTable(tableDesc)
    }
  }

}


