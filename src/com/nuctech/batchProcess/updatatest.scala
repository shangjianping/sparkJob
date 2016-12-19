package com.nuctech.batchProcess

import java.io.File
import java.util.Date

import com.nuctech.algorithm.Algorithm
import com.nuctech.hbase.HBaseStorage
import com.nuctech.utils.ParseXML
import net.lingala.zip4j.core.ZipFile
import net.lingala.zip4j.model.ZipParameters
import net.lingala.zip4j.util.Zip4jConstants
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client.{HBaseAdmin, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.{SparkConf, SparkContext}


/**
 * Created by Administrator on 2015/10/8.
 * args:
 * image tableName
 * zookeeper Host
 * image download path(zip/unzip)
 * update flag tableName
 * hdfs hostname
 * Model parentPath
 * hdfs save path
 */
object updatatest {
  def main(args:Array[String]){
    val sparkConf = new SparkConf().setAppName("updatatest")
    //sparkConf.setMaster("local")
    val sc = new SparkContext(sparkConf)
    val bb= Array("/home/spark/recive/unzip/86574MB02201309130016/")
    val aa = sc.parallelize(bb)


    print("bb=="+new Date(System.currentTimeMillis()))
    val result= aa.map(algorithm)



    result.foreach(print)
    println("savehdfs")

    sc.stop()
  }



  def algorithm(name:String): (String) = {
   //var arg = ParseXML.getArgs(name)
    println("aa============="+new Date(System.currentTimeMillis()))




    name
  }


}
