package com.nuctech.batchProcess

import java.io.File


import com.nuctech.algorithm.Algorithm
import com.nuctech.hbase.HBaseStorage

import com.nuctech.utils.ParseXML
import net.lingala.zip4j.core.ZipFile
import net.lingala.zip4j.model.ZipParameters
import net.lingala.zip4j.util.Zip4jConstants
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, HBaseAdmin}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.{SparkContext, SparkConf}
import org.slf4j.LoggerFactory

/**
 * Created by Administrator on 2015/10/8.
 * args:
 * image tableName
 * zookeeper Host
 * image download path(zip/unzip)
 * update flag tableName
 * hdfs hostname
 * Veri Model parentPath
 * Mapping Model parentPath
 * hdfs save path
 */
object updataModel {
  val log = LoggerFactory.getLogger(this.getClass.getName.stripSuffix("$"))
  def main(args:Array[String]){
    val sparkConf = new SparkConf().setAppName("updataModel")
    //sparkConf.setMaster("local")
    val sc = new SparkContext(sparkConf)

    val storage =new HBaseStorage(args(1),args(0))
    val conf = HBaseStorage.getConfiguration(args(1),args(0))
    conf.set(TableInputFormat.INPUT_TABLE, args(0))

    val admin = new HBaseAdmin(conf)
    if (admin.isTableAvailable(args(0))) {
      val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result]).map((rdd:(ImmutableBytesWritable,Result)) => rdd._2)
      val filterRDD = hBaseRDD.filter(result=>new String(result.getValue("meta".getBytes(),"state".getBytes())).equals("1"))
      val byteRDD = filterRDD.map(result=>(new String(result.getRow),result.getValue("data".getBytes,"zips".getBytes)))
      val imgRDD = byteRDD.map(x=>tofile(x,args(2)))
      val result= imgRDD.map(name=>algorithm(args(2),name))
      storage.saveState(result)
    }

    log.info("algorithm model save hdfs path:"+args(6))
    saveHdfs(args(3),args(4),args(6),"/Model")
    saveHdfs(args(3),args(5),args(6),"/wiMapping")
    sc.stop()
  }



  def algorithm(path :String,name:String): (String,String) = {
    var arg = ParseXML.getArgs(path + "/unzip/" + name + "/")
    val updateResult = Algorithm.updateModel(arg)
    val searchUpdate = Algorithm.updateSearchModelAdd(arg)
    FileUtils.deleteQuietly(new File(path + "/unzip/" + name + "/"))
    if (updateResult != null&&searchUpdate != null&&updateResult.equals("0")&&searchUpdate.equals("0")) {
      (name, "3")
    } else {
      (name,"2")
    }
  }

  def tofile(bean:(String,Array[Byte]),parentPath:String): String ={
    val id = bean._1
    val array=bean._2
    val path = parentPath + "/zip/"+id+".zip"

    log.info("=========="+path)
    FileUtils.writeByteArrayToFile(new File(path),array)
    val zipFile = new ZipFile(path)
    zipFile.extractAll(parentPath + "/unzip/")
    FileUtils.deleteQuietly(new File(path))
    id
  }

  def saveHdfs(master:String,srcPath:String,desPath:String,modelName:String): Unit ={
    log.info("save model begin")
    val zipFile = new ZipFile(srcPath + modelName+".zip")
    val folderToAdd = srcPath+modelName
    val parameters = new ZipParameters()
    parameters.setCompressionMethod(Zip4jConstants.COMP_DEFLATE)
    parameters.setCompressionLevel(Zip4jConstants.DEFLATE_LEVEL_NORMAL)
    zipFile.addFolder(folderToAdd, parameters)
    val conf = new Configuration()
    conf.set("fs.defaultFS",master)
    val fileSystem = FileSystem.get(conf)
    fileSystem.copyFromLocalFile(false,true,new Path("file:///"+srcPath + modelName+".zip"),new Path(desPath+modelName+".zip"))
    FileUtils.deleteQuietly(new File(srcPath + modelName+".zip"))
    log.info("save model end")
  }
}
