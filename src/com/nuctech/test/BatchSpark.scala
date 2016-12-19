package com.nuctech.test

import org.apache.hadoop.hbase.client.{HBaseAdmin, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Administrator on 2015/8/7.
 */
object BatchSpark {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("BatchSpark")
    sparkConf.setMaster("local")


    val sc = new SparkContext(sparkConf)
    val conf = HBaseConfiguration.create()
    // Other options for configuring scan behavior are available. More information available at
    // http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/mapreduce/TableInputFormat.html
    conf.set(TableInputFormat.INPUT_TABLE, "image")
    conf.set("hbase.master", "master.node:60000")
    conf.set("hbase.zookeeper.quorum", "master.node")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("zookeeper.znode.parent", "/hbase-unsecure")

    // Initialize hBase table if necessary
    val admin = new HBaseAdmin(conf)
    if (!admin.isTableAvailable("result")) {
      val tableDesc = new HTableDescriptor(TableName.valueOf(args(0)))
      tableDesc.addFamily(new HColumnDescriptor("meta"))
      admin.createTable(tableDesc)
    }

    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result]).map((test:(ImmutableBytesWritable,Result)) => test._2)
    println(hBaseRDD.count())
    hBaseRDD.foreach((arg:Result) => println(new String(arg.getValue("meta".getBytes,"1".getBytes))))
    sc.stop()
  }
}
