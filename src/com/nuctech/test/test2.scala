package com.nuctech.test

/**
 * Created by Administrator on 2015/8/25.
 */

import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

object test2 {
  def main(args : Array[String]) {
    val sparkConf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(sparkConf);
    var rdd1 = sc.makeRDD(Array((10,"A",2),(11,"B",6),(12,"C",7)))

    sc.hadoopConfiguration.set("hbase.zookeeper.quorum ","master.node")
    sc.hadoopConfiguration.set("zookeeper.znode.parent","/hbase-unsecure")
    sc.hadoopConfiguration.set("hbase.zookeeper.property.clientPort","2181")
    sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE,"test2")
    var job = Job.getInstance(sc.hadoopConfiguration)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    rdd1.map(
      x => {
        var put = new Put(Bytes.toBytes(x._1))
        put.add(Bytes.toBytes("meta"),Bytes.toBytes("name"),Bytes.toBytes(x._2))
        put.add(Bytes.toBytes("meta"),Bytes.toBytes("age"),Bytes.toBytes(x._3))
        (new ImmutableBytesWritable,put)
      }
    ).saveAsNewAPIHadoopDataset(job.getConfiguration)

    sc.stop()
  }
}
