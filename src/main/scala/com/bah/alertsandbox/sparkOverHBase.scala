package com.bah.alertsandbox

import org.apache.spark._
import org.apache.spark.rdd.NewHadoopRDD
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor}
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.util.Bytes

object sparkOverHBase {

  def main(args: Array[String]) {

    val tableName = "user"

    val skconf = new SparkConf().setAppName("sparkOverHBase").setMaster("local[2]").set("spark.executor.memory","1g")
    skconf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    skconf.registerKryoClasses(Array(classOf[org.apache.hadoop.hbase.client.Result]))
    val sc = new SparkContext(skconf)

    val conf = HBaseConfiguration.create()
    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    conf.set("zookeeper.znode.parent","/hbase-unsecure")
    // Initialize hBase table if necessary
    val admin = new HBaseAdmin(conf)
    if(!admin.isTableAvailable("user")) {
      val tableDesc = new HTableDescriptor(tableName)
      admin.createTable(tableDesc)
    }
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val count = hBaseRDD.count()
    println("__________________________HBase RDD Count_________________________________:" + count)

    hBaseRDD.collect().foreach{ case (_,result) =>
      val key = Bytes.toString(result.getRow)
      val name = Bytes.toString(result.getValue("app_user".getBytes,"userName".getBytes))
      val age = Bytes.toString(result.getValue("app_user".getBytes,"userID".getBytes))
      println("Row key: " + key + "    app_user(  userName :  " + name +"    userID:  " + age +"  )")
    }

    val userTable = new HTable(conf, tableName)
    var userRecord1 = new Put(new String("009karen").getBytes());
    userRecord1.add("app_user".getBytes(), "userName".getBytes(), new String("Christin").getBytes());
    userTable.put(userRecord1);

    userTable.flushCommits();
    
    sc.stop()
    System.exit(0)
  }
}
