package org.apache.ignite.spark

import java.nio.charset.Charset

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class DBFTest extends FunSuite{

  val conf = new SparkConf()
    .setAppName("IgniteRDDExample")
    .setMaster("local[2]")
    .set("spark.executor.instances", "2")
  var path = "file:///D://SJSJG.DBF"

  val partitionNum = 17
  val charset = Charset.forName("GBK")
  // Spark context.
  val sparkSession:SparkSession= SparkSession.builder().config(conf).getOrCreate()

  test("read"){
    // path = "file:///D://jsmx13.dbf"
    val filePath = "file:///D://"
    //path = filePath+"0904机构费用明细.dbf"
   // path = filePath+"0904交收后成交汇总表.DBF"
   // path = filePath+"0904交易一级清算表.DBF"
    //path = filePath+"0904保证金日结表.DBF"
    //path = filePath+"0904保证金日结表.DBF"
    import com.szkingdom.fspt.dao.file.spark._

    val s = sparkSession.sparkContext.loadAsRowRDD(path,charset,partitionNum)
    /*s.foreach(i=>{
      println(i)
    })*/
    println(s"=====sum:${s.count()}")
  }
}
