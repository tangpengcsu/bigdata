package org.apache.ignite.spark

import org.apache.hadoop.io.{IntWritable, NullWritable, Text}
import org.apache.hadoop.mapred.TextOutputFormat
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FunSuite}

class Test extends FunSuite with BeforeAndAfter{

  var sparkSession:SparkSession =_
  before{
    // Spark Configuration.
    val conf = new SparkConf()
      .setAppName("IgniteRDDExample")
      .setMaster("local[2]")
      .set("spark.executor.instances", "2")
    // Spark context.
    sparkSession = SparkSession.builder().config(conf).getOrCreate()

  }
  after{

  }


  test("cogrp"){
    val idName = sparkSession.sparkContext.parallelize(Array((1, "zhangsan"), (2, "lisi"), (3, "wangwu")),10)


    val idAge = sparkSession.sparkContext.parallelize(Array((1, 30), (2, 29), (4, 21)),12)
    val result = idName.cogroup(idAge)
    result.foreach(i=>println(i));
    println(s"result.partition:${result.partitions.size}")


  }

  test("insert data"){
    val DEFAULT_SEPARATOR:Char = 9
    println(DEFAULT_SEPARATOR)


    val arr:Array[String] = Array("480","481","482","483","484")


    val step = 1000000
    val totalNum = step*arr.length
    var v = 0
    for(num <- 0 until (5000000, 1000000)){
      val or = sparkSession.sparkContext.parallelize(num until  num+1000000,36)
      val no = arr(v)

      val data = or.map(i=>{
        val arr = Array("20180905","1",no,"BIZ_CODE_EX"
          ,"1",i.toString,"2","QSBH","ZQZH","XWH","ZQDM"+i,"3","4","5","GPNF","BDSL","BDLX-张三","BDRQ","SL","BH","BYZQ")
        arr.mkString(DEFAULT_SEPARATOR.toString())
      })
      val path =s"/FSPT/CLEARING_DATABASE/DATA/20180905/SETT_FILE/SH_ZQBD/${no}"
    //  val path =s"hdfs://192.168.50.88:9000/FSPT/CLEARING_DATABASE/DATA/20180905/SETT_FILE/SH_ZQBD/${no}"

    /*  var rdd1 = sparkSession.sparkContext.makeRDD(Array(("A",2),("A",1),("B",6),("B",3),("B",7)),2)
      rdd1.saveAsHadoopFile("hdfs://leen:8020/test01",classOf[Text],classOf[IntWritable],classOf[TextOutputFormat[Text,IntWritable]])*/
/*      var rdd1 = sparkSession.sparkContext.makeRDD(Array(("A",2),("A",1),("B",6),("B",3),("B",7)),2)

      rdd1.saveAsNewAPIHadoopFile("hdfs://leen:8020/test03",classOf[Text],classOf[IntWritable],classOf[TextOutputFormat[Text,IntWritable]])*/

      //gbk
     val d= data.map{
        item=>(NullWritable.get(),new Text(item.getBytes("GBK")))
      }

       d.saveAsHadoopFile( path,classOf[NullWritable],classOf[Text],classOf[TextOutputFormat[NullWritable, Text]])
//utf8
    //  data.saveAsTextFile(s"hdfs://192.168.50.88:9000/FSPT/CLEARING_DATABASE/DATA/20180905/SETT_FILE/SH_ZQBD/${no}")
      v = v+1
    }



  }

   test("xxx"){
     val or = sparkSession.sparkContext.parallelize(0 until  1000,2)
     val data = or.map(i=>{
       val arr = Array(s"张三${i}","1里斯",i)
       val DEFAULT_SEPARATOR:Char = 9
       arr.mkString(DEFAULT_SEPARATOR.toString())
     })
    // data.saveAsTextFile(s"/FSPT/CLEARING_DATABASE/DATA/test")

     val d= data.map{
       item=>(NullWritable.get(),new Text(item.getBytes("GBK")))
     }

     d.saveAsHadoopFile( s"/FSPT/CLEARING_DATABASE/DATA/testgbk",classOf[NullWritable],classOf[Text],classOf[TextOutputFormat[NullWritable, Text]])
  }
}
