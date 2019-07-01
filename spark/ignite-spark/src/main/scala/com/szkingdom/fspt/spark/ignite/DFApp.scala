package com.szkingdom.fspt.spark.ignite

import java.sql.Timestamp

import entity.StkOrder
import org.apache.ignite.Ignition
import org.apache.ignite.spark.IgniteDataFrameSettings
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.LoggerFactory

import scala.util.Random

/**
  */
object DFApp {
  val logger = LoggerFactory.getLogger(getClass)
  def main(args: Array[String]): Unit = {

    val dealType = args(0)
    val appName = args(1)
    val CONFIG = args(2)
    val sparkSession =  getOrcreateSparkContxt(appName)


    dealType match {
      case "insert"=>{
        val cacheName = args(3)
        val tableName = args(4)
        val dataCount = args(5).toInt
        val numSlices = args(6).toInt
        println(s"appName:${appName},CONFIG:${CONFIG},cacheName${cacheName},tableName:${tableName},dataCount:${dataCount},numSlices:${numSlices}")

        insert(sparkSession, CONFIG, dataCount, numSlices, cacheName, tableName)

      }
      case "cjoin"=>{
        //cjoin cjoin   stk_order1 stk_order2
        val mainTable = args(3)//主表
        val secTable = args(4)
        println(s"appName:${appName},CONFIG:${CONFIG},mainTable:${mainTable},secTable:${secTable}")
        collocatedJoin(sparkSession ,CONFIG,mainTable,secTable)

      }
      case _=>
        println(s"未定义该处理类型：${dealType}")
    }

    sparkSession.close()
    Ignition.stop(false)



  }


  def collocatedJoin(sparkSession: SparkSession, CONFIG:String, mainTable:String,secTable:String): Unit ={
    val order1DF = sparkSession.read.format(IgniteDataFrameSettings.FORMAT_IGNITE)
      .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE, CONFIG).option(IgniteDataFrameSettings.OPTION_TABLE, mainTable).load
    order1DF.createOrReplaceTempView(mainTable)

    val order2DF = sparkSession.read.format(IgniteDataFrameSettings.FORMAT_IGNITE)
      .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE, CONFIG).option(IgniteDataFrameSettings.OPTION_TABLE, secTable).load
    order2DF.createOrReplaceTempView(secTable)
    //SUBSYS_SN,TRD_DATE,ORDER_SN
    val resultDF = sparkSession.sql(s"select s1.* from ${mainTable} s1 INNER JOIN ${secTable} s2 ON " +
      " s1.ORDER_SN=s2.ORDER_SN ")

    //resultDF.collect()

    resultDF.foreachPartition(i=>{
      i.foreach(v=>{

      })
    })
  /*  val p =  resultDF.rdd.mapPartitionsWithIndex((index, ite) => {

      List( Tuple2(index,ite.size) ).iterator
    }).collect()
    println(resultDF.queryExecution)
    println(s"order1 df pn:${order1DF.rdd.partitions.size}; order2 df pn: ${order2DF.rdd.partitions.size}" +
      s"; result df pn:${resultDF.rdd.partitions.size}; parition:${p.mkString(", ")}")*/
  }
  def insert(sparkSession: SparkSession, CONFIG: String, dataCount:Int,numSlices :Int, cacheName:String, tableName:String){

    import sparkSession.implicits._

  //  val start = System.currentTimeMillis()
    val df = sparkSession.sparkContext.parallelize(1 to dataCount,numSlices).map(i=>{
      StkOrder(20180101,i,new Timestamp(System.currentTimeMillis()),i,i*3,s"order_id_${i}",s"raw_order_id_${i}","0","0"
        , Random.nextInt(i),"subsys_sn",Random.nextInt(i),s"cust_code_${i}",s"cust_name_${i}","1","2",s"cuacct_code_${i}"
        ,"3","4","5","6","7",Random.nextLong(),"m","S",s"stkpbu_${i}",s"firmid_${i}",s"trdacct_${i}",s"trdacct_exid_${i}",
        "T","e","1",s"stk_biz_ex_${i}",s"stk_code_${i}",s"stk_name_${i}","c","c",Random.nextInt(i),BigDecimal(Random.nextInt(1))
        ,Random.nextLong(),Random.nextLong(),Random.nextLong(),Random.nextLong(),Random.nextLong(),Random.nextLong(),Random.nextLong(),
        Random.nextLong(),"1","i",s"op_user_${i}","o",s"op_name_${i}",Random.nextLong(),s"op_site_${i}","c",new Timestamp(System.currentTimeMillis())
        ,s"metting_code_${i}",s"vote_id_${i}","c",Random.nextInt(),s"rptpbu_${i}")
    }).toDF()

    //val mid = System.currentTimeMillis()
    //如果要Collocated Joins并置连接 要设置AFFINITY_KEY
    //AFFINITY_KEY=<affinity key column name>：设置类同键名字，它应该是PRIMARY KEY约束中的一个列；
    df.write.mode(SaveMode.Overwrite)
      .format(IgniteDataFrameSettings.FORMAT_IGNITE)
      .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE, CONFIG)
      .option(IgniteDataFrameSettings.OPTION_TABLE, tableName)
      .option(IgniteDataFrameSettings.OPTION_STREAMER_ALLOW_OVERWRITE,"true")
      .option(IgniteDataFrameSettings.OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS, "SUBSYS_SN,TRD_DATE,ORDER_SN")
      .option(IgniteDataFrameSettings.OPTION_CREATE_TABLE_PARAMETERS, s"template=partitioned,CACHE_NAME='${cacheName}',AFFINITY_KEY=ORDER_SN,DATA_REGION=12GRegion")
      .save()
  //  println(s"data:${dataCount},parititon num:${df.rdd.partitions.size}; p1:${(mid-start)/1000}s; p2:${(System.currentTimeMillis()-mid)/1000}s")



  }
}
