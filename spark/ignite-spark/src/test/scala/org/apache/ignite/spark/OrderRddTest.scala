package org.apache.ignite.spark

import java.sql.Timestamp

import entity.{StkOrder, StkOrderKey}
import org.apache.ignite.IgniteSystemProperties
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FunSuite}

import scala.util.Random

class OrderRddTest  extends  FunSuite with BeforeAndAfter{
  val CONFIG = "D:\\users\\tangp\\Documents\\idea\\big-data\\spark\\ignite-spark\\src\\test\\resources\\stk_order.xml"
  var sparkContext: SparkContext =  _
  var igniteContext :IgniteContext= _
  System.setProperty(IgniteSystemProperties.IGNITE_UPDATE_NOTIFIER, "false")
  before{
    // Spark Configuration.
    val conf = new SparkConf()
      .setAppName("IgniteRDDExample")
      .setMaster("local[2]")
      .set("spark.executor.instances", "2")

    // Spark context.
    sparkContext = new SparkContext(conf)

    // Adjust the logger to exclude the logs of no interest.
    Logger.getRootLogger.setLevel(Level.ERROR)
    Logger.getLogger("org.apache.ignite").setLevel(Level.INFO)


    // Defines spring cache Configuration path.

    // Creates Ignite context with above configuration.
    igniteContext = new IgniteContext(sparkContext, CONFIG)
  }

  test("insert"){
    //SUBSYS_SN,TRD_DATE,ORDER_SN
    val stkOrder11RDD: IgniteRDD[StkOrderKey, StkOrder] = igniteContext.fromCache[StkOrderKey, StkOrder]("stk_order11")
   stkOrder11RDD.savePairs(genData(5000000))
    val stkOrder12RDD: IgniteRDD[StkOrderKey, StkOrder] = igniteContext.fromCache[StkOrderKey, StkOrder]("stk_order12")
    stkOrder12RDD.savePairs(genData(4000000))
  }

  test("sql"){
    val stkOrder12RDD: IgniteRDD[StkOrderKey, StkOrder] = igniteContext.fromCache[StkOrderKey, StkOrder]("stk_order12")
    val resultDf = stkOrder12RDD.sql("select * from STK_ORDER12   ")

    resultDf.foreachPartition(i=>{
      i.foreach(v=>{
        print()
      })
    })
    resultDf.printSchema()
    println(resultDf.rdd.partitions.size)
    println(resultDf.queryExecution)

  }
  test("join"){
    val stkOrder11RDD: IgniteRDD[StkOrderKey, StkOrder] = igniteContext.fromCache[StkOrderKey, StkOrder]("stk_order11")
    val stkOrder11Df = stkOrder11RDD.sql("select * from STK_ORDER11   ")

    val stkOrder12RDD: IgniteRDD[StkOrderKey, StkOrder] = igniteContext.fromCache[StkOrderKey, StkOrder]("stk_order12")
    val stkOrder12Df = stkOrder12RDD.sql("select * from STK_ORDER12   ")

    stkOrder11Df.createOrReplaceTempView("stk_order1")
    stkOrder12Df.createOrReplaceTempView("stk_order2")

    val spark = stkOrder11Df.sparkSession

    val resultDF = spark.sql("select s1.* from stk_order1 s1 INNER JOIN stk_order2 s2 ON " +
      " s1.SUBSYS_SN= s2.SUBSYS_SN and s1.TRD_DATE= s2.TRD_DATE and s1.ORDER_SN=s2.ORDER_SN ")
    println(resultDF.queryExecution)
    val p =  resultDF.rdd.mapPartitionsWithIndex((index, ite) => {
      List( Tuple2(index,ite.size) ).iterator
    }).collect()
    println(s"order11 pn:${stkOrder11Df.rdd.partitions.size}; " +
      s"order12 pn:${stkOrder12Df.rdd.partitions.size}; " +
      s"result:${resultDF.rdd.partitions.size}; " +
      s" result df pn:${resultDF.rdd.partitions.size}; parition:${p.mkString(", ")}")

  }

  def genData(dataSum:Int): RDD[(StkOrderKey,StkOrder)] ={
  val z=   sparkContext.parallelize(1 to dataSum,36).map(i=>{
      val key=StkOrderKey(20180101,i,"subsys_sn")
        val value =  StkOrder(20180101,i,new Timestamp(System.currentTimeMillis()),i,i*3,s"order_id_${i}",s"raw_order_id_${i}","0","0"
          , Random.nextInt(i),"subsys_sn",Random.nextInt(i),s"cust_code_${i}",s"cust_name_${i}","1","2",s"cuacct_code_${i}"
          ,"3","4","5","6","7",Random.nextLong(),"m","S",s"stkpbu_${i}",s"firmid_${i}",s"trdacct_${i}",s"trdacct_exid_${i}",
          "T","e","1",s"stk_biz_ex_${i}",s"stk_code_${i}",s"stk_name_${i}","c","c",Random.nextInt(i),BigDecimal(Random.nextInt(1))
          ,Random.nextLong(),Random.nextLong(),Random.nextLong(),Random.nextLong(),Random.nextLong(),Random.nextLong(),Random.nextLong(),
          Random.nextLong(),"1","i",s"op_user_${i}","o",s"op_name_${i}",Random.nextLong(),s"op_site_${i}","c",new Timestamp(System.currentTimeMillis())
          ,s"metting_code_${i}",s"vote_id_${i}","c",Random.nextInt(),s"rptpbu_${i}")
      (key,value)
    })
    z
  }

  after{
    igniteContext.close(true)
    sparkContext.stop()
  }
}
