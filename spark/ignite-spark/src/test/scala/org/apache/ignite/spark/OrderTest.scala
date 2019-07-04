package org.apache.ignite.spark

import java.sql.Timestamp

import entity.StkOrder
import org.apache.ignite.Ignition
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Encoders, SaveMode, SparkSession}
import org.scalatest.{BeforeAndAfter, FunSuite}

import scala.util.Random

class OrderTest extends FunSuite with BeforeAndAfter{

  var CONFIG = "D:\\users\\tangp\\Documents\\idea\\big-data\\spark\\ignite-spark\\src\\test\\resources\\example-shared-object-rdd.xml"

  CONFIG = "D:\\users\\tangp\\Documents\\idea\\big-data\\spark\\ignite-spark\\src\\test\\resources\\example-rdd.xml"
  CONFIG = "D:\\users\\tangp\\Documents\\idea\\big-data\\spark\\ignite-spark\\src\\test\\resources\\stk_order.xml"
  val tableName = "STK_OLD_ORDER"

  val conf = new SparkConf()
    .setAppName("IgniteRDDExample")
    .setMaster("local[2]")
    .set("spark.sql.shuffle.partitions","35")
    .set("spark.executor.instances", "2")


  // Spark context.
  val sparkSession:SparkSession= SparkSession.builder().config(conf).getOrCreate()



  after{
    sparkSession.close()
    Ignition.stop(false)
  }

  test("rand"){
    println(Random.nextInt(10))
    println(Random.nextInt(100))
  }
  def generateData(dataSum:Int): DataFrame ={
    import sparkSession.implicits._
    val df = sparkSession.sparkContext.parallelize(1 to dataSum,36).map(i=>{
      StkOrder(20180101,i,new Timestamp(System.currentTimeMillis()),i,i*3,s"order_id_${i}",s"raw_order_id_${i}","0","0"
        , Random.nextInt(i),"subsys_sn",Random.nextInt(i),s"cust_code_${i}",s"cust_name_${i}","1","2",s"cuacct_code_${i}"
        ,"3","4","5","6","7",Random.nextLong(),"m","S",s"stkpbu_${i}",s"firmid_${i}",s"trdacct_${i}",s"trdacct_exid_${i}",
        "T","e","1",s"stk_biz_ex_${i}",s"stk_code_${i}",s"stk_name_${i}","c","c",Random.nextInt(i),BigDecimal(Random.nextInt(1))
        ,Random.nextLong(),Random.nextLong(),Random.nextLong(),Random.nextLong(),Random.nextLong(),Random.nextLong(),Random.nextLong(),
        Random.nextLong(),"1","i",s"op_user_${i}","o",s"op_name_${i}",Random.nextLong(),s"op_site_${i}","c",new Timestamp(System.currentTimeMillis())
        ,s"metting_code_${i}",s"vote_id_${i}","c",Random.nextInt(),s"rptpbu_${i}")
    }).toDF()
    df
  }

  test("insert 1"){
   insert(1000000,"stk_order1","stk_order1")
     //insert(5000000,"stk_order2","stk_order2")
  }

  test(" none-Collocated  join  "){
    //非并置连接
    import sparkSession.implicits._

    val order1DF =  sparkSession.sparkContext.parallelize(1 to 4000000,36).map(i=>{
      StkOrder(20180101,i,new Timestamp(System.currentTimeMillis()),i,i*3,s"order_id_${i}",s"raw_order_id_${i}","0","0"
        , Random.nextInt(i),"subsys_sn",Random.nextInt(i),s"cust_code_${i}",s"cust_name_${i}","1","2",s"cuacct_code_${i}"
        ,"3","4","5","6","7",Random.nextLong(),"m","S",s"stkpbu_${i}",s"firmid_${i}",s"trdacct_${i}",s"trdacct_exid_${i}",
        "T","e","1",s"stk_biz_ex_${i}",s"stk_code_${i}",s"stk_name_${i}","c","c",Random.nextInt(i),BigDecimal(Random.nextInt(1))
        ,Random.nextLong(),Random.nextLong(),Random.nextLong(),Random.nextLong(),Random.nextLong(),Random.nextLong(),Random.nextLong(),
        Random.nextLong(),"1","i",s"op_user_${i}","o",s"op_name_${i}",Random.nextLong(),s"op_site_${i}","c",new Timestamp(System.currentTimeMillis())
        ,s"metting_code_${i}",s"vote_id_${i}","c",Random.nextInt(),s"rptpbu_${i}")
    }).toDF()

    order1DF.createOrReplaceTempView("stk_order1")

    val order2DF = sparkSession.read.format(IgniteDataFrameSettings.FORMAT_IGNITE)
      .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE, CONFIG).option(IgniteDataFrameSettings.OPTION_TABLE, "stk_order2").load
    order2DF.createOrReplaceTempView("stk_order2")
    val resultDF = sparkSession.sql("select s1.* from stk_order1 s1 INNER JOIN stk_order2 s2 ON " +
      " s1.SUBSYS_SN= s2.SUBSYS_SN and s1.TRD_DATE= s2.TRD_DATE and s1.ORDER_SN=s2.ORDER_SN ")

    println(resultDF.queryExecution)
    val p =  resultDF.rdd.mapPartitionsWithIndex((index, ite) => {
      List( Tuple2(index,ite.size) ).iterator
    }).collect()

    println(s"order1 df pn:${order1DF.rdd.partitions.size}; order2 df pn: ${order2DF.rdd.partitions.size}" +
      s"; result df pn:${resultDF.rdd.partitions.size}; parition:${p.mkString(", ")}")
    //order1 df pn:36; order2 df pn: 2; result df pn:35;
    // parition:(0,113824), (1,114200), (2,114329), (3,114784),
    // (4,113684), (5,113641), (6,114237), (7,114294), (8,114644),
    // (9,114479), (10,114199), (11,114419),
    // (12,114114), (13,114159), (14,114492), (15,114160), (16,114250), (17,114475), (18,114659), (19,114377), (20,114087), (21,114319), (22,114544), (23,114097), (24,114875), (25,114395), (26,113981), (27,114078), (28,114327), (29,114183), (30,114561), (31,114342), (32,114194), (33,114490), (34,114107)
  }
  test("Collocated  join"){
//并置连接
    val order1DF = sparkSession.read.format(IgniteDataFrameSettings.FORMAT_IGNITE)
      .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE, CONFIG).option(IgniteDataFrameSettings.OPTION_TABLE, "stk_order1").load
    order1DF.createOrReplaceTempView("stk_order1")

    val order2DF = sparkSession.read.format(IgniteDataFrameSettings.FORMAT_IGNITE)
      .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE, CONFIG).option(IgniteDataFrameSettings.OPTION_TABLE, "stk_order2").load
    order2DF.createOrReplaceTempView("stk_order2")
//SUBSYS_SN,TRD_DATE,ORDER_SN
    val resultDF = sparkSession.sql("select s1.* from stk_order1 s1 INNER JOIN stk_order2 s2 ON " +
      " s1.SUBSYS_SN= s2.SUBSYS_SN and s1.TRD_DATE= s2.TRD_DATE and s1.ORDER_SN=s2.ORDER_SN ")


   val p =  resultDF.rdd.mapPartitionsWithIndex((index, ite) => {
     List( Tuple2(index,ite.size) ).iterator
    }).collect()
    println(resultDF.queryExecution)
    println(s"order1 df pn:${order1DF.rdd.partitions.size}; order2 df pn: ${order2DF.rdd.partitions.size}" +
      s"; result df pn:${resultDF.rdd.partitions.size}; parition:${p.mkString(", ")}")
//order1 df pn:2; order2 df pn: 2; result df pn:1; parition:(0,4000000)
    //Thread.sleep(100000)


  }

  def insert(dataSum:Int,cacheName:String,tableName:String){

    import sparkSession.implicits._

    val start = System.currentTimeMillis()
    val df = sparkSession.sparkContext.parallelize(1 to dataSum,36).map(i=>{
      StkOrder(20180101,i,new Timestamp(System.currentTimeMillis()),i,i*3,s"order_id_${i}",s"raw_order_id_${i}","0","0"
        , Random.nextInt(i),"subsys_sn",Random.nextInt(i),s"cust_code_${i}",s"cust_name_${i}","1","2",s"cuacct_code_${i}"
        ,"3","4","5","6","7",Random.nextLong(),"m","S",s"stkpbu_${i}",s"firmid_${i}",s"trdacct_${i}",s"trdacct_exid_${i}",
        "T","e","1",s"stk_biz_ex_${i}",s"stk_code_${i}",s"stk_name_${i}","c","c",Random.nextInt(i),BigDecimal(Random.nextInt(1))
        ,Random.nextLong(),Random.nextLong(),Random.nextLong(),Random.nextLong(),Random.nextLong(),Random.nextLong(),Random.nextLong(),
        Random.nextLong(),"1","i",s"op_user_${i}","o",s"op_name_${i}",Random.nextLong(),s"op_site_${i}","c",new Timestamp(System.currentTimeMillis())
        ,s"metting_code_${i}",s"vote_id_${i}","c",Random.nextInt(),s"rptpbu_${i}")
    }).toDF()

    val mid = System.currentTimeMillis()
    //如果要Collocated Joins并置连接 要设置AFFINITY_KEY
    //AFFINITY_KEY=<affinity key column name>：设置类同键名字，它应该是PRIMARY KEY约束中的一个列；
    df.write.mode(SaveMode.Overwrite)
      .format(IgniteDataFrameSettings.FORMAT_IGNITE)
      .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE, CONFIG)
      .option(IgniteDataFrameSettings.OPTION_TABLE, tableName)
      .option(IgniteDataFrameSettings.OPTION_STREAMER_ALLOW_OVERWRITE,"true")
      .option(IgniteDataFrameSettings.OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS, "SUBSYS_SN,TRD_DATE,ORDER_SN")
      .option(IgniteDataFrameSettings.OPTION_CREATE_TABLE_PARAMETERS, s"template=partitioned,CACHE_NAME='${cacheName}',AFFINITY_KEY=ORDER_SN,DATA_REGION=9GRegion")
      .save()
    println(s"data:${dataSum},parititon num:${df.rdd.partitions.size}; p1:${(mid-start)/1000}s; p2:${(System.currentTimeMillis()-mid)/1000}s")



  }



  test("load df"){


 val encoder = Encoders.bean(classOf[StkOrder])
    val orderDF = sparkSession.read.format(IgniteDataFrameSettings.FORMAT_IGNITE)
      .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE, CONFIG).option(IgniteDataFrameSettings.OPTION_TABLE, tableName)
      .load
     // .as[StkOrder](encoder)
      .toDF()
    println("------------------------"+orderDF.rdd.partitions.size)
    //orderDF.show()
    /*val  newDF = orderDF.coalesce(1)

    orderDF.createOrReplaceTempView("t1")
    newDF.createOrReplaceTempView("t2")
    val result = sparkSession.sql("select t1.* from t1 inner join t2 on t1.INT_ORG= t2.INT_ORG")
    println(s"ori partition num:${orderDF.rdd.partitions.size}; newDF partition num:${newDF.rdd.partitions.size}; partition num:${result.rdd.partitions.size}, counter:${result.count()}")
*/  }

  test("join"){
    val order2Df = sparkSession.read.format(IgniteDataFrameSettings.FORMAT_IGNITE)
      .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE, CONFIG).option(IgniteDataFrameSettings.OPTION_TABLE, "").load
  }
  test("load"){
    val start = System.currentTimeMillis()

    val orderDF = sparkSession.read.format(IgniteDataFrameSettings.FORMAT_IGNITE)
      .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE, CONFIG).option(IgniteDataFrameSettings.OPTION_TABLE, tableName).load

  //  orderDF.show()
  val mid = System.currentTimeMillis()

    orderDF.createOrReplaceTempView("stk_order")
    //val newdf = sparkSession.sql("select * from stk_order where  REC_SN > 4000000 ")//第一批100w数据
    //val newdf = sparkSession.sql("select * from stk_order where REC_SN >= 3000000 and  REC_SN < 4000000 ")//第二批100w数据
    //val newdf = sparkSession.sql("select * from stk_order where REC_SN >= 3000000 ")//访问前两批数据，200w
    val newdf = sparkSession.sql("select * from stk_order  ")//访问500w数据
    newdf.foreachPartition(i=>{
      print()
    })

    //  newdf.show()
 /*   orderDF.foreachPartition(i=>{
      print()
    })*/
    println(s" orderDF partition num:${orderDF.rdd.partitions.size}; newDF partition num:${newdf.rdd.partitions.size}; p1:${(mid-start)/1000}s; p2:${(System.currentTimeMillis()-mid)/1000}s")

    //Thread.sleep(100000)

  }
}
