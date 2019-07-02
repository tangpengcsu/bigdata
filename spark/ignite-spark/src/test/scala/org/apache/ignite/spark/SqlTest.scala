package org.apache.ignite.spark

import com.szkingdom.fspt.spark.ignite.test.{Person, PersonKey}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.apache.ignite.spark._

class SqlTest extends FunSuite with BeforeAndAfter {

  var sparkContext: SparkContext = _
  var igniteContext: IgniteContext = _
  before {
    // Spark Configuration.
    val conf = new SparkConf()
      .setAppName("IgniteRDDExample")
      .setMaster("local[2]")
      .set("spark.executor.instances", "2")
    conf.set("fs.defaultFS", "hdfs://192.168.50.88:9000")
    sparkContext = new SparkContext(conf)

    Logger.getRootLogger.setLevel(Level.ERROR)
    Logger.getLogger("org.apache.ignite").setLevel(Level.INFO)

    var CONFIG = "D:\\users\\tangp\\Documents\\idea\\big-data\\spark\\ignite-spark\\src\\test\\resources\\example-shared-object-rdd.xml"
    /*  CONFIG = "hdfs://192.168.50.88:9000/FSPT/example-shared-object-rdd.xml"*/

    igniteContext = new IgniteContext(sparkContext, CONFIG, false)
  }
  after {
    igniteContext.close(true)
    sparkContext.stop()


  }

  test("insert") {
    val sharedRDD: IgniteRDD[PersonKey, Person] = igniteContext.fromCache[PersonKey, Person]("objectRDD")
    println("oragin-size:" + sharedRDD.partitions.size)
    sharedRDD.savePairs(sparkContext.parallelize(1 to 10000, 72).map(i => (new PersonKey(i + "name", i.toString), new Person(i + "name", i.toString, i + "addr"))))
  }
test("jfdjflakj"){
  val transformedValues: IgniteRDD[PersonKey, Person] = igniteContext.fromCache("objectRDD")
  transformedValues.take(100).foreach(i=>{
    println(s"key:${i._1}; value:${i._2}")
  })
  val filterRdd = transformedValues.filter(_._2.getId=="100")//tran partitionNum:36,tran count:1000000; filter.rdd.parttitionNum:36;filter.rdd.count:1

  val filterCollect =  filterRdd.collect()
  println(s"tran partitionNum:${transformedValues.partitions.size},tran count:${transformedValues.count()}; filter.rdd.parttitionNum:${filterRdd.partitions.size};filter.rdd.count:${filterCollect.length}")

}
  test("query object") {
    val transformedValues: IgniteRDD[PersonKey, Person] = igniteContext.fromCache("objectRDD")


    val values1 = transformedValues.sql("select * from Person  ", true)

    println(s"partitionNum:${values1.rdd.partitions.size},size:${values1.count()}") //partitionNum:1,size:1649855
  //  values1.show(10)
/*    values1.foreachPartition(i => {
      println(s"partition record size:${i.size}")
    })*/

  }


  test("sql mod "){
    val transformedValues: IgniteRDD[PersonKey, Person] = igniteContext.fromCache("objectRDD")
    val values1 = transformedValues.sqlModTemplate("select name,id,address from Person where mod(id,?)=?  and id = '100022'")
    println(s"partitionNum:${values1.rdd.partitions.size},size:${values1.count()}") //partitionNum:36,size:1649855
    val sizeRdd = values1.rdd.mapPartitions(r => {
      List(r.size).iterator
    })
    val sumRdd = sizeRdd.collect()
    println(s"分区数：${sizeRdd.count()}，总记录数:${sumRdd.sum},各个分区记录数：${sizeRdd.collect().mkString(", ")}")

  }
  test("query object  partition index") {
    val transformedValues: IgniteRDD[PersonKey, Person] = igniteContext.fromCache("objectRDD")
    //val values1 = transformedValues.partitionSql("select name,id,address from Person  ")
    val values1 = transformedValues.sqlTemplate("select name,id,address from Person ")

    println(s"partitionNum:${values1.rdd.partitions.size},size:${values1.count()}") //partitionNum:36,size:1649855
     values1.show(10)

    val sizeRdd = values1.rdd.mapPartitions(r => {
      List(r.size).iterator
    })
    val sumRdd = sizeRdd.collect()
    println(s"分区数：${sizeRdd.count()}，总记录数:${sumRdd.sum},各个分区记录数：${sizeRdd.collect().mkString(", ")}")
    // ignite 分区数据不均匀
    //qry.setPartitions(idx)
    //分区数：36，总记录数:1649855,各个分区记录数：92373, 0, 93535, 0, 90279, 0, 90144, 0, 92110, 0, 93433, 0, 90355, 0, 90043, 0, 92041, 0, 93400, 0, 90700, 0, 90186, 0, 92038, 0, 93297, 0, 90201, 0, 90156, 0, 92268, 0, 93296, 0
  }
}
