package org.apache.ignite.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FunSuite}

class RddTest extends  FunSuite with BeforeAndAfter {
  var sparkContext: SparkContext =  _
  var igniteContext :IgniteContext= _
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
    val CONFIG = "example-shared-rdd.xml"

    // Creates Ignite context with above configuration.
     igniteContext = new IgniteContext(sparkContext, CONFIG, false)
  }

 after{
   igniteContext.close(true)
   sparkContext.stop()
 }


  test("write"){
    // Creates an Ignite Shared RDD of Type (Int,Int) Integer Pair.
    val sharedRDD: IgniteRDD[Int, Int] = igniteContext.fromCache[Int, Int]("sharedRDD")
    // Fill the Ignite Shared RDD in with Int pairs.
    sharedRDD.savePairs(sparkContext.parallelize(1 to 200000, 10).map(i => (i, i)))
  }

  test("query"){
    // Retrieve sharedRDD back from the Cache.
    val transformedValues: IgniteRDD[Int, Int] = igniteContext.fromCache("sharedRDD")


    val s = transformedValues.objectSql("Integer"," _val < 100 and _val > 9")

    println("======start")

    s.foreach(i=> println("---value:"+i))
    print("=====end")
    println(s"============分区数：${transformedValues.getNumPartitions}")


    // Perform some transformations on IgniteRDD and print.
    val squareAndRootPair = transformedValues.map { case (x, y) => (x, Math.sqrt(y.toDouble)) }
    println(">>> Transforming values stored in Ignite Shared RDD...")

    // Filter out pairs which square roots are less than 100 and
    // take the first five elements from the transformed IgniteRDD and print them.
    squareAndRootPair.filter(_._2 < 100.0).take(5).foreach(println)

    println(">>> Executing SQL query over Ignite Shared RDD...")

    // Execute a SQL query over the Ignite Shared RDD.
    val df = transformedValues.sql("select _val from Integer where _val < 100 and _val > 9 ")

    // Show ten rows from the result set.
    df.show(10)
  }
  test("query object"){

  }

}
