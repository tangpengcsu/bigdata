package org.apache.ignite.spark

import com.szkingdom.fspt.spark.ignite.test.{Person, PersonKey}
import org.apache.ignite.{Ignite, Ignition}
import org.apache.spark.SparkConf
import org.apache.spark.sql.ignite.IgniteExternalCatalog.OPTION_GRID
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.{BeforeAndAfter, FunSuite}

import scala.collection.mutable.ArrayBuffer

class DFTest extends FunSuite with BeforeAndAfter{
    var CONFIG = "D:\\users\\tangp\\Documents\\idea\\big-data\\spark\\ignite-spark\\src\\test\\resources\\example-shared-object-rdd.xml"
  CONFIG = "D:\\users\\tangp\\Documents\\idea\\big-data\\spark\\ignite-spark\\src\\test\\resources\\example-rdd.xml"



   val conf = new SparkConf()
     .setAppName("IgniteRDDExample")
     .setMaster("local[2]")
     .set("spark.executor.instances", "2")


    // Spark context.
    val sparkSession:SparkSession= SparkSession.builder().config(conf).getOrCreate()



  after{
    sparkSession.close()
    Ignition.stop(false)
  }


  test("query"){

    val peopleDF = sparkSession.read.format(IgniteDataFrameSettings.FORMAT_IGNITE)
      .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE, CONFIG).option(IgniteDataFrameSettings.OPTION_TABLE, "PERSON5").load
    peopleDF.createOrReplaceTempView("Person")


    val sqlDF  = sparkSession.sql("SELECT * FROM Person where id = '100'")
     sqlDF.collect()
    println(s"peopleDf partitionNUm:${peopleDF.rdd.partitions.size}；peopleDf count:${peopleDF.count()};sqlDf partitionNum:${sqlDF.rdd.partitions.size}; count:${sqlDF.count()}")



  }

  test("write"){

    import sparkSession.implicits._
    val df = sparkSession.sparkContext
      .parallelize(1 to 20000, 16).map(i => Person1(i + "name", i.toString, i + "addr")).toDF()

    df.write.mode(SaveMode.Append)
      .format(IgniteDataFrameSettings.FORMAT_IGNITE)
      .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE, CONFIG)
      .option(IgniteDataFrameSettings.OPTION_TABLE, "PERSON5")
      .option(IgniteDataFrameSettings.OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS, "id")
      .option(IgniteDataFrameSettings.OPTION_CREATE_TABLE_PARAMETERS, "template=partitioned")
      .save()

    //通过取gridname（igniteInstanceName）也可取的IgniteConfiguration:
    val peopleDF = sparkSession.read.format(IgniteDataFrameSettings.FORMAT_IGNITE)
      .option(OPTION_GRID,"txt-test")
      .option(IgniteDataFrameSettings.OPTION_TABLE, "PERSON5").load()
    peopleDF.createOrReplaceTempView("Person")


    val sqlDF  = sparkSession.sql("SELECT * FROM Person where id = '100'")
    sqlDF.collect()
    println(s"peopleDf partitionNUm:${peopleDF.rdd.partitions.size}；peopleDf count:${peopleDF.count()};sqlDf partitionNum:${sqlDF.rdd.partitions.size}; count:${sqlDF.count()}")


  }

}
case class Person1(name: String, id: String, address: String) extends Serializable