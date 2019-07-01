package org.apache.spark.sql.execution.datasources.jdbc

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FunSuite}

class Test extends FunSuite with BeforeAndAfter{

  var sparkSession:SparkSession =_
  before{
/*    // Spark Configuration.
    val conf = new SparkConf()
      .setAppName("IgniteRDDExample")
      .setMaster("local[2]")
      .set("spark.executor.instances", "2")
    // Spark context.
    sparkSession = SparkSession.builder().config(conf).getOrCreate()*/

  }
  after{

  }

  test("insert data"){
    var DEFAULT_SEPARATOR:Character = '9'
    println(DEFAULT_SEPARATOR)
    /*SETT_DATE
SETT_BAT_NO
SETT_FILE_NO
BIZ_CODE_EX
PROC_SIGN
REC_SN
SCDM
QSBH
ZQZH
XWH
ZQDM
ZQLB
LTLX
QYLB
GPNF
BDSL
BDLX
BDRQ
SL
BH
BYZD*/
    val or = sparkSession.sparkContext.parallelize(0 until  2000000,36)
    or.map(i=>"20180808")

  }
}
